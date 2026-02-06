#!/usr/bin/env python3
"""
ACF Guidance — Browser-Based Recovery
========================================
Uses Playwright (headless Chrome) to render JavaScript-heavy pages
that the basic requests-based scraper couldn't handle.

Targets:
  - headstart.gov pages (OHS office) — JS-rendered content
  - Any remaining no_documents pages with saved HTML that had too little content
  - Failed rows get one more try with a real browser

Prerequisites:
  pip install playwright fpdf2 beautifulsoup4
  playwright install chromium

Usage:
  python browser_recover.py                  # Run full recovery
  python browser_recover.py --phase 1        # JS-rendered pages only
  python browser_recover.py --phase 2        # Retry failed only
  python browser_recover.py --dry-run        # Preview
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from fpdf import FPDF

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: Playwright not installed.")
    print("Run these commands:")
    print("  pip install playwright")
    print("  playwright install chromium")
    sys.exit(1)

DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output")
DEFAULT_REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")

DOWNLOAD_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt",
    ".rtf", ".ppt", ".pptx", ".zip", ".htm", ".html"
}

SKIP_URL_PATTERNS = {
    "vulnerability-disclosure-policy", "/privacy-policy", "/accessibility",
    "/foia", "/disclaimers", "/nofear", "/plainlanguage", "/usa.gov",
    "/digital-strategy", "/web-policies-and-important-links",
    "facebook.com", "twitter.com", "youtube.com", "linkedin.com", "instagram.com",
}

PAGE_TIMEOUT = 30000  # 30 seconds
NAVIGATE_TIMEOUT = 45000  # 45 seconds


def sha256_file(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_filename(name, max_len=100):
    name = re.sub(r'[<>:"/\\|?*]', '_', str(name))
    name = re.sub(r'\s+', '_', name)
    name = name.strip('._')
    return name[:max_len] if name else "unnamed"


# ─── PDF Creation ────────────────────────────────────────────────────────────

class GuidancePDF(FPDF):
    def __init__(self, title="", doc_number="", office="", source_url=""):
        super().__init__()
        self.doc_title = title
        self.doc_number = doc_number
        self.office = office
        self.source_url = source_url

    def header(self):
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(100, 100, 100)
        header_text = f"ACF {self.office}"
        if self.doc_number:
            header_text += f" | {self.doc_number}"
        self.cell(0, 5, header_text, new_x="LMARGIN", new_y="NEXT", align="L")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}} | Source: {self.source_url[:80]}",
                  new_x="LMARGIN", new_y="TOP", align="C")


def extract_blocks_from_html(html_content):
    """Extract structured text blocks from rendered HTML."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove junk
    for tag in soup.find_all(["script", "style", "nav", "footer", "noscript",
                               "iframe", "svg", "meta", "link", "button"]):
        tag.decompose()

    for tag in soup.find_all(attrs={"class": re.compile(
            r"header|banner|nav|menu|sidebar|footer|breadcrumb|skip|cookie|usa-banner",
            re.I)}):
        tag.decompose()
    for tag in soup.find_all(attrs={"id": re.compile(
            r"header|banner|nav|menu|sidebar|footer|breadcrumb|skip|cookie",
            re.I)}):
        tag.decompose()
    for tag in soup.find_all(attrs={"role": re.compile(
            r"navigation|banner|complementary", re.I)}):
        tag.decompose()

    # Find main content
    main = (soup.find("main") or
            soup.find("article") or
            soup.find("div", {"class": re.compile(
                r"field--name-body|node__content|content-area|main-content|page-content",
                re.I)}) or
            soup.find("div", {"id": re.compile(
                r"content|main|block-system-main", re.I)}) or
            soup.find("body"))

    if not main:
        main = soup

    blocks = []
    seen_starts = set()

    for element in main.find_all(["h1", "h2", "h3", "h4", "h5", "p", "li",
                                    "td", "th", "div", "span", "blockquote",
                                    "dd", "dt", "figcaption", "label",
                                    "section", "article"]):
        text = element.get_text(separator=" ", strip=True)

        if not text or len(text) < 10:
            continue

        # Skip navigation junk
        if any(skip in text.lower() for skip in [
            "skip to main", "here's how you know", ".gov website",
            "secure .gov", "official website", "share sensitive",
            "an official website of the united states",
            "contact us", "upcoming events", "login",
        ]):
            continue

        # Deduplicate
        start = text[:50]
        if start in seen_starts:
            continue
        seen_starts.add(start)

        is_heading = element.name in ["h1", "h2", "h3", "h4", "h5"]
        blocks.append({
            "text": text,
            "is_heading": is_heading,
            "level": int(element.name[1]) if is_heading else 0
        })

    return blocks


def create_pdf_from_blocks(blocks, metadata, output_path):
    """Create PDF from text blocks."""
    title = metadata.get("title", "")
    doc_number = metadata.get("doc_number", "")
    office = metadata.get("office", "")
    issue_date = metadata.get("issue_date", "")
    source_url = metadata["urls"][0]["url"] if metadata.get("urls") else ""

    pdf = GuidancePDF(title=title, doc_number=doc_number,
                      office=office, source_url=source_url)
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(0, 0, 0)
    if title:
        safe_title = title.encode('latin-1', 'replace').decode('latin-1')
        pdf.multi_cell(0, 7, safe_title, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # Metadata
    meta_parts = []
    if doc_number:
        meta_parts.append(f"Document: {doc_number}")
    if office:
        meta_parts.append(f"Office: {office}")
    if issue_date:
        meta_parts.append(f"Date: {issue_date[:10]}")
    if meta_parts:
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(0, 5, " | ".join(meta_parts), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    if source_url:
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(0, 0, 200)
        safe_url = source_url.encode('latin-1', 'replace').decode('latin-1')
        pdf.cell(0, 5, f"Source: {safe_url}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)
    pdf.set_text_color(0, 0, 0)

    for block in blocks:
        safe_text = block["text"].encode('latin-1', 'replace').decode('latin-1')
        if block["is_heading"]:
            size = {1: 14, 2: 12, 3: 11}.get(block["level"], 11)
            pdf.set_font("Helvetica", "B", size)
            pdf.ln(2)
            pdf.multi_cell(0, 6, safe_text, new_x="LMARGIN", new_y="NEXT")
            pdf.ln(1)
        else:
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 5, safe_text, new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

    pdf.output(output_path)


def find_downloadable_links_from_page(page_content, base_url):
    """Find downloadable file links in HTML."""
    soup = BeautifulSoup(page_content, "html.parser")
    links = []
    seen = set()

    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue
        full_url = urljoin(base_url, href)
        if any(p in full_url.lower() for p in SKIP_URL_PATTERNS):
            continue
        parsed = urlparse(full_url)
        ext = os.path.splitext(parsed.path.lower())[1]
        if ext in DOWNLOAD_EXTENSIONS and full_url not in seen:
            link_text = tag.get_text(strip=True)[:200] or os.path.basename(parsed.path)
            links.append({"url": full_url, "text": link_text, "extension": ext})
            seen.add(full_url)

    return links


# ─── Browser-Based Processing ───────────────────────────────────────────────

def process_with_browser(page, row_dir, metadata, url):
    """Visit a URL with Playwright browser and extract content/files."""
    try:
        response = page.goto(url, timeout=NAVIGATE_TIMEOUT,
                             wait_until="networkidle")
    except Exception as e:
        # Try with just domcontentloaded if networkidle times out
        try:
            response = page.goto(url, timeout=NAVIGATE_TIMEOUT,
                                 wait_until="domcontentloaded")
            page.wait_for_timeout(3000)  # Wait 3s for JS to render
        except Exception as e2:
            print(f"    [FAIL] Could not load page: {e2}")
            return False

    if not response:
        print(f"    [FAIL] No response")
        return False

    status = response.status
    if status >= 400:
        print(f"    [FAIL] HTTP {status}")
        return False

    # Get the fully rendered HTML
    rendered_html = page.content()
    final_url = page.url

    # Save rendered HTML
    html_path = os.path.join(row_dir, "page_content.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(rendered_html)

    # Check for downloadable file links
    doc_links = find_downloadable_links_from_page(rendered_html, final_url)
    downloaded_files = 0

    if doc_links:
        for link in doc_links:
            try:
                dl_response = page.request.get(link["url"], timeout=REQUEST_TIMEOUT)
                if dl_response.ok:
                    parsed = urlparse(link["url"])
                    fname = safe_filename(os.path.basename(parsed.path)) or "document"
                    fpath = os.path.join(row_dir, fname)

                    # Avoid overwriting
                    base, ext = os.path.splitext(fpath)
                    counter = 1
                    while os.path.exists(fpath):
                        fpath = f"{base}_{counter}{ext}"
                        counter += 1

                    with open(fpath, "wb") as f:
                        f.write(dl_response.body())

                    file_size = os.path.getsize(fpath)
                    if file_size > 100:  # Skip trivial files
                        metadata["files_downloaded"].append({
                            "filename": os.path.basename(fpath),
                            "source_url": link["url"],
                            "size_bytes": file_size,
                            "sha256": sha256_file(fpath),
                            "content_type": dl_response.headers.get("content-type", ""),
                            "download_type": "browser_scraped"
                        })
                        downloaded_files += 1
                        print(f"    [OK] {os.path.basename(fpath)} ({file_size:,} bytes)")
                    else:
                        os.remove(fpath)
            except Exception as e:
                pass  # Skip individual download failures

    if downloaded_files > 0:
        metadata["status"] = "success"
        metadata["recovery_method"] = "browser_scrape"
        return True

    # No files found — try to extract text and convert to PDF
    blocks = extract_blocks_from_html(rendered_html)

    if blocks and len(blocks) >= 2:
        doc_num = metadata.get("doc_number", "").strip()
        if doc_num:
            sname = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
            pdf_name = f"{sname}_browser.pdf"
        else:
            pdf_name = f"guidance_{os.path.basename(row_dir)}_browser.pdf"

        pdf_path = os.path.join(row_dir, pdf_name)

        try:
            create_pdf_from_blocks(blocks, metadata, pdf_path)
            file_size = os.path.getsize(pdf_path)
            metadata["files_downloaded"].append({
                "filename": pdf_name,
                "source_url": url,
                "size_bytes": file_size,
                "sha256": sha256_file(pdf_path),
                "content_type": "application/pdf",
                "download_type": "browser_html_to_pdf"
            })
            metadata["status"] = "success"
            metadata["recovery_method"] = "browser_html_to_pdf"
            print(f"    [OK] Converted to PDF: {pdf_name} ({file_size:,} bytes, {len(blocks)} blocks)")
            return True
        except Exception as e:
            print(f"    [ERROR] PDF creation: {e}")
            return False
    else:
        # Last resort: use Playwright's built-in PDF generation
        try:
            pdf_name = f"guidance_{os.path.basename(row_dir)}_print.pdf"
            pdf_path = os.path.join(row_dir, pdf_name)
            page.pdf(path=pdf_path, format="Letter",
                     margin={"top": "0.75in", "bottom": "0.75in",
                             "left": "0.75in", "right": "0.75in"},
                     print_background=True)
            file_size = os.path.getsize(pdf_path)
            if file_size > 1000:
                metadata["files_downloaded"].append({
                    "filename": pdf_name,
                    "source_url": url,
                    "size_bytes": file_size,
                    "sha256": sha256_file(pdf_path),
                    "content_type": "application/pdf",
                    "download_type": "browser_print_to_pdf"
                })
                metadata["status"] = "success"
                metadata["recovery_method"] = "browser_print_to_pdf"
                print(f"    [OK] Print-to-PDF: {pdf_name} ({file_size:,} bytes)")
                return True
            else:
                os.remove(pdf_path)
        except Exception as e:
            # print-to-pdf only works in headless Chromium, may fail in headed mode
            pass

        print(f"    [EMPTY] Page loaded but no extractable content")
        return False


REQUEST_TIMEOUT = 30000  # milliseconds for Playwright


def run_browser_recovery(output_dir, target_statuses, phase_name, dry_run=False):
    """Run browser-based recovery on rows with given statuses."""
    print(f"\n{'=' * 60}")
    print(f"{phase_name}")
    print("=" * 60)

    # Collect target rows
    targets = []
    for folder_name in sorted(os.listdir(output_dir)):
        row_dir = os.path.join(output_dir, folder_name)
        if not os.path.isdir(row_dir):
            continue

        meta_path = os.path.join(row_dir, "metadata.json")
        if not os.path.exists(meta_path):
            continue

        with open(meta_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        if metadata.get("status") not in target_statuses:
            continue

        if not metadata.get("urls"):
            continue

        targets.append((folder_name, row_dir, metadata))

    print(f"  Found {len(targets)} rows to process")

    if not targets:
        return {"recovered": 0, "still_failed": 0, "total": 0}

    if dry_run:
        for folder_name, _, meta in targets[:20]:
            url = meta["urls"][0]["url"][:70]
            print(f"  [DRY RUN] {folder_name} | {meta.get('office','?'):6s} | {url}")
        if len(targets) > 20:
            print(f"  ... and {len(targets) - 20} more")
        return {"recovered": len(targets), "still_failed": 0, "total": len(targets)}

    stats = {"recovered": 0, "still_failed": 0, "total": len(targets)}

    with sync_playwright() as p:
        # Launch headless Chromium
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            java_script_enabled=True
        )
        page = context.new_page()

        for i, (folder_name, row_dir, metadata) in enumerate(targets):
            url = metadata["urls"][0]["url"]
            print(f"\n  [{i+1}/{len(targets)}] Row {folder_name}: {url[:70]}")

            success = process_with_browser(page, row_dir, metadata, url)

            if success:
                stats["recovered"] += 1
            else:
                stats["still_failed"] += 1

            # Clear errors if we got something
            if success:
                metadata["errors"] = []

            metadata["browser_retry_at"] = datetime.now().isoformat()

            meta_path = os.path.join(row_dir, "metadata.json")
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, default=str)

            # Brief pause between requests
            time.sleep(1)

            # Progress update every 20 rows
            if (i + 1) % 20 == 0:
                print(f"\n  --- Progress: {i+1}/{len(targets)} "
                      f"({stats['recovered']} recovered, "
                      f"{stats['still_failed']} failed) ---")

        browser.close()

    print(f"\n{phase_name} Results: {stats['recovered']} recovered, "
          f"{stats['still_failed']} still failed out of {stats['total']}")
    return stats


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ACF Browser-Based Recovery")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--reports", default=DEFAULT_REPORTS)
    parser.add_argument("--phase", type=int, choices=[1, 2], default=None,
                        help="1=no_documents only, 2=failed only, default=both")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output)
    reports_dir = os.path.abspath(args.reports)
    os.makedirs(reports_dir, exist_ok=True)

    print("=" * 60)
    print("ACF GUIDANCE — BROWSER-BASED RECOVERY")
    print("=" * 60)
    print(f"Output: {output_dir}")
    if args.dry_run:
        print("MODE: DRY RUN")

    all_stats = {}
    start_time = datetime.now()

    # Phase 1: JS-rendered pages (no_documents)
    if args.phase is None or args.phase == 1:
        all_stats["phase1"] = run_browser_recovery(
            output_dir,
            target_statuses={"no_documents"},
            phase_name="PHASE 1: BROWSER RENDER — NO DOCUMENTS PAGES",
            dry_run=args.dry_run
        )

    # Phase 2: Failed rows
    if args.phase is None or args.phase == 2:
        all_stats["phase2"] = run_browser_recovery(
            output_dir,
            target_statuses={"failed"},
            phase_name="PHASE 2: BROWSER RETRY — FAILED ROWS",
            dry_run=args.dry_run
        )

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n{'=' * 60}")
    print(f"BROWSER RECOVERY COMPLETE ({elapsed:.1f} seconds)")
    print("=" * 60)

    # Summary
    total_recovered = sum(s.get("recovered", 0) for s in all_stats.values())
    total_failed = sum(s.get("still_failed", 0) for s in all_stats.values())
    print(f"  Total recovered: {total_recovered}")
    print(f"  Still failed: {total_failed}")

    # Save report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"browser_recovery_{timestamp}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"stats": all_stats, "duration": elapsed}, f, indent=2)
    print(f"Report saved: {report_path}")


if __name__ == "__main__":
    sys.exit(main())
