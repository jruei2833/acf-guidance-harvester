#!/usr/bin/env python3
"""
ACF Guidance — Browser Retry for Matched URLs
================================================
Uses Playwright to visit URLs that returned 403 Forbidden
when accessed via the requests library.

Also attempts to find correct URLs for 404s by visiting
the page the match came from and following redirects.

Usage:
  python browser_retry_matched.py
  python browser_retry_matched.py --dry-run
"""

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: playwright not installed. Run: pip install playwright")
    sys.exit(1)

DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output")
DEFAULT_REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")

DOWNLOAD_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt",
    ".rtf", ".ppt", ".pptx", ".zip"
}

SKIP_URL_PATTERNS = {
    "vulnerability-disclosure-policy", "/privacy-policy", "/accessibility",
    "/foia", "/disclaimers", "/nofear", "/plainlanguage", "/usa.gov",
    "/digital-strategy", "/web-policies-and-important-links",
    "facebook.com", "twitter.com", "youtube.com", "linkedin.com", "instagram.com",
}


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


def find_doc_links(html_content, base_url):
    """Find downloadable file links in HTML."""
    soup = BeautifulSoup(html_content, "html.parser")
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
            links.append({"url": full_url, "text": tag.get_text(strip=True)[:200]})
            seen.add(full_url)
    return links


def extract_blocks(html_content):
    """Extract text blocks for PDF creation."""
    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup.find_all(["script", "style", "nav", "footer", "noscript",
                               "iframe", "svg", "button"]):
        tag.decompose()
    for tag in soup.find_all(attrs={"class": re.compile(
            r"header|banner|nav|menu|sidebar|footer|breadcrumb|skip|cookie|usa-banner",
            re.I)}):
        tag.decompose()
    for tag in soup.find_all(attrs={"role": re.compile(
            r"navigation|banner|complementary", re.I)}):
        tag.decompose()

    main = (soup.find("main") or soup.find("article") or
            soup.find("div", {"class": re.compile(r"content|main", re.I)}) or
            soup.find("body"))
    if not main:
        main = soup

    blocks = []
    seen = set()
    for el in main.find_all(["h1", "h2", "h3", "h4", "p", "li", "td", "div",
                              "blockquote", "dd", "dt"]):
        text = el.get_text(separator=" ", strip=True)
        if text and len(text) > 10:
            start = text[:50]
            if start not in seen:
                seen.add(start)
                is_h = el.name in ["h1", "h2", "h3", "h4"]
                blocks.append({"text": text, "is_heading": is_h,
                               "level": int(el.name[1]) if is_h else 0})
    return blocks


def create_pdf_from_blocks(blocks, metadata, output_path, source_url):
    """Create PDF from text blocks using fpdf2."""
    from fpdf import FPDF

    class GuidancePDF(FPDF):
        def __init__(self, office, doc_number, src_url):
            super().__init__()
            self.office = office
            self.doc_number = doc_number
            self.src_url = src_url
        def header(self):
            self.set_font("Helvetica", "B", 8)
            self.set_text_color(100, 100, 100)
            h = f"ACF {self.office}"
            if self.doc_number:
                h += f" | {self.doc_number}"
            self.cell(0, 5, h, new_x="LMARGIN", new_y="NEXT", align="L")
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(3)
        def footer(self):
            self.set_y(-15)
            self.set_font("Helvetica", "I", 7)
            self.set_text_color(128, 128, 128)
            self.cell(0, 10, f"Page {self.page_no()}/{{nb}} | {self.src_url[:80]}",
                      new_x="LMARGIN", new_y="TOP", align="C")

    title = metadata.get("title", "")
    doc_number = metadata.get("doc_number", "")
    office = metadata.get("office", "")
    issue_date = metadata.get("issue_date", "")

    pdf = GuidancePDF(office, doc_number, source_url)
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    if title:
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(0, 0, 0)
        safe = title.encode('latin-1', 'replace').decode('latin-1')
        pdf.multi_cell(0, 7, safe, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

    meta_parts = []
    if doc_number: meta_parts.append(f"Document: {doc_number}")
    if office: meta_parts.append(f"Office: {office}")
    if issue_date: meta_parts.append(f"Date: {issue_date[:10]}")
    if meta_parts:
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(0, 5, " | ".join(meta_parts), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(0, 0, 200)
    safe_url = source_url.encode('latin-1', 'replace').decode('latin-1')[:100]
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


def process_with_browser(page, row_dir, metadata, url):
    """Visit URL with Playwright and download content."""
    try:
        response = page.goto(url, timeout=45000, wait_until="networkidle")
    except Exception:
        try:
            response = page.goto(url, timeout=45000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
        except Exception as e:
            print(f"    [FAIL] Could not load: {e}")
            return False

    if not response:
        print(f"    [FAIL] No response")
        return False

    status = response.status
    if status >= 400:
        print(f"    [FAIL] HTTP {status}")
        return False

    html = page.content()
    final_url = page.url

    # Save HTML
    html_path = os.path.join(row_dir, "page_content.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Look for downloadable links
    doc_links = find_doc_links(html, final_url)
    downloaded = 0

    for link in doc_links:
        try:
            dl = page.request.get(link["url"], timeout=30000)
            if dl.ok:
                parsed = urlparse(link["url"])
                fname = safe_filename(os.path.basename(parsed.path)) or "document"
                fpath = os.path.join(row_dir, fname)
                base, ext = os.path.splitext(fpath)
                counter = 1
                while os.path.exists(fpath):
                    fpath = f"{base}_{counter}{ext}"
                    counter += 1
                with open(fpath, "wb") as f:
                    f.write(dl.body())
                fsize = os.path.getsize(fpath)
                if fsize > 100:
                    metadata["files_downloaded"].append({
                        "filename": os.path.basename(fpath),
                        "source_url": link["url"],
                        "size_bytes": fsize,
                        "sha256": sha256_file(fpath),
                        "content_type": dl.headers.get("content-type", ""),
                        "download_type": "browser_matched_retry"
                    })
                    downloaded += 1
                    print(f"    [OK] {os.path.basename(fpath)} ({fsize:,} bytes)")
                else:
                    os.remove(fpath)
        except Exception:
            pass

    if downloaded:
        metadata["status"] = "success"
        metadata["recovery_method"] = f"browser_matched_{downloaded}_files"
        return True

    # No files — try PDF from content
    blocks = extract_blocks(html)
    if blocks and len(blocks) >= 2:
        doc_num = metadata.get("doc_number", "").strip()
        if doc_num:
            sname = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
            pdf_name = f"{sname}_matched.pdf"
        else:
            pdf_name = f"guidance_{metadata['folder']}_matched.pdf"
        pdf_path = os.path.join(row_dir, pdf_name)
        try:
            create_pdf_from_blocks(blocks, metadata, pdf_path, final_url)
            fsize = os.path.getsize(pdf_path)
            metadata["files_downloaded"].append({
                "filename": pdf_name,
                "source_url": final_url,
                "size_bytes": fsize,
                "sha256": sha256_file(pdf_path),
                "content_type": "application/pdf",
                "download_type": "browser_matched_html_to_pdf"
            })
            metadata["status"] = "success"
            metadata["recovery_method"] = "browser_matched_html_to_pdf"
            print(f"    [OK] Converted to {pdf_name} ({fsize:,} bytes, {len(blocks)} blocks)")
            return True
        except Exception as e:
            print(f"    [ERROR] PDF creation: {e}")

    # Last resort: Playwright print-to-PDF
    try:
        pdf_name = f"guidance_{metadata['folder']}_print.pdf"
        pdf_path = os.path.join(row_dir, pdf_name)
        page.pdf(path=pdf_path, format="Letter",
                 margin={"top": "0.75in", "bottom": "0.75in",
                         "left": "0.75in", "right": "0.75in"},
                 print_background=True)
        fsize = os.path.getsize(pdf_path)
        if fsize > 1000:
            metadata["files_downloaded"].append({
                "filename": pdf_name,
                "source_url": final_url,
                "size_bytes": fsize,
                "sha256": sha256_file(pdf_path),
                "content_type": "application/pdf",
                "download_type": "browser_print_to_pdf"
            })
            metadata["status"] = "success"
            metadata["recovery_method"] = "browser_print_to_pdf"
            print(f"    [OK] Print-to-PDF: {pdf_name} ({fsize:,} bytes)")
            return True
        else:
            os.remove(pdf_path)
    except Exception:
        pass

    print(f"    [EMPTY] Page loaded but no content extracted")
    return False


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--reports", default=DEFAULT_REPORTS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output)
    reports_dir = os.path.abspath(args.reports)
    os.makedirs(reports_dir, exist_ok=True)

    print("=" * 60)
    print("ACF — BROWSER RETRY FOR REMAINING FAILED ROWS")
    print("=" * 60)

    # Collect all still-failed rows
    targets = []
    for folder_name in sorted(os.listdir(output_dir)):
        row_dir = os.path.join(output_dir, folder_name)
        if not os.path.isdir(row_dir):
            continue
        meta_path = os.path.join(row_dir, "metadata.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        if meta.get("status") in ("failed", "no_documents"):
            # Determine which URL to try
            urls_to_try = []

            # If we have a matched_url from smart_match, try that first
            if meta.get("matched_url"):
                urls_to_try.append(meta["matched_url"])

            # Also try the original URL
            if meta.get("urls"):
                orig = meta["urls"][0]["url"]
                if orig not in urls_to_try:
                    urls_to_try.append(orig)

                # Try www.acf.hhs.gov variant
                if "acf.gov" in orig and "acf.hhs.gov" not in orig:
                    alt = orig.replace("acf.gov", "www.acf.hhs.gov", 1)
                    if alt not in urls_to_try:
                        urls_to_try.append(alt)

            if urls_to_try:
                meta["_row_dir"] = row_dir
                meta["_meta_path"] = meta_path
                meta["_urls_to_try"] = urls_to_try
                targets.append(meta)

    print(f"  Found {len(targets)} rows to retry with browser")

    if not targets:
        print("  Nothing to do.")
        return

    if args.dry_run:
        for meta in targets:
            folder = meta["folder"]
            doc = meta.get("doc_number", "")
            urls = meta["_urls_to_try"]
            print(f"  {folder} | {meta.get('office','?'):6s} | {doc[:30]:30s} | {urls[0][:50]}")
        print(f"\n  Total: {len(targets)} rows")
        return

    stats = {"recovered": 0, "failed": 0}
    start_time = datetime.now()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            java_script_enabled=True
        )
        page = context.new_page()

        for i, meta in enumerate(targets):
            folder = meta["folder"]
            doc = meta.get("doc_number", "")
            urls = meta["_urls_to_try"]

            print(f"\n  [{i+1}/{len(targets)}] Row {folder}: {doc or meta.get('title','')[:40]}")

            success = False
            for url in urls:
                print(f"    Trying: {url[:70]}")
                success = process_with_browser(page, meta["_row_dir"], meta, url)
                if success:
                    break
                time.sleep(1)

            if success:
                meta["errors"] = []
                meta["browser_final_at"] = datetime.now().isoformat()
                stats["recovered"] += 1
            else:
                stats["failed"] += 1

            # Save metadata
            with open(meta["_meta_path"], "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2, default=str)

            time.sleep(1)

            if (i + 1) % 10 == 0:
                print(f"\n  --- Progress: {i+1}/{len(targets)} "
                      f"(recovered: {stats['recovered']}, failed: {stats['failed']}) ---")

        browser.close()

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n{'=' * 60}")
    print(f"BROWSER RETRY COMPLETE ({elapsed:.1f} seconds)")
    print("=" * 60)
    print(f"  Recovered: {stats['recovered']}")
    print(f"  Still failed: {stats['failed']}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"browser_retry_matched_{timestamp}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"stats": stats, "duration": elapsed}, f, indent=2)
    print(f"Report saved: {report_path}")


if __name__ == "__main__":
    main()
