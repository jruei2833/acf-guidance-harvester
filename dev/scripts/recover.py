#!/usr/bin/env python3
"""
ACF Guidance — Recovery Script
================================
Handles the remaining ~300 rows that weren't captured in the initial harvest:

1. Converts remaining HTML pages to PDF using a more aggressive text extractor
2. Retries failed rows using Wayback Machine

Usage:
  python recover.py                    # Run both conversions and retries
  python recover.py --convert-only     # Only convert remaining HTML to PDF
  python recover.py --retry-only       # Only retry failed rows
  python recover.py --dry-run          # Show what would be done
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

import requests
from bs4 import BeautifulSoup
from fpdf import FPDF

DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output")
DEFAULT_REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

SKIP_URL_PATTERNS = {
    "vulnerability-disclosure-policy", "/privacy-policy", "/accessibility",
    "/foia", "/disclaimers", "/nofear", "/plainlanguage", "/usa.gov",
    "/digital-strategy", "/web-policies-and-important-links",
    "facebook.com", "twitter.com", "youtube.com", "linkedin.com", "instagram.com",
}

DOWNLOAD_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt",
    ".rtf", ".ppt", ".pptx", ".zip", ".htm", ".html"
}

REQUEST_DELAY = 1.5
REQUEST_TIMEOUT = 30


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


# ─── PART 1: Aggressive HTML to PDF ─────────────────────────────────────────

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


def aggressive_extract(html_path):
    """More aggressive text extraction — grabs ALL visible text."""
    with open(html_path, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")

    # Remove junk tags
    for tag in soup.find_all(["script", "style", "nav", "footer", "noscript",
                               "iframe", "svg", "meta", "link", "button"]):
        tag.decompose()

    # Remove header/banner/nav elements by class/id/role
    for tag in soup.find_all(attrs={"class": re.compile(
            r"header|banner|nav|menu|sidebar|footer|breadcrumb|skip|cookie",
            re.I)}):
        tag.decompose()
    for tag in soup.find_all(attrs={"id": re.compile(
            r"header|banner|nav|menu|sidebar|footer|breadcrumb|skip|cookie",
            re.I)}):
        tag.decompose()
    for tag in soup.find_all(attrs={"role": re.compile(
            r"navigation|banner|complementary", re.I)}):
        tag.decompose()

    # Try multiple selectors for main content
    main = (soup.find("main") or
            soup.find("article") or
            soup.find("div", {"class": re.compile(
                r"field--name-body|content|main-content|page-content|article",
                re.I)}) or
            soup.find("div", {"id": re.compile(
                r"content|main|block-system-main", re.I)}) or
            soup.find("body"))

    if not main:
        main = soup

    # Get all text blocks
    blocks = []
    for element in main.find_all(["h1", "h2", "h3", "h4", "h5", "p", "li",
                                    "td", "th", "div", "span", "blockquote",
                                    "dd", "dt", "figcaption", "label"]):
        text = element.get_text(separator=" ", strip=True)
        if text and len(text) > 10:
            if any(skip in text.lower() for skip in [
                "skip to main", "here's how you know", ".gov website",
                "secure .gov", "official website", "share sensitive",
                "an official website of the united states"
            ]):
                continue

            is_heading = element.name in ["h1", "h2", "h3", "h4", "h5"]
            blocks.append({
                "text": text,
                "is_heading": is_heading,
                "level": int(element.name[1]) if is_heading else 0
            })

    # Deduplicate
    deduped = []
    seen_starts = set()
    for block in blocks:
        start = block["text"][:50]
        if start not in seen_starts:
            seen_starts.add(start)
            deduped.append(block)

    return deduped


def create_pdf_from_blocks(blocks, metadata, output_path):
    """Create PDF from extracted text blocks."""
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

    # Metadata line
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


def convert_remaining_html(output_dir, dry_run=False):
    """Convert remaining no_documents HTML files to PDF."""
    print("\n" + "=" * 60)
    print("PHASE 1: AGGRESSIVE HTML TO PDF CONVERSION")
    print("=" * 60)

    stats = {"converted": 0, "empty": 0, "error": 0, "skipped": 0}

    for folder_name in sorted(os.listdir(output_dir)):
        row_dir = os.path.join(output_dir, folder_name)
        if not os.path.isdir(row_dir):
            continue

        meta_path = os.path.join(row_dir, "metadata.json")
        html_path = os.path.join(row_dir, "page_content.html")

        if not os.path.exists(meta_path) or not os.path.exists(html_path):
            stats["skipped"] += 1
            continue

        with open(meta_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        if metadata.get("status") != "no_documents":
            stats["skipped"] += 1
            continue

        if dry_run:
            print(f"  [DRY RUN] Would convert: {folder_name} — "
                  f"{metadata.get('doc_number', '')} ({metadata.get('office', '')})")
            stats["converted"] += 1
            continue

        blocks = aggressive_extract(html_path)

        if not blocks or len(blocks) < 2:
            print(f"  [EMPTY] {folder_name}: Insufficient content ({len(blocks)} blocks)")
            stats["empty"] += 1
            continue

        doc_num = metadata.get("doc_number", "").strip()
        if doc_num:
            safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
            pdf_name = f"{safe_name}_converted.pdf"
        else:
            pdf_name = f"guidance_{folder_name}_converted.pdf"

        pdf_path = os.path.join(row_dir, pdf_name)

        try:
            create_pdf_from_blocks(blocks, metadata, pdf_path)
            file_size = os.path.getsize(pdf_path)
            file_hash = sha256_file(pdf_path)

            source_url = metadata["urls"][0]["url"] if metadata.get("urls") else ""
            metadata["files_downloaded"].append({
                "filename": pdf_name,
                "source_url": source_url,
                "size_bytes": file_size,
                "sha256": file_hash,
                "content_type": "application/pdf",
                "download_type": "aggressive_html_to_pdf",
                "converted_at": datetime.now().isoformat()
            })
            metadata["status"] = "success"
            metadata["conversion_note"] = "Recovered via aggressive HTML extraction"

            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, default=str)

            print(f"  [OK] {folder_name}: {pdf_name} ({file_size:,} bytes)")
            stats["converted"] += 1

        except Exception as e:
            print(f"  [ERROR] {folder_name}: {e}")
            stats["error"] += 1

    print(f"\nPhase 1 Results: {stats['converted']} converted, "
          f"{stats['empty']} empty, {stats['error']} errors")
    return stats


# ─── PART 2: Retry Failed Rows ──────────────────────────────────────────────

def try_wayback(url, session):
    api_url = f"https://archive.org/wayback/available?url={url}"
    try:
        resp = session.get(api_url, timeout=15, headers=HEADERS)
        data = resp.json()
        snapshot = data.get("archived_snapshots", {}).get("closest", {})
        if snapshot.get("available"):
            wb_url = snapshot["url"]
            print(f"    Wayback found: {wb_url[:80]}")
            time.sleep(REQUEST_DELAY)
            resp2 = session.get(wb_url, timeout=REQUEST_TIMEOUT, headers=HEADERS,
                                allow_redirects=True)
            resp2.raise_for_status()
            return resp2
    except Exception as e:
        print(f"    Wayback failed: {e}")
    return None


def find_downloadable_links(html_content, base_url):
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
            link_text = tag.get_text(strip=True)[:200] or os.path.basename(parsed.path)
            links.append({"url": full_url, "text": link_text, "extension": ext})
            seen.add(full_url)
    return links


def retry_failed_rows(output_dir, dry_run=False):
    """Retry failed rows with direct, alternate domain, and Wayback fallback."""
    print("\n" + "=" * 60)
    print("PHASE 2: RETRY FAILED ROWS")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    stats = {"recovered": 0, "still_failed": 0, "saved_html": 0, "skipped": 0}

    for folder_name in sorted(os.listdir(output_dir)):
        row_dir = os.path.join(output_dir, folder_name)
        if not os.path.isdir(row_dir):
            continue

        meta_path = os.path.join(row_dir, "metadata.json")
        if not os.path.exists(meta_path):
            continue

        with open(meta_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        if metadata.get("status") != "failed":
            stats["skipped"] += 1
            continue

        if not metadata.get("urls"):
            stats["skipped"] += 1
            continue

        url = metadata["urls"][0]["url"]
        print(f"\n  Row {folder_name}: {url[:70]}")

        if dry_run:
            stats["recovered"] += 1
            continue

        time.sleep(REQUEST_DELAY)

        # Try direct
        resp = None
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS,
                               allow_redirects=True)
            resp.raise_for_status()
            print(f"    Direct: {resp.status_code}")
        except Exception:
            resp = None

        # Try alternate domain
        if not resp:
            alt_url = None
            if "acf.gov/" in url and "acf.hhs.gov" not in url:
                alt_url = url.replace("acf.gov/", "www.acf.hhs.gov/", 1)
            if alt_url:
                try:
                    print(f"    Alternate: {alt_url[:70]}")
                    time.sleep(REQUEST_DELAY)
                    resp = session.get(alt_url, timeout=REQUEST_TIMEOUT,
                                       headers=HEADERS, allow_redirects=True)
                    resp.raise_for_status()
                except Exception:
                    resp = None

        # Wayback
        if not resp:
            print(f"    Wayback...")
            time.sleep(REQUEST_DELAY)
            resp = try_wayback(url, session)

        if not resp:
            print(f"    [FAIL] Still unreachable")
            stats["still_failed"] += 1
            continue

        content_type = resp.headers.get("Content-Type", "").lower()

        # Direct file download
        if any(ct in content_type for ct in ["pdf", "msword", "spreadsheet",
                                              "zip", "octet-stream"]):
            parsed = urlparse(resp.url)
            fname = safe_filename(os.path.basename(parsed.path)) or "document.pdf"
            fpath = os.path.join(row_dir, fname)
            with open(fpath, "wb") as f:
                f.write(resp.content)
            metadata["files_downloaded"].append({
                "filename": fname,
                "source_url": resp.url,
                "size_bytes": os.path.getsize(fpath),
                "sha256": sha256_file(fpath),
                "content_type": content_type,
                "download_type": "retry_direct"
            })
            metadata["status"] = "success"
            metadata["retry_note"] = "Recovered on retry"
            print(f"    [OK] {fname}")
            stats["recovered"] += 1

        elif "html" in content_type or "text" in content_type:
            # Try to find docs on page
            doc_links = find_downloadable_links(resp.text, resp.url)

            if doc_links:
                downloaded = 0
                for link in doc_links:
                    time.sleep(REQUEST_DELAY)
                    try:
                        dl_resp = session.get(link["url"], timeout=REQUEST_TIMEOUT,
                                              headers=HEADERS, allow_redirects=True)
                        dl_resp.raise_for_status()
                        parsed = urlparse(dl_resp.url)
                        fname = safe_filename(os.path.basename(parsed.path)) or "document"
                        fpath = os.path.join(row_dir, fname)
                        base, ext = os.path.splitext(fpath)
                        counter = 1
                        while os.path.exists(fpath):
                            fpath = f"{base}_{counter}{ext}"
                            counter += 1
                        with open(fpath, "wb") as f:
                            f.write(dl_resp.content)
                        metadata["files_downloaded"].append({
                            "filename": os.path.basename(fpath),
                            "source_url": link["url"],
                            "size_bytes": os.path.getsize(fpath),
                            "sha256": sha256_file(fpath),
                            "content_type": dl_resp.headers.get("Content-Type", ""),
                            "download_type": "retry_scraped"
                        })
                        downloaded += 1
                        print(f"    [OK] {os.path.basename(fpath)}")
                    except Exception as e:
                        print(f"    [FAIL] Download: {e}")

                if downloaded:
                    metadata["status"] = "success"
                    metadata["retry_note"] = f"Recovered {downloaded} files on retry"
                    stats["recovered"] += 1
                else:
                    stats["still_failed"] += 1
            else:
                # No doc links — save HTML and try PDF conversion
                html_path = os.path.join(row_dir, "page_content.html")
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)

                blocks = aggressive_extract(html_path)
                if blocks and len(blocks) >= 2:
                    doc_num = metadata.get("doc_number", "").strip()
                    if doc_num:
                        sname = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
                        pdf_name = f"{sname}_recovered.pdf"
                    else:
                        pdf_name = f"guidance_{folder_name}_recovered.pdf"

                    pdf_path = os.path.join(row_dir, pdf_name)
                    try:
                        create_pdf_from_blocks(blocks, metadata, pdf_path)
                        metadata["files_downloaded"].append({
                            "filename": pdf_name,
                            "source_url": resp.url,
                            "size_bytes": os.path.getsize(pdf_path),
                            "sha256": sha256_file(pdf_path),
                            "content_type": "application/pdf",
                            "download_type": "retry_html_to_pdf"
                        })
                        metadata["status"] = "success"
                        metadata["retry_note"] = "Recovered: HTML to PDF on retry"
                        print(f"    [OK] Converted to {pdf_name}")
                        stats["recovered"] += 1
                    except Exception as e:
                        print(f"    [ERROR] PDF conversion: {e}")
                        metadata["status"] = "no_documents"
                        stats["saved_html"] += 1
                else:
                    metadata["status"] = "no_documents"
                    print(f"    [HTML] Saved page, insufficient content for PDF")
                    stats["saved_html"] += 1

        # Save updated metadata
        metadata["errors"] = []
        metadata["retry_at"] = datetime.now().isoformat()
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, default=str)

    print(f"\nPhase 2 Results: {stats['recovered']} recovered, "
          f"{stats['still_failed']} still failed, "
          f"{stats['saved_html']} saved as HTML")
    return stats


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ACF Guidance Recovery Script")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--reports", default=DEFAULT_REPORTS)
    parser.add_argument("--convert-only", action="store_true")
    parser.add_argument("--retry-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output)
    reports_dir = os.path.abspath(args.reports)
    os.makedirs(reports_dir, exist_ok=True)

    print("=" * 60)
    print("ACF GUIDANCE — RECOVERY SCRIPT")
    print("=" * 60)

    all_stats = {}
    start_time = datetime.now()

    if not args.retry_only:
        all_stats["conversion"] = convert_remaining_html(output_dir, args.dry_run)

    if not args.convert_only:
        all_stats["retry"] = retry_failed_rows(output_dir, args.dry_run)

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n{'=' * 60}")
    print(f"RECOVERY COMPLETE ({elapsed:.1f} seconds)")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"recovery_report_{timestamp}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(all_stats, f, indent=2)
    print(f"Report saved: {report_path}")


if __name__ == "__main__":
    sys.exit(main())
