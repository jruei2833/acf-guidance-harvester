#!/usr/bin/env python3
"""
ACF Guidance — Smart URL Matcher
===================================
Many of the 55 remaining "failed" rows have been moved to new URLs on ACF.gov.
This script:
1. Scrapes known index/listing pages for each office
2. Matches failed rows by document number or title
3. Downloads from the correct new URL

Usage:
  python smart_match.py               # Run full matching + download
  python smart_match.py --dry-run     # Preview matches only
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

SKIP_URL_PATTERNS = {
    "vulnerability-disclosure-policy", "/privacy-policy", "/accessibility",
    "/foia", "/disclaimers", "/nofear", "/plainlanguage", "/usa.gov",
    "/digital-strategy", "/web-policies-and-important-links",
    "facebook.com", "twitter.com", "youtube.com", "linkedin.com", "instagram.com",
}

DOWNLOAD_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt",
    ".rtf", ".ppt", ".pptx", ".zip"
}

# Index pages to scrape for each office
INDEX_PAGES = {
    "OCS": [
        "https://acf.gov/ocs/policy-guidance/liheap-dcls",
        "https://acf.gov/ocs/policy-guidance/liheap-action-transmittals",
        "https://acf.gov/ocs/policy-guidance/liheap-information-memoranda",
        "https://acf.gov/ocs/policy-guidance/ocs-dear-colleague-letters",
        "https://acf.gov/ocs/policy-guidance/lihwap-dear-colleague-letters",
    ],
    "ORR": [
        "https://acf.gov/orr/policy-guidance/dear-colleague-letters",
        "https://acf.gov/orr/policy-guidance/state-letters",
        "https://acf.gov/orr/grant-funding/formula-grant-allocations",
    ],
    "OCC": [
        "https://acf.gov/occ/policy-guidance/information-memoranda",
        "https://acf.gov/occ/policy-guidance/program-instructions",
    ],
    "CB": [
        "https://acf.gov/cb/policy-guidance",
    ],
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


def normalize_doc_id(text):
    """Normalize document numbers for matching.
    Strips whitespace, lowercases, removes common prefixes."""
    if not text:
        return ""
    text = text.lower().strip()
    # Remove common punctuation variations
    text = re.sub(r'[_\s]+', '-', text)
    text = re.sub(r'-+', '-', text)
    text = text.strip('-')
    return text


def extract_doc_ids_from_url(url):
    """Extract potential document identifiers from a URL slug."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    slug = path.split("/")[-1] if "/" in path else path

    # Common patterns
    ids = set()
    ids.add(normalize_doc_id(slug))

    # Try to extract DCL/IM/PI/AT numbers from slug
    patterns = [
        r'(liheap-dcl-\d{4}-\d+)',
        r'(ocs-dcl-\d{4}-\d+)',
        r'(dcl-\d{2,4}-\d+)',
        r'(liheap-im-\d{4}-\d+)',
        r'(liheap-at-\d{4}-\d+)',
        r'(ccdf-acf-[a-z]+-\d{4}-\d+)',
        r'(acf-[a-z]+-[a-z]+-\d{2,4}-\d+)',
        r'(\d{2,4}-\d{2})',  # e.g. 24-11, 15-10
    ]
    for pat in patterns:
        matches = re.findall(pat, slug)
        for m in matches:
            ids.add(normalize_doc_id(m))

    return ids


def scrape_index_page(url, session):
    """Scrape an index/listing page and return all links with their text."""
    print(f"  Scraping index: {url[:70]}")
    try:
        resp = session.get(url, timeout=30, headers=HEADERS, allow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        print(f"    [ERROR] Could not load: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links = []

    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue

        full_url = urljoin(url, href)

        # Skip social/footer links
        if any(p in full_url.lower() for p in SKIP_URL_PATTERNS):
            continue

        link_text = tag.get_text(strip=True)

        # Extract IDs from both URL and link text
        url_ids = extract_doc_ids_from_url(full_url)
        text_ids = set()

        # Try to find doc numbers in link text
        text_patterns = [
            r'(LIHEAP[- ]DCL[- ]\d{4}[- ]\d+)',
            r'(OCS[- ]DCL[- ]\d{4}[- ]\d+)',
            r'(DCL[- ]\d{2,4}[- ]\d+)',
            r'(LIHEAP[- ]IM[- ]\d{4}[- ]\d+)',
            r'(LIHEAP[- ]AT[- ]\d{4}[- ]\d+)',
            r'(CCDF[- ]ACF[- ][A-Z]+[- ]\d{4}[- ]\d+)',
            r'(ACF[- ][A-Z]+[- ][A-Z]+[- ]\d{2,4}[- ]\d+)',
        ]
        for pat in text_patterns:
            for m in re.findall(pat, link_text, re.IGNORECASE):
                text_ids.add(normalize_doc_id(m))

        all_ids = url_ids | text_ids

        links.append({
            "url": full_url,
            "text": link_text[:200],
            "ids": all_ids
        })

    print(f"    Found {len(links)} links")
    return links


def build_link_index(session, offices_needed):
    """Build a searchable index of links from all relevant index pages."""
    print("\n" + "=" * 60)
    print("BUILDING LINK INDEX FROM ACF LISTING PAGES")
    print("=" * 60)

    all_links = []

    for office in offices_needed:
        pages = INDEX_PAGES.get(office, [])
        for page_url in pages:
            links = scrape_index_page(page_url, session)
            for link in links:
                link["office"] = office
            all_links.extend(links)
            time.sleep(1.5)

    print(f"\n  Total links indexed: {len(all_links)}")
    return all_links


def match_failed_row(metadata, link_index):
    """Try to match a failed row to a link in the index."""
    doc_number = metadata.get("doc_number", "")
    title = metadata.get("title", "")
    original_url = metadata["urls"][0]["url"] if metadata.get("urls") else ""

    # Normalize our doc number
    norm_doc = normalize_doc_id(doc_number)
    norm_url_ids = extract_doc_ids_from_url(original_url)

    # Also extract identifiers from the original URL slug
    all_search_ids = {norm_doc} | norm_url_ids
    all_search_ids.discard("")

    # Search by ID match
    for link in link_index:
        if all_search_ids & link["ids"]:
            return link

    # Search by title substring match (fuzzy)
    if title and len(title) > 15:
        title_words = set(title.lower().split())
        # Remove common words
        title_words -= {"the", "of", "and", "for", "a", "an", "to", "in", "on",
                        "at", "is", "by", "with", "from", "or", "as", "acf"}

        best_match = None
        best_score = 0

        for link in link_index:
            link_words = set(link["text"].lower().split())
            overlap = len(title_words & link_words)
            if overlap > best_score and overlap >= 3:
                best_score = overlap
                best_match = link

        if best_match and best_score >= 3:
            return best_match

    return None


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
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}} | {self.source_url[:80]}",
                  new_x="LMARGIN", new_y="TOP", align="C")


def extract_and_create_pdf(html_content, metadata, output_path, source_url):
    """Extract text from HTML and create PDF."""
    soup = BeautifulSoup(html_content, "html.parser")

    for tag in soup.find_all(["script", "style", "nav", "footer", "noscript",
                               "iframe", "svg", "button"]):
        tag.decompose()
    for tag in soup.find_all(attrs={"class": re.compile(
            r"header|banner|nav|menu|sidebar|footer|breadcrumb|skip|cookie|usa-banner",
            re.I)}):
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

    if len(blocks) < 2:
        return False

    title = metadata.get("title", "")
    doc_number = metadata.get("doc_number", "")
    office = metadata.get("office", "")
    issue_date = metadata.get("issue_date", "")

    pdf = GuidancePDF(title=title, doc_number=doc_number,
                      office=office, source_url=source_url)
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
    return True


def process_matched_row(metadata, matched_link, row_dir, session):
    """Download content from the matched URL."""
    url = matched_link["url"]
    meta_path = os.path.join(row_dir, "metadata.json")

    print(f"    Fetching: {url[:70]}")
    try:
        resp = session.get(url, timeout=30, headers=HEADERS, allow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        print(f"    [FAIL] Could not load matched URL: {e}")
        return False

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
            "source_url": url,
            "size_bytes": os.path.getsize(fpath),
            "sha256": sha256_file(fpath),
            "content_type": content_type,
            "download_type": "smart_match_direct"
        })
        metadata["status"] = "success"
        metadata["recovery_method"] = "smart_url_match"
        print(f"    [OK] {fname} ({os.path.getsize(fpath):,} bytes)")
        return True

    elif "html" in content_type:
        # Look for downloadable links on the page
        soup = BeautifulSoup(resp.text, "html.parser")
        doc_links = []
        seen_urls = set()
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            full = urljoin(resp.url, href)
            if any(p in full.lower() for p in SKIP_URL_PATTERNS):
                continue
            parsed = urlparse(full)
            ext = os.path.splitext(parsed.path.lower())[1]
            if ext in DOWNLOAD_EXTENSIONS and full not in seen_urls:
                doc_links.append({"url": full, "text": tag.get_text(strip=True)[:100]})
                seen_urls.add(full)

        downloaded = 0
        for link in doc_links:
            try:
                time.sleep(1)
                dl = session.get(link["url"], timeout=30, headers=HEADERS,
                                 allow_redirects=True)
                dl.raise_for_status()
                parsed = urlparse(dl.url)
                fname = safe_filename(os.path.basename(parsed.path)) or "document"
                fpath = os.path.join(row_dir, fname)
                base, ext = os.path.splitext(fpath)
                counter = 1
                while os.path.exists(fpath):
                    fpath = f"{base}_{counter}{ext}"
                    counter += 1
                with open(fpath, "wb") as f:
                    f.write(dl.content)
                fsize = os.path.getsize(fpath)
                if fsize > 100:
                    metadata["files_downloaded"].append({
                        "filename": os.path.basename(fpath),
                        "source_url": link["url"],
                        "size_bytes": fsize,
                        "sha256": sha256_file(fpath),
                        "content_type": dl.headers.get("Content-Type", ""),
                        "download_type": "smart_match_scraped"
                    })
                    downloaded += 1
                    print(f"    [OK] {os.path.basename(fpath)} ({fsize:,} bytes)")
                else:
                    os.remove(fpath)
            except Exception:
                pass

        if downloaded:
            metadata["status"] = "success"
            metadata["recovery_method"] = f"smart_match_{downloaded}_files"
            return True

        # No files — save HTML and convert to PDF
        html_path = os.path.join(row_dir, "page_content.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(resp.text)

        doc_num = metadata.get("doc_number", "").strip()
        if doc_num:
            sname = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
            pdf_name = f"{sname}_matched.pdf"
        else:
            pdf_name = f"guidance_{metadata['folder']}_matched.pdf"
        pdf_path = os.path.join(row_dir, pdf_name)

        if extract_and_create_pdf(resp.text, metadata, pdf_path, url):
            metadata["files_downloaded"].append({
                "filename": pdf_name,
                "source_url": url,
                "size_bytes": os.path.getsize(pdf_path),
                "sha256": sha256_file(pdf_path),
                "content_type": "application/pdf",
                "download_type": "smart_match_html_to_pdf"
            })
            metadata["status"] = "success"
            metadata["recovery_method"] = "smart_match_html_to_pdf"
            print(f"    [OK] Converted to {pdf_name}")
            return True
        else:
            print(f"    [FAIL] Page loaded but no extractable content")
            return False

    return False


def main():
    parser = argparse.ArgumentParser(description="ACF Smart URL Matcher")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--reports", default=DEFAULT_REPORTS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output)
    reports_dir = os.path.abspath(args.reports)
    os.makedirs(reports_dir, exist_ok=True)

    print("=" * 60)
    print("ACF GUIDANCE — SMART URL MATCHER")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    # Load all failed rows
    failed_rows = []
    offices_needed = set()

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
            meta["_row_dir"] = row_dir
            meta["_meta_path"] = meta_path
            failed_rows.append(meta)
            office = meta.get("office", "")
            if office in INDEX_PAGES:
                offices_needed.add(office)

    print(f"\n  Failed/no_documents rows: {len(failed_rows)}")
    print(f"  Offices to search: {', '.join(sorted(offices_needed))}")

    # Build link index
    link_index = build_link_index(session, offices_needed)

    # Match and process
    print(f"\n{'=' * 60}")
    print("MATCHING AND DOWNLOADING")
    print("=" * 60)

    stats = {"matched": 0, "recovered": 0, "no_match": 0, "match_failed": 0}
    start_time = datetime.now()

    for i, meta in enumerate(failed_rows):
        folder = meta["folder"]
        doc_num = meta.get("doc_number", "")
        title = meta.get("title", "")[:50]
        url = meta["urls"][0]["url"][:60] if meta.get("urls") else "NO URL"

        print(f"\n  [{i+1}/{len(failed_rows)}] Row {folder}: {doc_num or title or url}")

        match = match_failed_row(meta, link_index)

        if match:
            stats["matched"] += 1
            print(f"    MATCHED -> {match['url'][:70]}")
            print(f"    Link text: {match['text'][:60]}")

            if args.dry_run:
                stats["recovered"] += 1
                continue

            time.sleep(1.5)
            success = process_matched_row(meta, match, meta["_row_dir"], session)

            if success:
                meta["errors"] = []
                meta["smart_match_at"] = datetime.now().isoformat()
                meta["matched_url"] = match["url"]
                with open(meta["_meta_path"], "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2, default=str)
                stats["recovered"] += 1
            else:
                stats["match_failed"] += 1
        else:
            print(f"    NO MATCH found in index pages")
            stats["no_match"] += 1

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n{'=' * 60}")
    print(f"SMART MATCH COMPLETE ({elapsed:.1f} seconds)")
    print("=" * 60)
    print(f"  Matched: {stats['matched']}")
    print(f"  Recovered: {stats['recovered']}")
    print(f"  Match failed: {stats['match_failed']}")
    print(f"  No match: {stats['no_match']}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"smart_match_{timestamp}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"stats": stats, "duration": elapsed}, f, indent=2)
    print(f"Report saved: {report_path}")


if __name__ == "__main__":
    sys.exit(main())
