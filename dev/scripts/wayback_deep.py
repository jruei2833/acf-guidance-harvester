#!/usr/bin/env python3
"""
ACF Guidance — Wayback Machine Deep Recovery
===============================================
For the final holdout rows, tries multiple Wayback Machine strategies:
1. CDX API search with original URL
2. CDX API search with redirect URL (from smart_match metadata)
3. CDX API search with URL variations (/resource/ <-> /policy-guidance/, /archive/ prefix)
4. For each found snapshot, downloads the page AND any linked PDFs from the snapshot

Usage:
  python wayback_deep.py
  python wayback_deep.py --dry-run
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
}

DOWNLOAD_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt",
    ".rtf", ".ppt", ".pptx", ".zip"
}

SKIP_PATTERNS = {
    "vulnerability-disclosure", "privacy-policy", "accessibility",
    "foia", "disclaimers", "nofear", "plainlanguage", "usa.gov",
    "facebook.com", "twitter.com", "youtube.com", "linkedin.com",
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


def generate_url_variants(url):
    """Generate URL variants to search in Wayback."""
    variants = [url]
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # acf.gov <-> www.acf.hhs.gov
    if "acf.gov" in url and "acf.hhs.gov" not in url:
        variants.append(url.replace("acf.gov", "www.acf.hhs.gov", 1))
    elif "acf.hhs.gov" in url:
        variants.append(url.replace("www.acf.hhs.gov", "acf.gov", 1))

    # /resource/ <-> /policy-guidance/
    if "/resource/" in path:
        variants.append(url.replace("/resource/", "/policy-guidance/"))
    elif "/policy-guidance/" in path:
        variants.append(url.replace("/policy-guidance/", "/resource/"))

    # /archive/ prefix
    if "/archive/" not in path:
        parts = path.split("/", 2)
        if len(parts) >= 3:
            archive_path = f"/{parts[1]}/archive/{'/'.join(parts[2:])}"
            variants.append(f"https://acf.gov{archive_path}")

    return list(dict.fromkeys(variants))  # deduplicate preserving order


def search_wayback_cdx(url, session):
    """Search Wayback Machine CDX API for snapshots of a URL."""
    cdx_url = (
        f"https://web.archive.org/cdx/search/cdx?"
        f"url={url}&output=json&limit=10"
        f"&fl=timestamp,statuscode,original,mimetype"
        f"&filter=statuscode:200&collapse=digest"
    )
    try:
        resp = session.get(cdx_url, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            if len(data) > 1:  # first row is headers
                results = []
                for row in data[1:]:
                    results.append({
                        "timestamp": row[0],
                        "status": row[1],
                        "original": row[2],
                        "mimetype": row[3],
                        "wayback_url": f"https://web.archive.org/web/{row[0]}id_/{row[2]}"
                    })
                return results
    except Exception:
        pass
    return []


def download_from_wayback(wayback_url, row_dir, metadata, session):
    """Download a page from Wayback and extract content/files."""
    try:
        resp = session.get(wayback_url, timeout=30, headers=HEADERS)
        resp.raise_for_status()
    except Exception as e:
        return False

    content_type = resp.headers.get("Content-Type", "").lower()

    # If it's a direct file (PDF, etc.)
    if any(ct in content_type for ct in ["pdf", "msword", "spreadsheet",
                                          "zip", "octet-stream"]):
        parsed = urlparse(wayback_url)
        # Extract original filename from wayback URL
        orig_path = parsed.path.split("/id_/")[-1] if "/id_/" in parsed.path else parsed.path
        fname = safe_filename(os.path.basename(urlparse(orig_path).path)) or "document.pdf"
        fpath = os.path.join(row_dir, fname)

        base, ext = os.path.splitext(fpath)
        counter = 1
        while os.path.exists(fpath):
            fpath = f"{base}_{counter}{ext}"
            counter += 1

        with open(fpath, "wb") as f:
            f.write(resp.content)

        fsize = os.path.getsize(fpath)
        if fsize > 500:
            metadata["files_downloaded"].append({
                "filename": os.path.basename(fpath),
                "source_url": wayback_url,
                "size_bytes": fsize,
                "sha256": sha256_file(fpath),
                "content_type": content_type,
                "download_type": "wayback_direct"
            })
            print(f"    [OK] {os.path.basename(fpath)} ({fsize:,} bytes)")
            return True
        else:
            os.remove(fpath)

    elif "html" in content_type:
        # Parse for downloadable links
        soup = BeautifulSoup(resp.text, "html.parser")
        doc_links = []
        seen = set()

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href or href.startswith("#") or href.startswith("mailto:"):
                continue

            # Resolve relative URLs against wayback
            full = urljoin(wayback_url, href)

            # Check if it's a file link
            # For wayback URLs, the original URL is embedded after /web/TIMESTAMP/
            if "/web/" in full:
                # Extract original URL from wayback link
                match = re.search(r'/web/\d+[a-z]*/(.+)', full)
                if match:
                    orig_url = match.group(1)
                    if not orig_url.startswith("http"):
                        orig_url = "https://" + orig_url
                    parsed_orig = urlparse(orig_url)
                    ext = os.path.splitext(parsed_orig.path.lower())[1]
                    if ext in DOWNLOAD_EXTENSIONS and orig_url not in seen:
                        doc_links.append({
                            "wayback_url": full,
                            "original_url": orig_url,
                            "text": tag.get_text(strip=True)[:100],
                            "ext": ext
                        })
                        seen.add(orig_url)
            else:
                parsed = urlparse(full)
                ext = os.path.splitext(parsed.path.lower())[1]
                if ext in DOWNLOAD_EXTENSIONS and full not in seen:
                    if not any(p in full.lower() for p in SKIP_PATTERNS):
                        doc_links.append({
                            "wayback_url": full,
                            "original_url": full,
                            "text": tag.get_text(strip=True)[:100],
                            "ext": ext
                        })
                        seen.add(full)

        downloaded = 0
        for link in doc_links[:15]:
            try:
                time.sleep(0.5)
                dl = session.get(link["wayback_url"], timeout=30, headers=HEADERS)
                dl.raise_for_status()

                fname = safe_filename(os.path.basename(
                    urlparse(link["original_url"]).path)) or f"document{link['ext']}"
                fpath = os.path.join(row_dir, fname)

                base, ext = os.path.splitext(fpath)
                counter = 1
                while os.path.exists(fpath):
                    fpath = f"{base}_{counter}{ext}"
                    counter += 1

                with open(fpath, "wb") as f:
                    f.write(dl.content)

                fsize = os.path.getsize(fpath)
                if fsize > 500:
                    metadata["files_downloaded"].append({
                        "filename": os.path.basename(fpath),
                        "source_url": link["original_url"],
                        "size_bytes": fsize,
                        "sha256": sha256_file(fpath),
                        "content_type": dl.headers.get("Content-Type", ""),
                        "download_type": "wayback_scraped"
                    })
                    downloaded += 1
                    print(f"    [OK] {os.path.basename(fpath)} ({fsize:,} bytes)")
                else:
                    os.remove(fpath)
            except Exception:
                pass

        if downloaded:
            return True

        # No files found — convert HTML to PDF
        return convert_html_to_pdf(resp.text, wayback_url, row_dir, metadata)

    return False


def convert_html_to_pdf(html_content, source_url, row_dir, metadata):
    """Extract text and create PDF from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove wayback toolbar
    for tag in soup.find_all(id=re.compile(r"wm-ipp|wayback|donate")):
        tag.decompose()
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

    doc_num = metadata.get("doc_number", "").strip()
    if doc_num:
        sname = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
        pdf_name = f"{sname}_wayback.pdf"
    else:
        pdf_name = f"guidance_{metadata['folder']}_wayback.pdf"

    pdf_path = os.path.join(row_dir, pdf_name)

    try:
        title = metadata.get("title", "")
        office = metadata.get("office", "")
        issue_date = metadata.get("issue_date", "")

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=20)
        pdf.add_page()

        if title:
            pdf.set_font("Helvetica", "B", 14)
            safe = title.encode('latin-1', 'replace').decode('latin-1')
            pdf.multi_cell(0, 7, safe, new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

        meta_parts = []
        if doc_num: meta_parts.append(f"Document: {doc_num}")
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

        pdf.output(pdf_path)
        fsize = os.path.getsize(pdf_path)

        metadata["files_downloaded"].append({
            "filename": pdf_name,
            "source_url": source_url,
            "size_bytes": fsize,
            "sha256": sha256_file(pdf_path),
            "content_type": "application/pdf",
            "download_type": "wayback_html_to_pdf"
        })
        print(f"    [OK] Converted to {pdf_name} ({fsize:,} bytes, {len(blocks)} blocks)")
        return True

    except Exception as e:
        print(f"    [ERROR] PDF creation: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="ACF Wayback Deep Recovery")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--reports", default=DEFAULT_REPORTS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output)
    reports_dir = os.path.abspath(args.reports)
    os.makedirs(reports_dir, exist_ok=True)

    print("=" * 60)
    print("ACF — WAYBACK MACHINE DEEP RECOVERY")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    # Collect failed rows
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
            meta["_row_dir"] = row_dir
            meta["_meta_path"] = meta_path
            targets.append(meta)

    print(f"  Found {len(targets)} rows to search in Wayback Machine\n")

    if not targets:
        print("  Nothing to do.")
        return

    stats = {"recovered": 0, "failed": 0, "snapshots_found": 0}
    start_time = datetime.now()

    for i, meta in enumerate(targets):
        folder = meta["folder"]
        doc_num = meta.get("doc_number", "")
        title = meta.get("title", "")[:40]
        original_url = meta["urls"][0]["url"] if meta.get("urls") else ""

        print(f"  [{i+1}/{len(targets)}] Row {folder}: {doc_num or title}")

        if args.dry_run:
            variants = generate_url_variants(original_url)
            print(f"    URLs to search: {len(variants)}")
            for v in variants[:3]:
                print(f"      {v[:70]}")
            continue

        # Generate all URL variants to search
        urls_to_search = generate_url_variants(original_url)

        # Also add matched_url if we have one from smart_match
        if meta.get("matched_url"):
            matched_variants = generate_url_variants(meta["matched_url"])
            for mv in matched_variants:
                if mv not in urls_to_search:
                    urls_to_search.append(mv)

        # Search Wayback for each variant
        all_snapshots = []
        for search_url in urls_to_search:
            time.sleep(1)
            snapshots = search_wayback_cdx(search_url, session)
            if snapshots:
                print(f"    Found {len(snapshots)} snapshots for: {search_url[:60]}")
                all_snapshots.extend(snapshots)
                stats["snapshots_found"] += len(snapshots)
                break  # Found some, no need to search more variants

        if not all_snapshots:
            print(f"    No Wayback snapshots found for any URL variant")
            stats["failed"] += 1
            continue

        # Try snapshots from newest to oldest
        all_snapshots.sort(key=lambda s: s["timestamp"], reverse=True)

        success = False
        for snap in all_snapshots[:3]:  # Try up to 3 snapshots
            wb_url = snap["wayback_url"]
            print(f"    Trying snapshot {snap['timestamp']}: {snap['original'][:50]}")

            time.sleep(1.5)
            success = download_from_wayback(wb_url, meta["_row_dir"], meta, session)
            if success:
                break

        if success:
            meta["status"] = "success"
            meta["recovery_method"] = "wayback_deep"
            meta["errors"] = []
            meta["wayback_recovery_at"] = datetime.now().isoformat()
            stats["recovered"] += 1
        else:
            stats["failed"] += 1

        # Save metadata
        with open(meta["_meta_path"], "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, default=str)

        if (i + 1) % 10 == 0:
            print(f"\n  --- Progress: {i+1}/{len(targets)} "
                  f"(recovered: {stats['recovered']}, failed: {stats['failed']}) ---")

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n{'=' * 60}")
    print(f"WAYBACK DEEP RECOVERY COMPLETE ({elapsed:.1f} seconds)")
    print("=" * 60)
    print(f"  Snapshots found: {stats['snapshots_found']}")
    print(f"  Recovered: {stats['recovered']}")
    print(f"  Still failed: {stats['failed']}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"wayback_deep_{timestamp}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"stats": stats, "duration": elapsed}, f, indent=2)
    print(f"Report saved: {report_path}")


if __name__ == "__main__":
    main()
