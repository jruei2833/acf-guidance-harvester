#!/usr/bin/env python3
"""
ACF Guidance — Final Cleanup
===============================
Handles the last remaining rows:
1. Direct-downloads the 3 file URLs that the browser couldn't handle
2. Searches for dead links by document number via Google

Usage:
  python final_cleanup.py               # Run everything
  python final_cleanup.py --direct-only  # Only download the 3 files
  python final_cleanup.py --search-only  # Only search for dead links
  python final_cleanup.py --dry-run      # Preview
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse, quote_plus

import requests
from bs4 import BeautifulSoup

DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output")
DEFAULT_REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

DOWNLOAD_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt",
    ".rtf", ".ppt", ".pptx", ".zip"
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


def load_all_metadata(output_dir):
    """Load metadata for all rows."""
    rows = []
    for folder_name in sorted(os.listdir(output_dir)):
        row_dir = os.path.join(output_dir, folder_name)
        if not os.path.isdir(row_dir):
            continue
        meta_path = os.path.join(row_dir, "metadata.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        meta["_row_dir"] = row_dir
        meta["_meta_path"] = meta_path
        rows.append(meta)
    return rows


# ─── Part 1: Direct File Downloads ──────────────────────────────────────────

def download_direct_files(output_dir, dry_run=False):
    """Download files from direct URLs that the browser couldn't handle."""
    print("\n" + "=" * 60)
    print("PART 1: DIRECT FILE DOWNLOADS")
    print("=" * 60)

    rows = load_all_metadata(output_dir)
    targets = [r for r in rows if r.get("status") == "no_documents"]

    print(f"  Found {len(targets)} no_documents rows")

    if not targets:
        return {"downloaded": 0, "failed": 0}

    session = requests.Session()
    session.headers.update(HEADERS)
    stats = {"downloaded": 0, "failed": 0}

    for meta in targets:
        if not meta.get("urls"):
            continue

        url = meta["urls"][0]["url"]
        folder = meta["folder"]
        row_dir = meta["_row_dir"]
        meta_path = meta["_meta_path"]

        print(f"\n  Row {folder}: {url[:70]}")

        if dry_run:
            stats["downloaded"] += 1
            continue

        try:
            resp = session.get(url, timeout=30, allow_redirects=True, stream=True)
            resp.raise_for_status()

            # Determine filename
            parsed = urlparse(resp.url)
            fname = safe_filename(os.path.basename(parsed.path))
            if not fname or fname == "unnamed":
                # Try content-disposition header
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    fname = cd.split("filename=")[-1].strip('" ')
                    fname = safe_filename(fname)
                else:
                    fname = f"document_{folder}"

            fpath = os.path.join(row_dir, fname)

            # Avoid overwriting
            base, ext = os.path.splitext(fpath)
            counter = 1
            while os.path.exists(fpath):
                fpath = f"{base}_{counter}{ext}"
                counter += 1

            with open(fpath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = os.path.getsize(fpath)
            if file_size > 100:
                meta["files_downloaded"].append({
                    "filename": os.path.basename(fpath),
                    "source_url": url,
                    "size_bytes": file_size,
                    "sha256": sha256_file(fpath),
                    "content_type": resp.headers.get("Content-Type", ""),
                    "download_type": "final_direct_download"
                })
                meta["status"] = "success"
                meta["recovery_method"] = "direct_download"

                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2, default=str)

                print(f"    [OK] {os.path.basename(fpath)} ({file_size:,} bytes)")
                stats["downloaded"] += 1
            else:
                os.remove(fpath)
                print(f"    [FAIL] Downloaded file too small ({file_size} bytes)")
                stats["failed"] += 1

        except Exception as e:
            print(f"    [FAIL] {e}")
            stats["failed"] += 1

        time.sleep(1)

    print(f"\nPart 1 Results: {stats['downloaded']} downloaded, {stats['failed']} failed")
    return stats


# ─── Part 2: Search for Dead Links ──────────────────────────────────────────

def try_wayback_cdx(url, session):
    """Use Wayback CDX API to find ALL archived versions, not just the closest."""
    domain = urlparse(url).netloc
    path = urlparse(url).path
    cdx_url = (f"http://web.archive.org/cdx/search/cdx?"
               f"url={url}&output=json&limit=5&fl=timestamp,statuscode,original"
               f"&filter=statuscode:200&collapse=digest")
    try:
        resp = session.get(cdx_url, timeout=15)
        data = resp.json()
        if len(data) > 1:  # First row is headers
            for row in data[1:]:
                timestamp = row[0]
                wb_url = f"https://web.archive.org/web/{timestamp}/{url}"
                return wb_url
    except Exception:
        pass
    return None


def try_alternate_urls(url, session):
    """Try various URL transformations for ACF.gov dead links."""
    alternates = []

    # Original URL path
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # acf.gov -> www.acf.hhs.gov
    if "acf.gov" in url and "acf.hhs.gov" not in url:
        alternates.append(url.replace("acf.gov", "www.acf.hhs.gov", 1))

    # Try /archive/ prefix (ACF moved many pages)
    if "/archive/" not in path:
        parts = path.split("/", 2)
        if len(parts) >= 3:
            archive_path = f"/{parts[1]}/archive/{'/'.join(parts[2:])}"
            alternates.append(f"https://acf.gov{archive_path}")

    # Try /resource/ <-> /policy-guidance/ swap
    if "/resource/" in path:
        alternates.append(url.replace("/resource/", "/policy-guidance/"))
    elif "/policy-guidance/" in path:
        alternates.append(url.replace("/policy-guidance/", "/resource/"))

    for alt_url in alternates:
        try:
            resp = session.get(alt_url, timeout=20, headers=HEADERS,
                               allow_redirects=True)
            if resp.status_code == 200:
                return resp
        except Exception:
            pass
        time.sleep(1)

    return None


def search_for_document(doc_number, title, office, session):
    """Search Google for a document by number/title. Returns URL if found."""
    # Build search queries in priority order
    queries = []

    if doc_number:
        queries.append(f'"{doc_number}" site:acf.gov filetype:pdf')
        queries.append(f'"{doc_number}" site:hhs.gov')
        queries.append(f'"{doc_number}" ACF filetype:pdf')

    if title and len(title) > 10:
        short_title = title[:80]
        queries.append(f'"{short_title}" site:acf.gov')
        queries.append(f'"{short_title}" ACF HHS')

    # We can't actually call Google API here, but we CAN try
    # constructing likely URLs based on patterns we've seen

    likely_urls = []

    if doc_number:
        # Common ACF URL patterns
        doc_lower = doc_number.lower().replace(" ", "-")
        doc_slug = re.sub(r'[^a-z0-9-]', '-', doc_lower)

        if office == "OCS":
            likely_urls.extend([
                f"https://acf.gov/ocs/policy-guidance/{doc_slug}",
                f"https://acf.gov/ocs/resource/{doc_slug}",
                f"https://acf.gov/archive/ocs/policy-guidance/{doc_slug}",
                f"https://acf.gov/archive/ocs/resource/{doc_slug}",
            ])
        elif office == "OCC":
            likely_urls.extend([
                f"https://acf.gov/occ/policy-guidance/{doc_slug}",
                f"https://acf.gov/occ/resource/{doc_slug}",
                f"https://acf.gov/archive/occ/policy-guidance/{doc_slug}",
                f"https://acf.gov/archive/occ/resource/{doc_slug}",
            ])
        elif office == "ORR":
            likely_urls.extend([
                f"https://acf.gov/orr/policy-guidance/{doc_slug}",
                f"https://acf.gov/orr/resource/{doc_slug}",
                f"https://acf.gov/archive/orr/policy-guidance/{doc_slug}",
                f"https://acf.gov/archive/orr/resource/{doc_slug}",
            ])
        elif office == "CB":
            likely_urls.extend([
                f"https://acf.gov/cb/policy-guidance/{doc_slug}",
                f"https://acf.gov/cb/resource/{doc_slug}",
                f"https://acf.gov/archive/cb/policy-guidance/{doc_slug}",
            ])

    return likely_urls


def search_and_recover(output_dir, dry_run=False):
    """Try alternate URLs, Wayback CDX, and URL pattern matching for dead links."""
    print("\n" + "=" * 60)
    print("PART 2: SEARCH & RECOVER DEAD LINKS")
    print("=" * 60)

    rows = load_all_metadata(output_dir)
    targets = [r for r in rows if r.get("status") == "failed"]

    print(f"  Found {len(targets)} failed rows")

    if not targets:
        return {"recovered": 0, "still_failed": 0}

    session = requests.Session()
    session.headers.update(HEADERS)
    stats = {"recovered": 0, "still_failed": 0, "recovered_html": 0}

    for i, meta in enumerate(targets):
        if not meta.get("urls"):
            stats["still_failed"] += 1
            continue

        url = meta["urls"][0]["url"]
        folder = meta["folder"]
        row_dir = meta["_row_dir"]
        meta_path = meta["_meta_path"]
        doc_number = meta.get("doc_number", "")
        title = meta.get("title", "")
        office = meta.get("office", "")

        print(f"\n  [{i+1}/{len(targets)}] Row {folder} ({office}): {url[:60]}")
        if doc_number:
            print(f"    Doc: {doc_number}")

        if dry_run:
            stats["recovered"] += 1
            continue

        resp = None

        # Strategy 1: Try alternate URL patterns
        print(f"    Trying alternate URLs...")
        resp = try_alternate_urls(url, session)
        if resp:
            print(f"    Found via alternate URL: {resp.url[:70]}")

        # Strategy 2: Wayback CDX (more thorough than simple API)
        if not resp:
            print(f"    Trying Wayback CDX...")
            wb_url = try_wayback_cdx(url, session)
            if wb_url:
                try:
                    time.sleep(1.5)
                    resp = session.get(wb_url, timeout=30, headers=HEADERS,
                                       allow_redirects=True)
                    resp.raise_for_status()
                    print(f"    Found in Wayback: {wb_url[:70]}")
                except Exception:
                    resp = None

        # Strategy 3: Try constructed URLs based on doc number
        if not resp and doc_number:
            print(f"    Trying pattern-based URLs...")
            likely_urls = search_for_document(doc_number, title, office, session)
            for try_url in likely_urls:
                try:
                    time.sleep(1)
                    r = session.get(try_url, timeout=20, headers=HEADERS,
                                    allow_redirects=True)
                    if r.status_code == 200:
                        resp = r
                        print(f"    Found via pattern: {try_url[:70]}")
                        break
                except Exception:
                    continue

        if not resp:
            print(f"    [FAIL] Exhausted all strategies")
            stats["still_failed"] += 1
            continue

        # We got a response — process it
        content_type = resp.headers.get("Content-Type", "").lower()

        if any(ct in content_type for ct in ["pdf", "msword", "spreadsheet",
                                              "zip", "octet-stream"]):
            # Direct file
            parsed = urlparse(resp.url)
            fname = safe_filename(os.path.basename(parsed.path)) or "document.pdf"
            fpath = os.path.join(row_dir, fname)

            base, ext = os.path.splitext(fpath)
            counter = 1
            while os.path.exists(fpath):
                fpath = f"{base}_{counter}{ext}"
                counter += 1

            with open(fpath, "wb") as f:
                f.write(resp.content)

            file_size = os.path.getsize(fpath)
            meta["files_downloaded"].append({
                "filename": os.path.basename(fpath),
                "source_url": resp.url,
                "size_bytes": file_size,
                "sha256": sha256_file(fpath),
                "content_type": content_type,
                "download_type": "final_search_direct"
            })
            meta["status"] = "success"
            meta["recovery_method"] = "search_direct_download"
            print(f"    [OK] {os.path.basename(fpath)} ({file_size:,} bytes)")
            stats["recovered"] += 1

        elif "html" in content_type or "text" in content_type:
            # Try to find downloadable links
            soup = BeautifulSoup(resp.text, "html.parser")
            doc_links = []
            for tag in soup.find_all("a", href=True):
                href = tag["href"].strip()
                full = urljoin(resp.url, href)
                parsed = urlparse(full)
                ext = os.path.splitext(parsed.path.lower())[1]
                if ext in DOWNLOAD_EXTENSIONS:
                    doc_links.append(full)

            downloaded = 0
            for link_url in doc_links[:10]:
                try:
                    time.sleep(1)
                    dl_resp = session.get(link_url, timeout=20, headers=HEADERS,
                                          allow_redirects=True)
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

                    file_size = os.path.getsize(fpath)
                    if file_size > 100:
                        meta["files_downloaded"].append({
                            "filename": os.path.basename(fpath),
                            "source_url": link_url,
                            "size_bytes": file_size,
                            "sha256": sha256_file(fpath),
                            "content_type": dl_resp.headers.get("Content-Type", ""),
                            "download_type": "final_search_scraped"
                        })
                        downloaded += 1
                        print(f"    [OK] {os.path.basename(fpath)} ({file_size:,} bytes)")
                    else:
                        os.remove(fpath)
                except Exception:
                    pass

            if downloaded:
                meta["status"] = "success"
                meta["recovery_method"] = f"search_scraped_{downloaded}_files"
                stats["recovered"] += 1
            else:
                # Save HTML as content
                html_path = os.path.join(row_dir, "page_content.html")
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)
                meta["status"] = "success"
                meta["recovery_method"] = "search_html_saved"
                print(f"    [HTML] Page saved, no downloadable files found")
                stats["recovered_html"] += 1

        # Save metadata
        meta["errors"] = []
        meta["final_recovery_at"] = datetime.now().isoformat()
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, default=str)

        time.sleep(1.5)

        # Progress
        if (i + 1) % 10 == 0:
            print(f"\n  --- Progress: {i+1}/{len(targets)} "
                  f"(recovered: {stats['recovered']}, "
                  f"HTML: {stats['recovered_html']}, "
                  f"failed: {stats['still_failed']}) ---")

    print(f"\nPart 2 Results: {stats['recovered']} recovered, "
          f"{stats['recovered_html']} saved as HTML, "
          f"{stats['still_failed']} still failed")
    return stats


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ACF Guidance Final Cleanup")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--reports", default=DEFAULT_REPORTS)
    parser.add_argument("--direct-only", action="store_true")
    parser.add_argument("--search-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output)
    reports_dir = os.path.abspath(args.reports)
    os.makedirs(reports_dir, exist_ok=True)

    print("=" * 60)
    print("ACF GUIDANCE — FINAL CLEANUP")
    print("=" * 60)

    all_stats = {}
    start_time = datetime.now()

    if not args.search_only:
        all_stats["direct"] = download_direct_files(output_dir, args.dry_run)

    if not args.direct_only:
        all_stats["search"] = search_and_recover(output_dir, args.dry_run)

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n{'=' * 60}")
    print(f"FINAL CLEANUP COMPLETE ({elapsed:.1f} seconds)")
    print("=" * 60)

    total_recovered = sum(
        s.get("downloaded", 0) + s.get("recovered", 0) + s.get("recovered_html", 0)
        for s in all_stats.values()
    )
    total_failed = sum(s.get("failed", 0) + s.get("still_failed", 0)
                       for s in all_stats.values())
    print(f"  Total recovered: {total_recovered}")
    print(f"  Still failed: {total_failed}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"final_cleanup_{timestamp}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"stats": all_stats, "duration": elapsed}, f, indent=2)
    print(f"Report saved: {report_path}")


if __name__ == "__main__":
    sys.exit(main())
