#!/usr/bin/env python3
"""
ACF Guidance v2 — Targeted Repair
===================================
Two-phase repair of the existing v1 collection:

  Phase 1: Re-download the ~91 files that failed validation
           (DOJ blocks, Wayback junk, empty shells, HTML masquerades)

  Phase 2: Convert ~2,004 valid HTML files to clean PDFs using Playwright

Usage:
  python repair.py                        # Full repair (both phases)
  python repair.py --phase 1              # Re-download bad files only
  python repair.py --phase 2              # Convert HTMLs only
  python repair.py --dry-run              # Preview both phases
  python repair.py --phase 2 --no-playwright  # Use reportlab fallback

Requires:
  pip install openpyxl requests beautifulsoup4 playwright
  playwright install chromium
"""

import os
import sys
import json
import re
import time
import hashlib
import shutil
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urljoin
from collections import defaultdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from validate import (
    validate_content, detect_file_type, ValidationResult,
    check_for_junk_indicators, DOJ_INDICATORS, WAYBACK_ERROR_INDICATORS,
    extract_visible_text
)

# ── Configuration ────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if not os.path.exists(os.path.join(BASE_DIR, "output")):
    BASE_DIR = os.path.dirname(BASE_DIR)

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CATALOG_EXCEL = os.path.join(BASE_DIR, "catalog", "ACF_Guidance_Master_Catalog.xlsx")
INPUT_EXCEL = os.path.join(BASE_DIR, "input", "Sub-Regulatory Inventory for Programs_12112025.xlsx.xlsx")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
BACKUP_DIR = os.path.join(BASE_DIR, "repair_backups")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "application/pdf,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_DELAY = 1.5
WAYBACK_DELAY = 2.0

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DATE = "%H:%M:%S"

# Playwright clean CSS for Phase 2
CLEAN_CSS = """
<style>
  nav, header, footer, .breadcrumb, .breadcrumbs, .site-header,
  .site-footer, .skip-link, .skip-nav, .navbar, .menu, .sidebar,
  .social-share, .cookie-notice, .banner, .alert-banner,
  #skip-to-content, [role="navigation"], [role="banner"],
  [role="contentinfo"], .usa-banner, .usa-header, .usa-footer,
  .usa-nav, .usa-menu, .usa-accordion {
    display: none !important;
  }
  body {
    font-family: 'Times New Roman', serif;
    font-size: 12pt;
    line-height: 1.6;
    color: #000;
    background: #fff;
    margin: 0.75in;
    max-width: 100%;
  }
  main, article, .content, .main-content, #main-content,
  [role="main"] {
    max-width: 100%;
    margin: 0;
    padding: 0;
  }
  table { border-collapse: collapse; width: 100%; margin: 1em 0; }
  td, th { border: 1px solid #999; padding: 6px 8px; }
  a { color: #000; text-decoration: underline; }
</style>
"""

CLEANUP_JS = """
() => {
    const removeSelectors = [
        'nav', 'header', 'footer', '.breadcrumb', '.breadcrumbs',
        '.site-header', '.site-footer', '.skip-link', '.skip-nav',
        '.navbar', '.menu', '.sidebar', '.social-share', '.cookie-notice',
        '.banner', '.alert-banner', '#skip-to-content',
        '[role="navigation"]', '[role="banner"]', '[role="contentinfo"]',
        '.usa-banner', '.usa-header', '.usa-footer', '.usa-nav',
        '.usa-menu', '.usa-accordion', 'script', 'noscript',
        'iframe', '.overlay', '.modal'
    ];
    removeSelectors.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => el.remove());
    });
    document.querySelectorAll('[style*="display: none"], [style*="display:none"], [hidden]')
        .forEach(el => el.remove());
    document.body.style.margin = '0';
    document.body.style.padding = '20px';
    return document.body.innerText.trim().length;
}
"""


def sha256_file(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_filename(name, max_len=100):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', '_', name).strip('_.')
    return name[:max_len] if name else "document"


def setup_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["HEAD", "GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


# ══════════════════════════════════════════════════════════════════════════
# PHASE 1: Re-download bad files
# ══════════════════════════════════════════════════════════════════════════

def identify_bad_files(output_dir):
    """
    Scan the collection and identify files that failed validation.
    Returns list of dicts: {row, row_dir, bad_files, metadata}
    """
    logger = logging.getLogger("repair")
    bad_rows = []

    folders = sorted(f for f in os.listdir(output_dir)
                     if os.path.isdir(os.path.join(output_dir, f)) and f.isdigit())

    for folder in folders:
        row_dir = os.path.join(output_dir, folder)
        meta_path = os.path.join(row_dir, "metadata.json")

        metadata = {}
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except Exception:
                pass

        files = [f for f in os.listdir(row_dir)
                 if os.path.isfile(os.path.join(row_dir, f)) and f != "metadata.json"]

        if not files:
            continue

        # Check if ANY file in this row is valid
        has_valid = False
        bad_files = []

        for fname in files:
            fpath = os.path.join(row_dir, fname)
            vr = validate_content(fpath)
            if vr.valid:
                has_valid = True
            else:
                bad_files.append({
                    "filename": fname,
                    "filepath": fpath,
                    "reason": vr.reason,
                    "details": vr.details,
                })

        if bad_files:
            bad_rows.append({
                "row": folder,
                "row_dir": row_dir,
                "has_valid_file": has_valid,
                "bad_files": bad_files,
                "metadata": metadata,
            })

    return bad_rows


def guess_filename_from_response(response, original_url):
    """Guess filename from response headers or URL."""
    cd = response.headers.get("Content-Disposition", "")
    if "filename=" in cd:
        match = re.search(r'filename[*]?="?([^";\n]+)"?', cd)
        if match:
            return safe_filename(match.group(1))
    parsed = urlparse(response.url)
    path_name = os.path.basename(parsed.path)
    if path_name and "." in path_name:
        return safe_filename(path_name)
    parsed = urlparse(original_url)
    path_name = os.path.basename(parsed.path)
    if path_name and "." in path_name:
        return safe_filename(path_name)
    ct = response.headers.get("Content-Type", "").lower()
    ext = ".bin"
    for key, val in {"pdf": ".pdf", "msword": ".doc", "wordprocessingml": ".docx",
                     "spreadsheetml": ".xlsx", "html": ".html"}.items():
        if key in ct:
            ext = val
            break
    return f"document{ext}"


def try_redownload(row_data, session, row_dir, logger):
    """
    Try to re-download a file through the source chain:
    1. Direct URLs from metadata
    2. Wayback Machine (with validation)

    Returns (success: bool, files: list)
    """
    metadata = row_data.get("metadata", {})
    raw_urls = metadata.get("urls", [])
    urls = []
    for u in raw_urls:
        if isinstance(u, dict):
            s = u.get("url", "")
        else:
            s = str(u) if u else ""
        if s and s.startswith("http"):
            urls.append(s)

    if not urls:
        for fd in metadata.get("files_downloaded", []):
            u = fd.get("source_url", "")
            if isinstance(u, dict):
                u = u.get("url", "")
            if u and isinstance(u, str) and u.startswith("http"):
                urls.append(u)

    if not urls:
        return False, []

    downloaded = []

    # Try direct URLs first
    for url in urls[:5]:
        try:
            # Skip known-bad domains
            parsed = urlparse(url)
            if "justice.gov" in parsed.netloc.lower():
                continue

            time.sleep(REQUEST_DELAY)
            resp = session.get(url, timeout=30, allow_redirects=True)

            # Check if we got redirected to DOJ
            if "justice.gov" in urlparse(resp.url).netloc.lower():
                logger.debug(f"    Redirected to DOJ — skipping")
                continue

            if resp.status_code != 200:
                continue

            content_type = resp.headers.get("Content-Type", "").lower()

            # Direct file download
            if any(ct in content_type for ct in ["pdf", "msword", "officedocument",
                                                  "octet-stream", "ms-excel"]):
                fname = guess_filename_from_response(resp, url)
                fpath = os.path.join(row_dir, f"repaired_{fname}")
                with open(fpath, "wb") as f:
                    f.write(resp.content)

                vr = validate_content(fpath)
                if vr.valid:
                    downloaded.append({
                        "filename": os.path.basename(fpath),
                        "source_url": url,
                        "size_bytes": os.path.getsize(fpath),
                        "sha256": sha256_file(fpath),
                        "source": "direct_redownload",
                        "validation": vr.to_dict(),
                    })
                    return True, downloaded
                else:
                    os.remove(fpath)

            # HTML page — scrape for file links
            elif "html" in content_type:
                try:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(resp.text, "html.parser")
                    file_exts = {".pdf", ".doc", ".docx", ".xls", ".xlsx"}

                    for tag in soup.find_all("a", href=True):
                        href = tag["href"].strip()
                        full_url = urljoin(url, href)
                        ext = os.path.splitext(urlparse(full_url).path)[1].lower()

                        if ext in file_exts:
                            time.sleep(REQUEST_DELAY)
                            dl = session.get(full_url, timeout=30)
                            if dl.status_code == 200:
                                fname = guess_filename_from_response(dl, full_url)
                                fpath = os.path.join(row_dir, f"repaired_{fname}")
                                with open(fpath, "wb") as f:
                                    f.write(dl.content)

                                vr = validate_content(fpath)
                                if vr.valid:
                                    downloaded.append({
                                        "filename": os.path.basename(fpath),
                                        "source_url": full_url,
                                        "size_bytes": os.path.getsize(fpath),
                                        "sha256": sha256_file(fpath),
                                        "source": "scraped_redownload",
                                        "validation": vr.to_dict(),
                                    })
                                    return True, downloaded
                                else:
                                    os.remove(fpath)
                except ImportError:
                    pass

        except requests.exceptions.RequestException as e:
            logger.debug(f"    Request failed: {e}")
            continue

    # Try Wayback as last resort
    for url in urls[:3]:
        wb_url = f"https://web.archive.org/web/2/{url}"
        try:
            time.sleep(WAYBACK_DELAY)
            resp = session.get(wb_url, timeout=30, allow_redirects=True)
            if resp.status_code != 200:
                continue

            # Validate Wayback didn't give us junk
            if "justice.gov" in urlparse(resp.url).netloc.lower():
                continue

            content_type = resp.headers.get("Content-Type", "").lower()

            if any(ct in content_type for ct in ["pdf", "msword", "officedocument"]):
                fname = guess_filename_from_response(resp, url)
                fpath = os.path.join(row_dir, f"repaired_{fname}")
                with open(fpath, "wb") as f:
                    f.write(resp.content)

                vr = validate_content(fpath)
                if vr.valid:
                    downloaded.append({
                        "filename": os.path.basename(fpath),
                        "source_url": url,
                        "wayback_url": wb_url,
                        "size_bytes": os.path.getsize(fpath),
                        "sha256": sha256_file(fpath),
                        "source": "wayback_redownload",
                        "validation": vr.to_dict(),
                    })
                    return True, downloaded
                else:
                    os.remove(fpath)

            elif "html" in content_type:
                # Check content quality before accepting
                doj = check_for_junk_indicators(resp.text, DOJ_INDICATORS)
                wbe = check_for_junk_indicators(resp.text, WAYBACK_ERROR_INDICATORS)
                if doj or wbe:
                    continue

                visible = extract_visible_text(resp.text)
                if len(visible) >= 500:
                    fname = "repaired_wayback_page.html"
                    fpath = os.path.join(row_dir, fname)
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write(resp.text)
                    downloaded.append({
                        "filename": fname,
                        "source_url": url,
                        "wayback_url": wb_url,
                        "size_bytes": os.path.getsize(fpath),
                        "source": "wayback_redownload",
                        "needs_conversion": True,
                    })
                    return True, downloaded

        except requests.exceptions.RequestException:
            continue

    return False, []


def phase1_redownload(output_dir, dry_run=False):
    """Phase 1: Identify and re-download bad files."""
    logger = logging.getLogger("repair")

    logger.info("=" * 60)
    logger.info("PHASE 1: RE-DOWNLOAD BAD FILES")
    logger.info("=" * 60)

    logger.info("Scanning for bad files...")
    bad_rows = identify_bad_files(output_dir)

    # Focus on rows where the bad file is the ONLY file (or all files are bad)
    critical_rows = [r for r in bad_rows if not r["has_valid_file"]]
    fixable_rows = [r for r in bad_rows if r["has_valid_file"]]

    total_bad_files = sum(len(r["bad_files"]) for r in bad_rows)
    logger.info(f"  Found {total_bad_files} bad files across {len(bad_rows)} rows")
    logger.info(f"  Critical (no valid files): {len(critical_rows)} rows")
    logger.info(f"  Fixable (has valid + bad): {len(fixable_rows)} rows")

    if dry_run:
        logger.info("\n  [DRY RUN] Would attempt re-download for:")
        for r in critical_rows:
            reasons = ", ".join(bf["reason"] for bf in r["bad_files"])
            logger.info(f"    {r['row']} | {r['metadata'].get('office', '?'):6s} | {reasons}")
        return {"identified": len(bad_rows), "critical": len(critical_rows),
                "repaired": 0, "failed": 0}

    # Backup bad files before replacing
    os.makedirs(BACKUP_DIR, exist_ok=True)

    session = setup_session()
    stats = {"attempted": 0, "repaired": 0, "failed": 0, "details": []}

    # Process critical rows (no valid content at all)
    for i, row_info in enumerate(critical_rows):
        row_id = row_info["row"]
        row_dir = row_info["row_dir"]
        office = row_info["metadata"].get("office", "?")

        logger.info(f"  [{i+1}/{len(critical_rows)}] {row_id} | {office:6s} | "
                     f"Attempting re-download...")
        stats["attempted"] += 1

        # Backup the bad files
        backup_row_dir = os.path.join(BACKUP_DIR, row_id)
        os.makedirs(backup_row_dir, exist_ok=True)
        for bf in row_info["bad_files"]:
            src = bf["filepath"]
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(backup_row_dir, bf["filename"]))

        # Try re-download
        success, files = try_redownload(row_info, session, row_dir, logger)

        if success:
            logger.info(f"    ✓ Repaired! Got {len(files)} file(s)")
            stats["repaired"] += 1

            # Update metadata
            meta_path = os.path.join(row_dir, "metadata.json")
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                metadata["repair_files"] = files
                metadata["repair_date"] = datetime.now(timezone.utc).isoformat()
                metadata["repair_version"] = "2.0"
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

            stats["details"].append({"row": row_id, "result": "repaired", "files": files})
        else:
            logger.info(f"    ✗ Still failed — no valid source found")
            stats["failed"] += 1
            stats["details"].append({"row": row_id, "result": "failed"})

    # Also clean up bad files in rows that DO have valid content
    # (just remove/rename the junk files so they don't confuse James)
    cleaned = 0
    for row_info in fixable_rows:
        for bf in row_info["bad_files"]:
            src = bf["filepath"]
            if os.path.exists(src):
                # Rename with _INVALID prefix so it's obvious
                bad_dir = os.path.join(row_info["row_dir"], "_invalid")
                os.makedirs(bad_dir, exist_ok=True)
                shutil.move(src, os.path.join(bad_dir, bf["filename"]))
                cleaned += 1

    logger.info(f"\n  Phase 1 complete:")
    logger.info(f"    Re-download attempted: {stats['attempted']}")
    logger.info(f"    Repaired:              {stats['repaired']}")
    logger.info(f"    Still failed:          {stats['failed']}")
    logger.info(f"    Junk files quarantined: {cleaned}")

    stats["quarantined"] = cleaned
    return stats


# ══════════════════════════════════════════════════════════════════════════
# PHASE 2: Convert HTML files to clean PDFs
# ══════════════════════════════════════════════════════════════════════════

def identify_html_files(output_dir):
    """
    Find all HTML files that are valid content but need PDF conversion.
    Returns list of dicts: {row, row_dir, html_files, metadata}
    """
    html_rows = []

    folders = sorted(f for f in os.listdir(output_dir)
                     if os.path.isdir(os.path.join(output_dir, f)) and f.isdigit())

    for folder in folders:
        row_dir = os.path.join(output_dir, folder)
        meta_path = os.path.join(row_dir, "metadata.json")

        metadata = {}
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except Exception:
                pass

        # Find HTML files (not in _invalid subfolder)
        html_files = []
        for fname in os.listdir(row_dir):
            fpath = os.path.join(row_dir, fname)
            if not os.path.isfile(fpath):
                continue
            if fname == "metadata.json":
                continue

            # Check if it's HTML content
            ftype = detect_file_type(fpath)
            ext = os.path.splitext(fname)[1].lower()

            if ftype == "html" or ext in (".html", ".htm"):
                # Skip already-converted indicator files
                if fname.endswith(".clean.html"):
                    continue

                # Check if there's already a PDF version
                pdf_name = os.path.splitext(fname)[0] + ".pdf"
                converted_pdf = os.path.splitext(fname)[0] + "_converted.pdf"
                has_pdf = (os.path.exists(os.path.join(row_dir, pdf_name)) or
                           os.path.exists(os.path.join(row_dir, converted_pdf)))

                # Also check for any existing valid PDF in the directory
                existing_pdfs = [f for f in os.listdir(row_dir)
                                 if f.endswith(".pdf") and os.path.isfile(os.path.join(row_dir, f))]
                # Validate existing PDFs
                valid_pdfs = []
                for pf in existing_pdfs:
                    vr = validate_content(os.path.join(row_dir, pf))
                    if vr.valid:
                        valid_pdfs.append(pf)

                if valid_pdfs:
                    continue  # Already has a valid PDF, skip

                # Validate the HTML
                vr = validate_content(fpath)
                if vr.valid:
                    html_files.append({
                        "filename": fname,
                        "filepath": fpath,
                        "size": os.path.getsize(fpath),
                    })

        if html_files:
            html_rows.append({
                "row": folder,
                "row_dir": row_dir,
                "html_files": html_files,
                "metadata": metadata,
            })

    return html_rows


def convert_with_playwright(html_path, pdf_path, logger):
    """Convert HTML to clean PDF using Playwright."""
    from playwright.sync_api import sync_playwright

    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        html_content = f.read()

    # Inject clean CSS
    if "</head>" in html_content.lower():
        html_content = re.sub(r"(</head>)", CLEAN_CSS + r"\1",
                              html_content, count=1, flags=re.IGNORECASE)
    else:
        html_content = CLEAN_CSS + html_content

    temp_html = html_path + ".clean.tmp.html"
    with open(temp_html, "w", encoding="utf-8") as f:
        f.write(html_content)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(f"file:///{os.path.abspath(temp_html).replace(os.sep, '/')}")
            page.wait_for_load_state("networkidle")

            visible_chars = page.evaluate(CLEANUP_JS)

            if visible_chars < 200:
                browser.close()
                return False, f"insufficient_content ({visible_chars} chars)"

            page.pdf(
                path=pdf_path,
                format="Letter",
                margin={"top": "0.75in", "bottom": "0.75in",
                        "left": "0.75in", "right": "0.75in"},
                print_background=False,
            )
            browser.close()

        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:
            return True, "ok"
        return False, "pdf_too_small"

    except Exception as e:
        return False, str(e)
    finally:
        if os.path.exists(temp_html):
            os.remove(temp_html)


def convert_with_reportlab(html_path, pdf_path, metadata, logger):
    """Fallback PDF conversion using reportlab."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.units import inch
    except ImportError:
        return False, "reportlab not installed"

    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        html_content = f.read()

    text = extract_visible_text(html_content)
    if len(text) < 200:
        return False, "insufficient_content"

    try:
        doc = SimpleDocTemplate(pdf_path, pagesize=letter,
                                leftMargin=0.75*inch, rightMargin=0.75*inch,
                                topMargin=0.75*inch, bottomMargin=0.75*inch)
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'],
                                     fontSize=14, spaceAfter=12)
        body_style = ParagraphStyle('DocBody', parent=styles['Normal'],
                                    fontSize=11, leading=14, spaceAfter=8)
        story = []

        doc_num = metadata.get("doc_number", "")
        title = metadata.get("title", "")
        if doc_num or title:
            header = f"{doc_num} — {title}" if doc_num and title else (doc_num or title)
            header = header.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            story.append(Paragraph(header, title_style))
            story.append(Spacer(1, 12))

        paragraphs = [p.strip() for p in text.split('\n') if p.strip()]
        for para in paragraphs:
            para = para.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            try:
                story.append(Paragraph(para[:2000], body_style))
            except Exception:
                pass

        doc.build(story)
        return True, "ok"
    except Exception as e:
        return False, str(e)


def phase2_convert(output_dir, dry_run=False, use_playwright=True):
    """Phase 2: Convert valid HTML files to clean PDFs."""
    logger = logging.getLogger("repair")

    logger.info("")
    logger.info("=" * 60)
    logger.info("PHASE 2: CONVERT HTML FILES TO CLEAN PDFs")
    logger.info("=" * 60)

    logger.info("Identifying HTML files needing conversion...")
    html_rows = identify_html_files(output_dir)

    total_files = sum(len(r["html_files"]) for r in html_rows)
    logger.info(f"  Found {total_files} HTML files across {len(html_rows)} rows")

    if dry_run:
        logger.info(f"\n  [DRY RUN] Would convert {total_files} files")
        # Show sample
        for r in html_rows[:10]:
            files_str = ", ".join(hf["filename"] for hf in r["html_files"])
            logger.info(f"    {r['row']} | {r['metadata'].get('office', '?'):6s} | {files_str}")
        if len(html_rows) > 10:
            logger.info(f"    ... and {len(html_rows) - 10} more rows")
        return {"identified": total_files, "converted": 0, "failed": 0}

    # Check Playwright availability
    if use_playwright:
        try:
            from playwright.sync_api import sync_playwright
            # Quick test
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()
            logger.info("  Playwright: ready")
        except Exception as e:
            logger.warning(f"  Playwright not available ({e}) — using reportlab fallback")
            use_playwright = False

    stats = {"converted": 0, "failed": 0, "skipped": 0, "details": []}
    batch_start = datetime.now()

    for i, row_info in enumerate(html_rows):
        row_id = row_info["row"]
        row_dir = row_info["row_dir"]
        metadata = row_info["metadata"]

        for hf in row_info["html_files"]:
            html_path = hf["filepath"]

            # Generate PDF name
            doc_num = metadata.get("doc_number", "").strip()
            if doc_num:
                safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
                pdf_name = f"{safe_name}.pdf"
            else:
                base = os.path.splitext(hf["filename"])[0]
                pdf_name = f"{base}.pdf"

            pdf_path = os.path.join(row_dir, pdf_name)

            # Don't overwrite existing
            if os.path.exists(pdf_path):
                base, ext = os.path.splitext(pdf_path)
                pdf_path = f"{base}_clean{ext}"
                pdf_name = os.path.basename(pdf_path)

            # Convert
            if use_playwright:
                success, reason = convert_with_playwright(html_path, pdf_path, logger)
            else:
                success, reason = convert_with_reportlab(html_path, pdf_path, metadata, logger)

            if success:
                stats["converted"] += 1
                stats["details"].append({
                    "row": row_id, "html": hf["filename"],
                    "pdf": pdf_name, "result": "ok"
                })

                # Update metadata
                meta_path = os.path.join(row_dir, "metadata.json")
                if os.path.exists(meta_path):
                    try:
                        with open(meta_path, "r", encoding="utf-8") as f:
                            m = json.load(f)
                        if "converted_pdfs" not in m:
                            m["converted_pdfs"] = []
                        m["converted_pdfs"].append({
                            "pdf": pdf_name,
                            "from_html": hf["filename"],
                            "size_bytes": os.path.getsize(pdf_path),
                            "sha256": sha256_file(pdf_path),
                            "method": "playwright" if use_playwright else "reportlab",
                            "converted_at": datetime.now(timezone.utc).isoformat(),
                        })
                        with open(meta_path, "w", encoding="utf-8") as f:
                            json.dump(m, f, indent=2, ensure_ascii=False, default=str)
                    except Exception:
                        pass
            else:
                stats["failed"] += 1
                stats["details"].append({
                    "row": row_id, "html": hf["filename"],
                    "result": "failed", "reason": reason
                })
                logger.debug(f"  {row_id}: Failed — {reason}")

        # Progress every 100 rows
        if (i + 1) % 100 == 0:
            elapsed = (datetime.now() - batch_start).total_seconds()
            rate = (i + 1) / elapsed * 60 if elapsed > 0 else 0
            logger.info(f"  Progress: {i+1}/{len(html_rows)} rows | "
                        f"{stats['converted']} converted | {rate:.0f} rows/min")

    logger.info(f"\n  Phase 2 complete:")
    logger.info(f"    Converted:  {stats['converted']}")
    logger.info(f"    Failed:     {stats['failed']}")

    return stats


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="ACF Guidance — Targeted Repair")
    parser.add_argument("--phase", type=int, choices=[1, 2], default=None,
                        help="Run only phase 1 or 2 (default: both)")
    parser.add_argument("--dir", type=str, default=None, help="Override output directory")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--no-playwright", action="store_true",
                        help="Use reportlab instead of Playwright for conversion")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format=LOG_FORMAT, datefmt=LOG_DATE)
    logger = logging.getLogger("repair")

    output_dir = args.dir or OUTPUT_DIR
    if not os.path.isdir(output_dir):
        logger.error(f"Output directory not found: {output_dir}")
        sys.exit(1)

    use_playwright = not args.no_playwright

    start_time = datetime.now()
    all_stats = {}

    logger.info("=" * 60)
    logger.info("ACF GUIDANCE — TARGETED REPAIR")
    logger.info("=" * 60)
    logger.info(f"  Directory:   {output_dir}")
    logger.info(f"  Dry run:     {args.dry_run}")
    logger.info(f"  Playwright:  {use_playwright}")

    # Phase 1
    if args.phase is None or args.phase == 1:
        all_stats["phase1"] = phase1_redownload(output_dir, args.dry_run)

    # Phase 2
    if args.phase is None or args.phase == 2:
        all_stats["phase2"] = phase2_convert(output_dir, args.dry_run, use_playwright)

    elapsed = datetime.now() - start_time

    # ── Final summary ──
    print("\n" + "=" * 60)
    print("REPAIR COMPLETE" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 60)

    if "phase1" in all_stats:
        p1 = all_stats["phase1"]
        print(f"  Phase 1 (Re-download):")
        if args.dry_run:
            print(f"    Would attempt: {p1.get('critical', p1.get('identified', 0))} rows")
        else:
            print(f"    Repaired:      {p1.get('repaired', 0)}")
            print(f"    Still failed:  {p1.get('failed', 0)}")
            print(f"    Quarantined:   {p1.get('quarantined', 0)}")

    if "phase2" in all_stats:
        p2 = all_stats["phase2"]
        print(f"  Phase 2 (HTML → PDF):")
        if args.dry_run:
            print(f"    Would convert: {p2.get('identified', 0)} files")
        else:
            print(f"    Converted:     {p2.get('converted', 0)}")
            print(f"    Failed:        {p2.get('failed', 0)}")

    print(f"  Elapsed:         {elapsed}")

    # Save report
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(REPORTS_DIR,
                               f"repair_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(report_path, "w") as f:
        json.dump({"stats": all_stats, "elapsed_seconds": elapsed.total_seconds(),
                    "dry_run": args.dry_run}, f, indent=2, default=str)
    print(f"  Report saved:    {report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
