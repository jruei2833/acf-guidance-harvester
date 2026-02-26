#!/usr/bin/env python3
"""
ACF Guidance Harvester v2 — Content Validation
================================================
Validates downloaded content to catch the failure modes from v1:
  1. Wayback Machine junk captures (archived error pages)
  2. DOJ "access denied" redirects
  3. Empty shell pages (nav/header only, no document content)
  4. HTML files masquerading as PDFs
  5. Generic error/redirect pages

Every downloaded file passes through validate_content() before being
accepted into the collection.
"""

import os
import re
import struct
import logging

logger = logging.getLogger("acf_v2.validate")

# ── Junk content indicators ──────────────────────────────────────────────

DOJ_INDICATORS = [
    "access denied",
    "you don't have permission to access",
    "403 forbidden",
    "department of justice",
    "justice.gov",
    "you are not authorized",
    "access to this page is restricted",
]

WAYBACK_ERROR_INDICATORS = [
    "the wayback machine has not archived",
    "this url has been excluded from the wayback machine",
    "hrly captures",              # Wayback calendar/listing page
    "web.archive.org/web/",       # URL still in Wayback wrapper
    "sorry. this url has been excluded",
    "this snapshot cannot be displayed",
    "wayback machine doesn't have that page archived",
    "got an http 302 response",   # Wayback redirect notice
]

GENERIC_ERROR_INDICATORS = [
    "page not found",
    "404 not found",
    "404 error",
    "the page you are looking for",
    "this page has been removed",
    "this page is no longer available",
    "the requested url was not found",
    "we're sorry, but the page you requested",
    "content no longer available",
    "has been archived or removed",
    "server error",
    "500 internal server error",
    "502 bad gateway",
    "503 service unavailable",
]

SHELL_ONLY_PATTERNS = [
    r"skip\s+to\s+(main\s+)?content",
    r"skip\s+navigation",
]

# ── File signature detection ─────────────────────────────────────────────

FILE_SIGNATURES = {
    b"%PDF":           "pdf",
    b"PK\x03\x04":    "zip_based",   # docx, xlsx, pptx, etc.
    b"\xd0\xcf\x11":  "ole",         # doc, xls, ppt (old Office)
    b"\x89PNG":        "png",
    b"\xff\xd8\xff":   "jpg",
    b"GIF8":           "gif",
    b"{\\":            "rtf",
}


def detect_file_type(filepath):
    """Detect actual file type by reading magic bytes, ignoring extension."""
    try:
        with open(filepath, "rb") as f:
            header = f.read(16)
    except (OSError, IOError):
        return "unknown"

    for sig, ftype in FILE_SIGNATURES.items():
        if header.startswith(sig):
            return ftype

    # Check if it looks like HTML/XML
    try:
        text_start = header.decode("utf-8", errors="ignore").strip().lower()
    except Exception:
        return "binary"

    if text_start.startswith("<!doctype") or text_start.startswith("<html") or text_start.startswith("<?xml"):
        return "html"

    # Could be plain text
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(2000)
        if "<html" in sample.lower() or "<body" in sample.lower():
            return "html"
        return "text"
    except Exception:
        return "unknown"


def is_html_masquerading_as_pdf(filepath):
    """Check if a .pdf file is actually HTML content."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext != ".pdf":
        return False
    actual_type = detect_file_type(filepath)
    return actual_type == "html"


# ── Content quality checks ───────────────────────────────────────────────

def extract_visible_text(html_content):
    """
    Extract meaningful visible text from HTML, stripping nav/header/footer/scripts.
    Uses regex-based approach to avoid requiring BeautifulSoup as a hard dependency
    at the validation layer.
    """
    text = html_content

    # Remove script, style, noscript, nav, header, footer blocks
    for tag in ["script", "style", "noscript", "nav", "header", "footer"]:
        text = re.sub(
            rf"<{tag}[^>]*>.*?</{tag}>",
            "", text, flags=re.DOTALL | re.IGNORECASE
        )

    # Remove all remaining HTML tags
    text = re.sub(r"<[^>]+>", " ", text)

    # Decode common entities
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = text.replace("&lt;", "<").replace("&gt;", ">")
    text = re.sub(r"&#?\w+;", " ", text)

    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text


def check_for_junk_indicators(text, indicators):
    """Check if text contains any of the given junk indicators."""
    text_lower = text.lower()
    matched = []
    for indicator in indicators:
        if indicator.lower() in text_lower:
            matched.append(indicator)
    return matched


def is_empty_shell(html_content, min_chars=500):
    """
    Check if an HTML page is just a navigation shell with no real content.
    Returns (is_shell: bool, char_count: int, reason: str)
    """
    visible_text = extract_visible_text(html_content)

    # Check for shell-only patterns
    for pattern in SHELL_ONLY_PATTERNS:
        if re.search(pattern, html_content, re.IGNORECASE):
            # Page has "skip to content" — check if there's actual content after
            if len(visible_text) < min_chars:
                return True, len(visible_text), "shell_page_no_content"

    if len(visible_text) < min_chars:
        return True, len(visible_text), "insufficient_content"

    return False, len(visible_text), "ok"


# ── Main validation function ─────────────────────────────────────────────

class ValidationResult:
    """Result of content validation."""
    def __init__(self, valid, filepath, reason="ok", details=None):
        self.valid = valid
        self.filepath = filepath
        self.reason = reason
        self.details = details or {}

    def __repr__(self):
        status = "PASS" if self.valid else "FAIL"
        return f"ValidationResult({status}, {self.reason})"

    def to_dict(self):
        return {
            "valid": self.valid,
            "filepath": self.filepath,
            "reason": self.reason,
            "details": self.details,
        }


def validate_content(filepath, min_text_chars=500):
    """
    Comprehensive content validation for a downloaded file.

    Returns ValidationResult with:
      - valid: bool — whether the file contains real document content
      - reason: str — machine-readable reason code
      - details: dict — additional diagnostic info

    Reason codes:
      ok                    — File passed validation
      file_not_found        — File doesn't exist
      file_empty            — File is 0 bytes
      file_too_small        — File under 200 bytes
      html_masquerade       — .pdf file is actually HTML
      doj_access_denied     — DOJ block page
      wayback_error         — Wayback Machine error/calendar page
      generic_error_page    — 404, 500, or similar error page
      empty_shell           — Nav/header only, no document content
      insufficient_content  — HTML with too little real text
    """

    # ── Basic file checks ──
    if not os.path.exists(filepath):
        return ValidationResult(False, filepath, "file_not_found")

    file_size = os.path.getsize(filepath)
    if file_size == 0:
        return ValidationResult(False, filepath, "file_empty")

    if file_size < 200:
        return ValidationResult(False, filepath, "file_too_small",
                                {"size_bytes": file_size})

    # ── Detect actual file type ──
    actual_type = detect_file_type(filepath)

    # If it's a real binary document (PDF, Office, etc.), it's valid
    if actual_type in ("pdf", "zip_based", "ole", "rtf"):
        return ValidationResult(True, filepath, "ok",
                                {"actual_type": actual_type, "size_bytes": file_size})

    # Images are unusual for guidance docs but technically valid
    if actual_type in ("png", "jpg", "gif"):
        return ValidationResult(True, filepath, "ok",
                                {"actual_type": actual_type, "size_bytes": file_size})

    # ── HTML/text content needs deeper inspection ──
    if actual_type not in ("html", "text"):
        # Unknown binary — accept if reasonably sized
        if file_size > 5000:
            return ValidationResult(True, filepath, "ok",
                                    {"actual_type": "unknown_binary", "size_bytes": file_size})
        return ValidationResult(False, filepath, "file_too_small",
                                {"actual_type": actual_type, "size_bytes": file_size})

    # Read the HTML/text content
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception as e:
        return ValidationResult(False, filepath, "read_error",
                                {"error": str(e)})

    # ── Check for HTML masquerading as PDF ──
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".pdf" and actual_type == "html":
        return ValidationResult(False, filepath, "html_masquerade",
                                {"declared_type": "pdf", "actual_type": "html"})

    # ── Check for DOJ access denied ──
    doj_matches = check_for_junk_indicators(content, DOJ_INDICATORS)
    if doj_matches:
        return ValidationResult(False, filepath, "doj_access_denied",
                                {"matched_indicators": doj_matches})

    # ── Check for Wayback errors ──
    wb_matches = check_for_junk_indicators(content, WAYBACK_ERROR_INDICATORS)
    if wb_matches:
        return ValidationResult(False, filepath, "wayback_error",
                                {"matched_indicators": wb_matches})

    # ── Check for generic error pages ──
    err_matches = check_for_junk_indicators(content, GENERIC_ERROR_INDICATORS)
    if err_matches:
        # Only flag if the page is short (long pages with a 404 mention might be legit)
        visible_text = extract_visible_text(content)
        if len(visible_text) < 2000:
            return ValidationResult(False, filepath, "generic_error_page",
                                    {"matched_indicators": err_matches,
                                     "visible_chars": len(visible_text)})

    # ── Check for empty shell ──
    is_shell, char_count, shell_reason = is_empty_shell(content, min_text_chars)
    if is_shell:
        return ValidationResult(False, filepath, shell_reason,
                                {"visible_chars": char_count})

    # ── Passed all checks ──
    return ValidationResult(True, filepath, "ok",
                            {"actual_type": actual_type,
                             "size_bytes": file_size,
                             "visible_chars": extract_visible_text(content).__len__()})


def validate_directory(dir_path, min_text_chars=500):
    """
    Validate all files in a harvest row directory.
    Returns list of ValidationResults.
    """
    results = []
    if not os.path.isdir(dir_path):
        return results

    for fname in os.listdir(dir_path):
        if fname == "metadata.json":
            continue
        fpath = os.path.join(dir_path, fname)
        if os.path.isfile(fpath):
            results.append(validate_content(fpath, min_text_chars))

    return results


# ── CLI for standalone testing ───────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Usage: python validate.py <file_or_directory>")
        print("  Validates a single file or all files in a harvest row directory.")
        sys.exit(1)

    target = sys.argv[1]

    if os.path.isdir(target):
        results = validate_directory(target)
        for r in results:
            status = "✓ PASS" if r.valid else "✗ FAIL"
            fname = os.path.basename(r.filepath)
            print(f"  {status}  {fname:40s}  {r.reason}")
            if r.details:
                for k, v in r.details.items():
                    print(f"          {k}: {v}")
    elif os.path.isfile(target):
        r = validate_content(target)
        status = "✓ PASS" if r.valid else "✗ FAIL"
        print(f"{status}  {r.reason}")
        if r.details:
            print(json.dumps(r.details, indent=2))
    else:
        print(f"Not found: {target}")
        sys.exit(1)
