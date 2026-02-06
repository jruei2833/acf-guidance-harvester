#!/usr/bin/env python3
"""Quick diagnostic of remaining problem rows."""

import json
import os
from collections import Counter

output_dir = os.path.join(os.path.dirname(__file__), "..", "output")

rows = []
for d in sorted(os.listdir(output_dir)):
    meta_path = os.path.join(output_dir, d, "metadata.json")
    if os.path.isdir(os.path.join(output_dir, d)) and os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            rows.append(json.load(f))

# Overall status
status_counts = Counter(r["status"] for r in rows)
print("=" * 70)
print("OVERALL STATUS")
print("=" * 70)
for status, count in status_counts.most_common():
    print(f"  {status:20s}: {count}")
print(f"  {'TOTAL':20s}: {len(rows)}")

# Failed rows
failed = [r for r in rows if r["status"] == "failed"]
print(f"\n{'=' * 70}")
print(f"FAILED ROWS ({len(failed)})")
print("=" * 70)
err_types = Counter()
for r in failed:
    err = r["errors"][0].get("error", "?") if r["errors"] else "?"
    err_types[err] += 1
print("Error types:")
for err, count in err_types.most_common():
    print(f"  {err}: {count}")

print("\nFirst 30 failed rows:")
for r in failed[:30]:
    err = r["errors"][0].get("error", "?") if r["errors"] else "?"
    url = r["urls"][0]["url"][:70] if r.get("urls") else "NO URL"
    print(f"  {r['folder']} | {r.get('office','?'):6s} | {err:20s} | {url}")

# No documents rows (that weren't converted)
no_docs = [r for r in rows if r["status"] == "no_documents"]
print(f"\n{'=' * 70}")
print(f"STILL NO DOCUMENTS ({len(no_docs)})")
print("=" * 70)
by_office = Counter(r.get("office", "?") for r in no_docs)
print("By office:")
for office, count in by_office.most_common():
    print(f"  {office}: {count}")

print("\nSample rows (first 20):")
for r in no_docs[:20]:
    url = r["urls"][0]["url"][:70] if r.get("urls") else "NO URL"
    has_html = os.path.exists(os.path.join(output_dir, r["folder"], "page_content.html"))
    print(f"  {r['folder']} | {r.get('office','?'):6s} | HTML:{'Y' if has_html else 'N'} | {url}")

# No URL rows
no_urls = [r for r in rows if r["status"] == "no_urls"]
print(f"\n{'=' * 70}")
print(f"NO URLs ({len(no_urls)})")
print("=" * 70)
for r in no_urls:
    print(f"  {r['folder']} | {r.get('office','?'):6s} | {r.get('doc_number',''):30s} | {r.get('title','')[:50]}")
