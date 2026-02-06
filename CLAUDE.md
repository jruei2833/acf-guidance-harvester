# CLAUDE.md — AI Assistant Context

## Project Overview
This is a federal compliance project to systematically collect all ACF (Administration for Children and Families) sub-regulatory guidance documents from a master Excel inventory containing 3,383 entries.

## Key Technical Details

### Excel Structure
- **File**: `input/Sub-Regulatory_Inventory_for_Programs_12112025_xlsx.xlsx`
- **Sheet**: "ALL" — 3,383 data rows + 1 header row
- **Critical**: URLs are stored as **embedded hyperlinks**, not visible cell text
  - Title column: 1,871 hyperlinks
  - Document Number column: 1,019 hyperlinks
  - URL column: 144 hyperlinks + 462 plain text URLs
  - 32 rows have no links at all (require web search fallback)
- Use `cell.hyperlink.target` (not `cell.value`) to extract URLs

### Scripts
- `scripts/acf_harvest.py` — Main harvester. Supports `--start-row`, `--end-row`, `--resume`, `--dry-run`
- `scripts/acf_audit.py` — Post-harvest audit. Supports `--verify-checksums`, `--export-retry-list`

### Output Structure
```
output/
  0001/           # Row 1 (Excel row 2)
    metadata.json
    document.pdf
    attachment.pdf
  0002/
    ...
```

### Running the Harvester
```bash
pip install -r requirements.txt
python scripts/acf_harvest.py                              # All rows
python scripts/acf_harvest.py --start-row 1 --end-row 500  # Batch
python scripts/acf_harvest.py --resume                      # Skip completed
python scripts/acf_harvest.py --dry-run                     # URLs only
```

## Important Notes
- Government servers can be slow — 1 second delay between requests is built in
- Wayback Machine fallback is automatic for dead links
- Each row may produce MULTIPLE files (main doc + attachments/appendices/FAQs)
- The `output/` directory is gitignored — only scripts, reports, and input are tracked
