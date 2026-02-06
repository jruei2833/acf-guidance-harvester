"""Patch organize.py to strip duplicate doc_id from attachment labels."""

f = 'scripts/organize.py'
text = open(f, 'r').read()

old = """            if link_text:
                att_label = sanitize_filename(os.path.splitext(link_text)[0])
            else:
                att_label = f"Attachment_{att_idx}\""""

new = """            if link_text:
                att_label = sanitize_filename(os.path.splitext(link_text)[0])
                # Strip doc_id prefix if link_text already contains it
                doc_id_clean = sanitize_filename(doc_id)
                if att_label.startswith(doc_id_clean + "_"):
                    att_label = att_label[len(doc_id_clean) + 1:]
                elif att_label.startswith(doc_id_clean):
                    att_label = att_label[len(doc_id_clean):].lstrip("_")
                if not att_label:
                    att_label = f"Attachment_{att_idx}"
            else:
                att_label = f"Attachment_{att_idx}\""""

# Try exact match
if old in text:
    text = text.replace(old, new)
    open(f, 'w').write(text)
    print("Patched successfully (exact match)")
else:
    # Try CRLF
    old_crlf = old.replace('\n', '\r\n')
    new_crlf = new.replace('\n', '\r\n')
    if old_crlf in text:
        text = text.replace(old_crlf, new_crlf)
        open(f, 'w').write(text)
        print("Patched successfully (CRLF match)")
    else:
        print("ERROR: Could not find target code")
        # Show what's actually there
        import re
        m = re.search(r'if link_text:.*?att_label.*?Attachment_', text, re.DOTALL)
        if m:
            print(f"Found similar at pos {m.start()}:")
            print(repr(text[m.start():m.start()+200]))

# Verify
text = open(f).read()
if 'doc_id_clean' in text:
    print("VERIFIED: duplicate doc_id stripping is in place")
else:
    print("WARNING: patch may not have applied")
