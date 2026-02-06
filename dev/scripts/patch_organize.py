"""Patch organize.py to use row numbers as folder names."""
import re

f = 'scripts/organize.py'
text = open(f, 'r').read()

# Replace the dest_dir logic that uses doc_id with one that uses folder (row number)
old = """        dest_dir = os.path.join(ORGANIZED_DIR, office, doc_type_folder, doc_id)

        if os.path.exists(dest_dir):
            doc_id = f\"{doc_id}_R{folder}\"
            dest_dir = os.path.join(ORGANIZED_DIR, office, doc_type_folder, doc_id)"""

new = """        # Use row number as folder name for direct Excel correlation
        dest_dir = os.path.join(ORGANIZED_DIR, office, doc_type_folder, folder)"""

if old in text:
    text = text.replace(old, new)
    open(f, 'w').write(text)
    print("Patched successfully (exact match)")
else:
    # Try with \r\n line endings
    old_crlf = old.replace('\n', '\r\n')
    if old_crlf in text:
        text = text.replace(old_crlf, new.replace('\n', '\r\n'))
        open(f, 'w').write(text)
        print("Patched successfully (CRLF match)")
    else:
        # Brute force: find and replace line by line
        lines = text.splitlines(True)
        out = []
        i = 0
        patched = False
        while i < len(lines):
            stripped = lines[i].strip()
            if stripped == 'dest_dir = os.path.join(ORGANIZED_DIR, office, doc_type_folder, doc_id)' and not patched:
                # Get the indentation
                indent = lines[i][:len(lines[i]) - len(lines[i].lstrip())]
                out.append(indent + '# Use row number as folder name for direct Excel correlation\n')
                out.append(indent + 'dest_dir = os.path.join(ORGANIZED_DIR, office, doc_type_folder, folder)\n')
                patched = True
                i += 1
                # Skip the if os.path.exists block
                while i < len(lines):
                    s = lines[i].strip()
                    if s.startswith('if os.path.exists(dest_dir)'):
                        i += 1
                        continue
                    elif 'doc_id = f"{doc_id}_R{folder}"' in s or "doc_id = f'{doc_id}_R{folder}'" in s:
                        i += 1
                        continue
                    elif s == 'dest_dir = os.path.join(ORGANIZED_DIR, office, doc_type_folder, doc_id)':
                        i += 1
                        continue
                    elif s == '':
                        i += 1
                        continue
                    else:
                        break
                continue
            out.append(lines[i])
            i += 1
        
        if patched:
            open(f, 'w').writelines(out)
            print("Patched successfully (line-by-line)")
        else:
            print("ERROR: Could not find the target code to patch!")
            print("Looking for: dest_dir = os.path.join(ORGANIZED_DIR, office, doc_type_folder, doc_id)")

# Verify
text = open(f).read()
if 'os.path.join(ORGANIZED_DIR, office, doc_type_folder, folder)' in text:
    print("VERIFIED: folder naming uses row number")
else:
    print("WARNING: patch may not have applied correctly")
