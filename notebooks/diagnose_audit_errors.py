"""Fabric notebook helper -- inspect the CREATE VIEW signatures of every
view that timing_audit classified as 'error'.

Workflow:
    1. Run timing_audit.batch.audit_timing on a corpus.
    2. Some views land in status='error' (sqlglot parse failure during
       resolve).
    3. Run THIS cell to print the CREATE/ALTER VIEW signature line of
       each error view + the next 4 lines (the column-list opener / AS
       region). The output is structural only -- no literal values, no
       row data, just SQL syntax shape.
    4. Look for patterns: schema/name with spaces in brackets, WITH
       SCHEMABINDING between cols and AS, etc. Often 10+ errors share
       ONE shape variant; ONE new parsing rule fixes them all.

Each `# %%` block is one notebook cell.
"""


# %% [Cell A: print the CREATE VIEW signature of every error view]

import csv
import os
import re

# EDIT THESE if your paths differ
AUDIT_CSV = '/lakehouse/default/Files/outputs/timing_audit.csv'
VIEWS_DIR = '/lakehouse/default/Files/views'


def _read_text(path: str) -> str:
    """Handle the BOMs SSMS exports use."""
    raw = open(path, 'rb').read()
    if raw.startswith(b'\xff\xfe'):
        return raw.decode('utf-16-le')[1:]
    if raw.startswith(b'\xfe\xff'):
        return raw.decode('utf-16-be')[1:]
    if raw.startswith(b'\xef\xbb\xbf'):
        return raw.decode('utf-8')[1:]
    try:
        return raw.decode('utf-8')
    except UnicodeDecodeError:
        return raw.decode('utf-16-le', errors='replace')


# Pull error-status rows from the audit CSV
error_views: list[str] = []
with open(AUDIT_CSV, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        if row['status'] == 'error':
            error_views.append(row['view_name'])

print(f"{len(error_views)} views in 'error' status\n")
print("=" * 70)

# Build a filename map (the audit's view_name is the file stem; the actual
# file might have any case / extension variant).
all_files = {f.lower(): f for f in os.listdir(VIEWS_DIR) if f.lower().endswith('.sql')}

CREATE_VIEW_RE = re.compile(
    r'^\s*(CREATE|ALTER)\s+(OR\s+ALTER\s+)?VIEW\b',
    re.IGNORECASE,
)


def _find_view_file(view_name: str) -> str | None:
    """Find the .sql file matching the audit row's view_name."""
    candidates = [stem + '.sql' for stem in [view_name, view_name.lower(), view_name.upper()]]
    for c in candidates:
        if c.lower() in all_files:
            return os.path.join(VIEWS_DIR, all_files[c.lower()])
    # Fallback: any file whose stem starts with the view_name
    for fname in all_files.values():
        if fname.lower().startswith(view_name.lower() + '.'):
            return os.path.join(VIEWS_DIR, fname)
    return None


for vn in error_views:
    path = _find_view_file(vn)
    if not path:
        print(f"\n[{vn}]  FILE NOT FOUND under {VIEWS_DIR}")
        continue
    text = _read_text(path)
    lines = text.splitlines()
    sig_idx = None
    for i, line in enumerate(lines):
        if CREATE_VIEW_RE.match(line):
            sig_idx = i
            break
    print(f"\n[{vn}]")
    if sig_idx is None:
        print(f"  (no CREATE/ALTER VIEW line found in first {len(lines)} lines)")
        continue
    # Show 5 lines starting at the CREATE line (the signature region)
    snippet = lines[sig_idx:sig_idx + 5]
    for j, sline in enumerate(snippet):
        print(f"  L{sig_idx + 1 + j}: {sline}")


# %% [Cell B (optional): group error views by their first-line signature shape]
#
# Groups identical signature lines so you can see at a glance "10 views
# all use shape A, 4 use shape B, 1 oddball" -- exactly what tells you
# whether a single new rule will fix many of them.

import csv
import os
import re
from collections import Counter

AUDIT_CSV = '/lakehouse/default/Files/outputs/timing_audit.csv'
VIEWS_DIR = '/lakehouse/default/Files/views'

CREATE_VIEW_RE = re.compile(r'^\s*(CREATE|ALTER)\s+(OR\s+ALTER\s+)?VIEW\b', re.IGNORECASE)
all_files = {f.lower(): f for f in os.listdir(VIEWS_DIR) if f.lower().endswith('.sql')}

shape_counter: Counter[str] = Counter()
error_views = []
with open(AUDIT_CSV, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        if row['status'] == 'error':
            error_views.append(row['view_name'])

for vn in error_views:
    path = None
    for c in [vn + '.sql', vn.lower() + '.sql', vn.upper() + '.sql']:
        if c.lower() in all_files:
            path = os.path.join(VIEWS_DIR, all_files[c.lower()])
            break
    if not path:
        continue
    raw = open(path, 'rb').read().lstrip(b'\xff\xfe').lstrip(b'\xef\xbb\xbf')
    text = raw.decode('utf-8', errors='replace')
    for line in text.splitlines():
        if CREATE_VIEW_RE.match(line):
            # Normalize: collapse whitespace, drop the actual schema/name
            shape = re.sub(r'\s+', ' ', line.strip())
            # Mask schema/name identifiers so similar shapes group together.
            # `[Foo Bar].[Baz]` -> `[X].[X]` ; bare ident -> `IDENT`
            shape = re.sub(r'\[[^\]]+\]', '[X]', shape)
            shape = re.sub(r'\b\w+\.\w+\b', 'IDENT.IDENT', shape)
            shape_counter[shape] += 1
            break

print(f"{len(error_views)} error views; CREATE/ALTER VIEW shape distribution:\n")
for shape, n in shape_counter.most_common():
    print(f"  ({n:>3}x) {shape}")
