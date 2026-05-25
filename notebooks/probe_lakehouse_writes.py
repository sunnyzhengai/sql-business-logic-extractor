"""Probe which write APIs actually persist on the Fabric lakehouse mount.

Yang's MyChart re-extract trapped behind this twice already:
  - First time: existing file on lakehouse silently doesn't overwrite.
  - Second time: NEW file with a fresh name also doesn't appear.

Before guessing more fixes, this cell tests each write path against the
actual lakehouse mount and reports which one(s) work in this specific
Fabric runtime. Outputs a verdict line per method.

Usage: paste into a notebook cell, edit the OUTPUTS_DIR path if needed,
run. Paste the printed report back.
"""


# %% [Cell: probe lakehouse write paths]

import os
import sys


# ---- EDIT if your outputs folder is somewhere else ----------------------
OUTPUTS_DIR = "/lakehouse/default/Files/outputs"
# -------------------------------------------------------------------------


# Make sure the parent dir exists; print what's there so we can see if
# previous attempts created anything weird.
os.makedirs(OUTPUTS_DIR, exist_ok=True)
print(f"=== {OUTPUTS_DIR} contents BEFORE probe ===")
for f in sorted(os.listdir(OUTPUTS_DIR)):
    full = os.path.join(OUTPUTS_DIR, f)
    try:
        size = os.path.getsize(full)
        mtime = os.path.getmtime(full)
    except Exception:
        size, mtime = "?", "?"
    print(f"  {f}  ({size} bytes, mtime={mtime})")
print()


def _check(name: str, path: str, expected_content: str) -> None:
    """After a write attempt, check whether the file landed."""
    exists = os.path.isfile(path)
    if exists:
        try:
            actual = open(path).read()
            match = actual == expected_content
            print(f"  {name}: file exists ({len(actual)} bytes), "
                  f"content match: {match}")
        except Exception as e:
            print(f"  {name}: file exists but read failed: {e}")
    else:
        print(f"  {name}: FILE DOES NOT EXIST AT {path}")


print("=== Probe 1: plain Python open(w) to a NEW file ===")
path_python = os.path.join(OUTPUTS_DIR, "_probe_python.txt")
content_python = "hello from python\n"
try:
    # Make sure we're not just reading a leftover from a previous run.
    if os.path.exists(path_python):
        os.remove(path_python)
    with open(path_python, "w") as fh:
        fh.write(content_python)
    print(f"  open(w) returned without error.")
except Exception as e:
    print(f"  open(w) raised: {type(e).__name__}: {e}")
_check("plain Python", path_python, content_python)
print()


print("=== Probe 2: notebookutils.fs.put ===")
path_nb = os.path.join(OUTPUTS_DIR, "_probe_notebookutils.txt")
content_nb = "hello from notebookutils.fs.put\n"
try:
    import notebookutils
    notebookutils.fs.put(path_nb, content_nb, overwrite=True)
    print(f"  notebookutils.fs.put returned without error.")
except ImportError:
    print(f"  notebookutils not importable -- not on Fabric runtime?")
except Exception as e:
    print(f"  notebookutils.fs.put raised: {type(e).__name__}: {e}")
_check("notebookutils.fs.put", path_nb, content_nb)
print()


print("=== Probe 3: write to /tmp then notebookutils.fs.cp ===")
tmp_src = "/tmp/_probe_staging_src.txt"
path_cp = os.path.join(OUTPUTS_DIR, "_probe_staged.txt")
content_cp = "hello from /tmp via fs.cp\n"
try:
    # Write the source file locally (this MUST succeed; /tmp is local).
    with open(tmp_src, "w") as fh:
        fh.write(content_cp)
    print(f"  /tmp src wrote {os.path.getsize(tmp_src)} bytes.")
    import notebookutils
    notebookutils.fs.cp(f"file://{tmp_src}", path_cp, recurse=False)
    print(f"  notebookutils.fs.cp returned without error.")
except ImportError:
    print(f"  notebookutils not importable -- not on Fabric runtime?")
except Exception as e:
    print(f"  fs.cp raised: {type(e).__name__}: {e}")
_check("fs.cp from /tmp", path_cp, content_cp)
print()


print("=== After-probe directory listing ===")
for f in sorted(os.listdir(OUTPUTS_DIR)):
    if f.startswith("_probe"):
        full = os.path.join(OUTPUTS_DIR, f)
        try:
            size = os.path.getsize(full)
            mtime = os.path.getmtime(full)
        except Exception:
            size, mtime = "?", "?"
        print(f"  {f}  ({size} bytes, mtime={mtime})")
print()


print("=== Interpretation ===")
print("Whichever probe shows 'file exists' AND 'content match: True' is the")
print("write path your runtime actually persists. We wire extract_corpus")
print("(and any other lakehouse writes) to use THAT path.")
print()
print("If NONE of them persisted, the lakehouse mount in this runtime is")
print("read-only for writes from the notebook -- different problem,")
print("possibly a permissions / workspace setting issue worth raising")
print("with whoever provisioned your Fabric workspace.")
