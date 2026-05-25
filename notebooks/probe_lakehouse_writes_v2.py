"""Round-two probe for the corpus-write-doesn't-persist mystery.

Yang's earlier probe results showed:
  - Probe 1 (plain Python open(w) for a NEW file): succeeded, file
    persisted, content matched.
  - Probe 2 (notebookutils.fs.put): silent no-op.
  - Probe 3 (notebookutils.fs.cp from /tmp): Py4JJavaError.

But extract_corpus -- which uses the SAME plain Python open(w) --
claimed it wrote corpus_v2.jsonl successfully and the file does NOT
exist on disk. Same runtime, same API, different outcome.

This cell triangulates the contradiction with two follow-up tests:

  Cell A: list everything currently in the outputs/ directory, with
          sizes + mtimes. Confirms whether _probe_python.txt from
          earlier persists (durable write?) and shows what's there
          NOW (maybe corpus_v2.jsonl landed with an unexpected name).

  Cell B: minimal streaming-write test that mirrors what extract_corpus
          does -- multiple line writes + flush + close. If extract's
          failure is about streaming semantics (vs. single-shot write),
          this reproduces it. If this writes successfully, the bug is
          something more specific to extract_corpus that we'll dig into.

Paste both cell outputs back.
"""


# %% [Cell A: directory inventory + probe-file persistence check]

import os
import datetime

OUTPUTS_DIR = "/lakehouse/default/Files/outputs"

print(f"=== Files in {OUTPUTS_DIR} ===")
if not os.path.isdir(OUTPUTS_DIR):
    print(f"  DIRECTORY DOESN'T EXIST. Path may be wrong.")
else:
    for f in sorted(os.listdir(OUTPUTS_DIR)):
        full = os.path.join(OUTPUTS_DIR, f)
        try:
            size = os.path.getsize(full)
            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(full))
            print(f"  {f:50s}  {size:>10d} bytes  {mtime}")
        except Exception as e:
            print(f"  {f:50s}  ERROR: {e}")

# Probe-file persistence: did _probe_python.txt from the prior probe
# stick around? If yes, plain Python writes are DURABLE in this runtime
# (not just instant-but-transient). If no, that's a different problem.
probe = os.path.join(OUTPUTS_DIR, "_probe_python.txt")
print(f"\n_probe_python.txt persistence check:")
print(f"  exists?  {os.path.exists(probe)}")
if os.path.exists(probe):
    print(f"  size:    {os.path.getsize(probe)} bytes")
    print(f"  mtime:   {datetime.datetime.fromtimestamp(os.path.getmtime(probe))}")


# %% [Cell B: streaming-write test that mirrors extract_corpus's pattern]

import os

test_path = "/lakehouse/default/Files/outputs/_test_streaming.jsonl"

# Ensure clean slate.
if os.path.exists(test_path):
    try:
        os.remove(test_path)
        print(f"Removed pre-existing {test_path}")
    except Exception as e:
        print(f"Couldn't remove pre-existing: {e}")
else:
    print(f"(No pre-existing {test_path})")

# Streaming write: open in 'w', write multiple lines with flush after
# each, then exit the with-block (which calls close()). This mirrors
# extract_corpus's per-view-write pattern.
try:
    with open(test_path, "w", encoding="utf-8") as fh:
        for i in range(3):
            fh.write(f'{{"line": {i}, "view_name": "test_{i}"}}\n')
            fh.flush()
    print("Streaming write returned without error.")
except Exception as e:
    print(f"Streaming write raised: {type(e).__name__}: {e}")

# Now check whether the file is actually on disk after close.
print(f"\nAfter close:")
print(f"  exists?  {os.path.exists(test_path)}")
if os.path.exists(test_path):
    print(f"  size:    {os.path.getsize(test_path)} bytes")
    print(f"  content:")
    for line in open(test_path):
        print(f"    {line.rstrip()}")
else:
    print(f"  FILE DOES NOT EXIST. Streaming writes don't persist in this runtime.")
    print(f"  This explains extract_corpus's behavior -- it uses the same pattern.")
