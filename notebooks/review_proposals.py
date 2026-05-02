"""Fabric notebook helper -- review and group rule proposals.

Run this AFTER `tools.auto_propose_rule.batch.propose_rules` has written
the proposal markdown files. It buckets the proposals into "hypothesis
fired" (cheap wins, ready to promote) vs "needs human investigation",
and groups the human-investigation cases by their failing-line snippet
so you can see how many distinct constructs are actually broken.

Each `# %%` block below is ONE notebook cell.
"""


# %% [Cell A: list proposals + bucket by type + frequency-rank constructs]

import os
from collections import Counter

PROP_DIR = '/lakehouse/default/Files/sql_logic_extractor/parsing_rules/proposed'

proposals = sorted(p for p in os.listdir(PROP_DIR) if p.endswith('.md'))
print(f"{len(proposals)} proposal(s) under {PROP_DIR}\n")

hypothesis = []      # (filename, suggested rule_id) -- ready to promote
investigation = []   # (filename, redacted_failing_line)

for name in proposals:
    text = open(os.path.join(PROP_DIR, name)).read()
    if "NEEDS HUMAN INVESTIGATION" in text:
        snippet = '(no caret-marked line found)'
        for line in text.splitlines():
            if line.startswith('>> '):
                snippet = line[:160]
                break
        investigation.append((name, snippet))
    else:
        rid = '(unparsed)'
        for line in text.splitlines():
            if line.startswith('# Proposed parsing rule:') and '`' in line:
                rid = line.split('`')[1]
                break
        hypothesis.append((name, rid))

# ---- Hypothesis-driven proposals (promote these first) ----
print(f"== {len(hypothesis)} hypothesis-driven proposals (cheap wins) ==")
hyp_counts = Counter(rid for _, rid in hypothesis)
for rid, n in hyp_counts.most_common():
    print(f"  ({n}x) {rid}")
print()
for name, rid in hypothesis:
    print(f"  {name:40} -> {rid}")

# ---- Human-investigation proposals, GROUPED by snippet ----
print(f"\n== {len(investigation)} need human investigation ==\n")
print("Frequency-ranked failing constructs (paste these to share):")
inv_counts = Counter(snippet.strip() for _, snippet in investigation)
for snippet, n in inv_counts.most_common():
    print(f"  ({n}x) {snippet[:160]}")

# ---- One representative file per unique snippet (to read in detail) ----
print("\nOne representative .md filename per unique construct:")
seen: set[str] = set()
for name, snippet in investigation:
    key = snippet.strip()
    if key in seen:
        continue
    seen.add(key)
    print(f"  {key[:80]:82} -> {name}")


# %% [Cell B: dump one specific proposal's full markdown]

# Set this to one of the filenames printed above to read its full content.
TARGET = 'EXAMPLE_VIEW_NAME.md'   # <-- edit me

path = os.path.join(PROP_DIR, TARGET)
if os.path.isfile(path):
    print(open(path).read())
else:
    print(f"not found: {path}")


# %% [Cell C (optional): copy proposal markdown bodies to one combined file
# under outputs/ so you can download / share / paste in chat]

import os
from pathlib import Path

PROP_DIR = '/lakehouse/default/Files/sql_logic_extractor/parsing_rules/proposed'
OUT = '/lakehouse/default/Files/outputs/proposals_combined.md'

Path(os.path.dirname(OUT)).mkdir(parents=True, exist_ok=True)
with open(OUT, 'w') as out:
    for name in sorted(os.listdir(PROP_DIR)):
        if not name.endswith('.md'):
            continue
        out.write(f"\n\n===== {name} =====\n\n")
        out.write(open(os.path.join(PROP_DIR, name)).read())
print(f"Wrote combined proposals -> {OUT}")
