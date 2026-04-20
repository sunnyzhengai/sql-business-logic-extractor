---
description: Ingest new sources from wiki/raw/ into the conceptual wiki (Karpathy pattern)
---

# /ingest

Process unprocessed sources in `wiki/raw/` into the human-curated wiki at `wiki/`.

## What to do

Arguments: `$ARGUMENTS` — optional filename in `wiki/raw/`. If empty, process all unprocessed sources.

### Step 1 — list candidate sources

```bash
ls wiki/raw/ 2>/dev/null
```

If a specific filename was given in $ARGUMENTS, process only that one. Otherwise process every file in `wiki/raw/` that is NOT already referenced in `wiki/log.md` (grep for the filename).

If there are no unprocessed sources, say so and stop. Do not invent content.

### Step 2 — for each source

Read the file (Read tool for text/markdown/PDF; for images, describe what you see).

Then:

1. **Discuss it with the user briefly.** 1-2 sentences on what this source contains and what it adds. Wait for their reaction before writing anything, in case they want to redirect focus. Skip this for screenshots with obvious content.

2. **Identify which concept pages it touches.** Check `wiki/concepts/` for existing pages by name and alias. If a relevant concept page doesn't exist, propose a new one with its slug. Get user confirmation before creating new concept pages.

3. **Update affected concept pages.** For each affected page:
   - Add a citation to `sources:` frontmatter (e.g. `raw/customer-call-2026-04.md`)
   - Update the body if the source adds, contradicts, or refines existing content. Preserve the "Definition → Why it matters → Open questions" structure defined in `wiki/SCHEMA.md`.
   - If the source contradicts existing content, flag it in "Open questions" rather than silently overwriting.

4. **Cross-link.** If the update mentions another concept that has a wiki page, add a `see_also` entry both ways.

5. **Log it.** Append one line to `wiki/log.md` at the TOP of the entries:
   ```
   YYYY-MM-DD  ingest  <source filename>: <1-line takeaway>  [affected: <page1>, <page2>]
   ```
   Use today's date from the environment context.

### Step 3 — report

One short summary: N sources processed, which pages touched, any new concept pages created, any open questions flagged.

## Guardrails

- **Never invent facts not in the source.** If the source is thin, the update is thin.
- **Never move files out of `raw/`.** The raw source stays put as the audit trail.
- **Don't edit `graphify-out/wiki/`** — that's auto-generated from code.
- **Don't touch decisions/** unless the user explicitly asks.
- If a source is irrelevant (spam, accidental file), say so and add one log line noting it was skipped. Do not silently ignore.
