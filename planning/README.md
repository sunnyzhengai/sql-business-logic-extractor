# Planning

Project planning for the SQL Logic Extractor — cascading goals from monthly down to daily.

## Structure

```
planning/
├── monthly/  # Strategic goals for the month (3-5 themes max)
├── weekly/   # Concrete deliverables for the week (mapped to monthly themes)
└── daily/    # Today's actions (mapped to weekly deliverables)
```

## Cadence

- **Monthly** — set at the start of each month. File name: `YYYY-MM.md` (e.g. `2026-04.md`).
- **Weekly** — set on Monday. File name: `YYYY-MM-DD.md` using the Monday date.
- **Daily** — set first thing. File name: `YYYY-MM-DD.md`.

## How goals chain

Each weekly file should reference the monthly theme it advances. Each daily file should reference the weekly deliverable it advances. If a daily task does not ladder up, ask whether it belongs at all.

## Off-plan weeks / days

If a week or day is off-plan (travel, vacation, on-call, illness), create
the file anyway and put `STATUS: paused — <reason>` near the top, plus a
one-line note on what minimal monitoring (if any) you'll do. The absence
of a file vs. an explicit "paused" file is a cleaner signal — future-you
can tell at a glance whether a missing file means "no plan yet" or
"intentionally off-plan."

## Template files

- `monthly/_template.md`
- `weekly/_template.md`
- `daily/_template.md`
