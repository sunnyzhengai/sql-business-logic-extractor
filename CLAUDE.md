## graphify

This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `graphify update .` to keep the graph current (AST-only, no API cost)

## Conceptual wiki (Karpathy pattern)

Human-curated knowledge lives in `wiki/` (separate from auto-generated `graphify-out/wiki/`). See `wiki/SCHEMA.md` for layout.

Rules:
- When the user asks about domain concepts (healthcare lineage, Collibra connector semantics, customer feedback, design history), check `wiki/index.md` and `wiki/concepts/` first. Cite the concept page in your answer.
- Sources land in `wiki/raw/`. Process them with `/ingest` — never silently move or delete raw files.
- Never edit files in `graphify-out/wiki/` by hand; that directory is regenerated from the code graph.
- After you create a git commit in this session, append one line to `wiki/log.md` at the top of the entries:
  `YYYY-MM-DD  commit  <short-sha> <subject>  [affected: <wiki pages touched, if any>]`
  Use the date from the environment context. Skip this only if the commit is trivial (whitespace, typo).
- If you learn a durable domain fact during a conversation that isn't in the wiki yet, propose a concept page — don't silently rely on memory. Wiki over memory for project knowledge.
