"""Just-in-time question-answering over ingested SQL business logic.

Instead of opening a ticket and waiting months for IT to write a report,
a business analyst asks a question in a notebook cell and gets a grounded
answer in ~5 seconds, with citations back to the actual SQL views/procs.

Architecture (three layers):
  1. Index     -- pre-computed structural + semantic indices over corpus
  2. Retriever -- routes questions to graph traversal or semantic search
  3. Synthesizer -- formats answers with citations

Phase 1 (this module): structural queries only (no LLM at query time).
  - Table lookup: "which views use REFERRAL?"
  - Column lookup: "which views produce PAT_ID?"
  - View lookup: "what does VW_REFERRAL_STATUS do?"
  - Filter lookup: "which views filter on denied status?"

Usage in a notebook:
    from tools.jit import ask
    ask.build_index("corpus.jsonl")
    ask("which views use the REFERRAL table?")
"""
