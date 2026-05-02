"""Tool 8 -- audit author comments across a corpus of views.

Walks a folder of *.sql views, extracts every SQL comment, classifies
intent, and reports the distribution. Use to ground design decisions
about how comments should surface in Tool 3 / Tool 4 with empirical
data, BEFORE wiring them into the resolver.
"""
