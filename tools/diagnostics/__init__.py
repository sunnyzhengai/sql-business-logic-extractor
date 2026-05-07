"""Diagnostic helpers -- standalone scripts for triaging pipeline issues.

Not a regular tool. Functions here are designed to be called once
when something looks wrong (e.g., expected annotations missing in
cohort output) and pinpoint which stage of the corpus->cohort chain
broke.
"""
