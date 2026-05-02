"""Tool 5 -- find columns across views that share the same business logic.

For data governance: when 50 views all compute "denied referrals" with
slightly different alias names but the same AST shape, this tool surfaces
those clusters so the governance team can register ONE business term
("denied referrals") instead of 50 duplicates in Collibra.
"""
