"""The ordered AST-rule registry.

Each entry is one tree-level normalization that runs AFTER sqlglot
produces a parse tree. Rules go HERE with a matching fixture under
fixtures/<rule_id>/. The fixture-driven test in
tests/parsing_rules/test_ast_rules.py validates every entry
automatically.

Ordering matters only when one rule's output feeds another's input.
"""

from sqlglot import exp

from .ast_rule import AstRule


def _drop_table_hints(tree: exp.Expression) -> exp.Expression:
    """Strip T-SQL table hints (`WITH (NOLOCK)` etc.) from every Table.

    Table hints affect locking and the optimizer; they have NO effect
    on column lineage / business logic / English definitions. Dropping
    them at the AST level normalizes the tree so downstream consumers
    don't need to special-case them.
    """
    def visit(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Table) and node.args.get("hints"):
            node.set("hints", None)
        return node
    return tree.transform(visit)


AST_RULES: list[AstRule] = [
    AstRule(
        id="drop_table_hints",
        description=(
            "T-SQL `WITH (NOLOCK)` / `WITH (HOLDLOCK)` etc. table hints "
            "affect locking but not column lineage. Strip them so the "
            "tree carries only semantically relevant nodes."
        ),
        transform=_drop_table_hints,
    ),
    # NOTE: `OPTION (MAXDOP n)` etc. are already silently dropped by
    # sqlglot's T-SQL emitter on round-trip (it warns "Unsupported query
    # option" and emits the SQL without them). No AST rule needed.
]
