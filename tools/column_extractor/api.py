"""HTTP entry point for Tool 1 -- Column extractor (online SaaS).

Implementation deferred to June (per planning/monthly/2026-06.md). Skeleton:

    from fastapi import FastAPI, Depends
    from pydantic import BaseModel
    from sql_logic_extractor.products import extract_columns
    from .auth import verify_subscription

    app = FastAPI()

    class ExtractRequest(BaseModel):
        sql: str
        dialect: str = "tsql"

    @app.post("/api/v1/columns/extract")
    def columns_endpoint(req: ExtractRequest, user=Depends(verify_subscription)):
        inv = extract_columns(req.sql, dialect=req.dialect)
        return {"columns": [
            {"database": c.database, "schema": c.schema,
             "table": c.table, "column": c.column}
            for c in inv.columns
        ]}

The route is intentionally NOT live yet -- adding FastAPI as a dependency
without a deployment target is premature. Wire this up when building the
website in June Week 1.
"""
