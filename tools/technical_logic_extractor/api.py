"""HTTP entry point for Tool 2 -- Technical logic extractor (online SaaS).

Implementation deferred to June. Skeleton:

    from fastapi import FastAPI, Depends
    from pydantic import BaseModel
    from sql_logic_extractor.products import extract_technical_lineage
    from .auth import verify_subscription

    app = FastAPI()

    class ExtractRequest(BaseModel):
        sql: str
        dialect: str = "tsql"

    @app.post("/api/v1/lineage/extract")
    def lineage_endpoint(req: ExtractRequest, user=Depends(verify_subscription)):
        lineage = extract_technical_lineage(req.sql, dialect=req.dialect)
        return {
            "inventory": {"columns": [vars(c) for c in lineage.inventory.columns]},
            "resolved_columns": lineage.resolved_columns,
            "query_filters": lineage.query_filters,
        }
"""
