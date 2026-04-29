"""HTTP entry point for Tool 4 -- Report description generator (online SaaS).

Implementation deferred to June. Skeleton:

    from fastapi import FastAPI, Depends, HTTPException
    from pydantic import BaseModel
    from sql_logic_extractor.products import generate_report_description
    from sql_logic_extractor.license import LicenseError
    from .auth import verify_subscription

    app = FastAPI()

    class ExtractRequest(BaseModel):
        sql: str
        schema_id: str
        dialect: str = "tsql"
        use_llm: bool = False     # default OFF

    @app.post("/api/v1/report/describe")
    def report_endpoint(req: ExtractRequest, user=Depends(verify_subscription)):
        try:
            desc = generate_report_description(req.sql, load_schema(req.schema_id),
                                                 use_llm=req.use_llm, dialect=req.dialect)
        except LicenseError as e:
            raise HTTPException(status_code=403, detail=str(e))
        return {
            "use_llm": desc.use_llm,
            "query_summary": desc.query_summary,
            "primary_purpose": desc.primary_purpose,
            "key_metrics": desc.key_metrics,
        }
"""
