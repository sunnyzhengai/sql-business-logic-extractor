"""HTTP entry point for Tool 3 -- Business logic extractor (online SaaS).

Implementation deferred to June. Skeleton:

    from fastapi import FastAPI, Depends, HTTPException
    from pydantic import BaseModel
    from sql_logic_extractor.products import extract_business_logic
    from sql_logic_extractor.license import LicenseError
    from .auth import verify_subscription

    app = FastAPI()

    class ExtractRequest(BaseModel):
        sql: str
        schema_id: str            # references a schema stored on the server
        dialect: str = "tsql"
        use_llm: bool = False     # default OFF -- engineered mode

    @app.post("/api/v1/business-logic/extract")
    def business_endpoint(req: ExtractRequest, user=Depends(verify_subscription)):
        try:
            bl = extract_business_logic(req.sql, load_schema(req.schema_id),
                                         use_llm=req.use_llm, dialect=req.dialect)
        except LicenseError as e:
            raise HTTPException(status_code=403, detail=str(e))
        return {
            "use_llm": bl.use_llm,
            "column_translations": bl.column_translations,
        }
"""
