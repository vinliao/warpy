import asyncio
import os
from time import time

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from src.app.logger import logger
from src.db.query import execute_sql, is_read_only, text_to_sql, upload_df_to_s3
from src.db.utils import get_schema_hash

load_dotenv()

app = FastAPI()


@app.get("/")
def read_root():
    return {"result": "Hello, world! Docs: github.com/vinliao/warpy"}


@app.post("/query")
async def query(query_data: dict):
    query_str = query_data.get("query")
    type_str = query_data.get("type")
    export_str = query_data.get("export")
    valid_types = ["raw", "english", "english-advanced"]

    if type_str not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type: {type_str}. Allowed types: {', '.join(valid_types)}",
        )

    logger.info(f"/query: Query: {query_str}; Type: {type_str}")

    if query_str is None:
        raise HTTPException(status_code=400, detail="Missing query data")

    thoughts = None
    if type_str in ["english", "english-advanced"]:
        advanced = type_str == "english-advanced"
        thoughts, sql = text_to_sql(query_str, advanced)
    else:
        sql = query_str

    logger.info(f"/query: Executing SQL: {sql}")

    if not is_read_only(sql):
        raise HTTPException(
            status_code=403,
            detail="Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed",
        )

    try:
        df = await asyncio.wait_for(execute_sql(sql), timeout=30)
    except asyncio.TimeoutError:
        print("The query took too long to complete. It was cancelled.")
        return None

    if df is None:
        raise HTTPException(status_code=500, detail="SQL execution took too long")

    result = df.to_dict("records") if df is not None else []
    logger.info(f"SQL: {sql}")

    if export_str == "csv" and df is not None:
        unix_timestamp_ms = int(time() * 1000)
        file_name = f"{unix_timestamp_ms}.csv"

        bucket_name = "warpy-datasets"
        uploaded = upload_df_to_s3(df, bucket_name, file_name)
        if uploaded:
            s3_url = f"https://pub-3916d8c82abb435eb70175747fdc2119.r2.dev/{file_name}"
            return {"result": s3_url, "sql": sql, "schema": get_schema_hash()}
        else:
            raise HTTPException(
                status_code=500, detail="Failed to upload the CSV to S3"
            )

    return {
        "result": result,
        "sql": sql,
        "thoughts": thoughts,
        "schema": get_schema_hash(),
    }


@app.post("/translate")
async def translate(query_data: dict):
    query_str = query_data.get("query")
    type_str = query_data.get("type")  # Literal["english", "english-advanced"]
    advanced = type_str == "english-advanced"

    logger.info(f"/translate: Query: {query_str}, type: {type_str}")
    if query_str is None:
        raise HTTPException(status_code=400, detail="Missing query data")

    thoughts, sql = text_to_sql(query_str, advanced)

    return {"thoughts": thoughts, "sql": sql, "schema": get_schema_hash()}


if __name__ == "__main__":
    from uvicorn import run

    port = int(os.getenv("PORT", 8000))
    run("api:app", host="0.0.0.0", port=port, log_level="info")
