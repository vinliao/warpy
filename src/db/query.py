import io
import json
import os
from typing import Dict, Optional, Tuple

import boto3
import pandas as pd
import pymysql
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from langchain.chat_models import ChatOpenAI
from langchain.schema import AIMessage, HumanMessage, SystemMessage

load_dotenv()


def upload_df_to_s3(df: pd.DataFrame, bucket_name: str, object_name: str):
    s3 = boto3.resource(
        "s3",
        endpoint_url=(
            "https://159cf33b100c2bee8783ee5604f0f62e.r2.cloudflarestorage.com"
        ),
        aws_access_key_id=os.getenv("WARPY_R2_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("WARPY_R2_SECRET_KEY"),
    )
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  # Reset the buffer's file pointer to the beginning

        s3.Object(bucket_name, object_name).put(Body=csv_buffer.getvalue())
        print(f"CSV uploaded successfully to {bucket_name} as {object_name}")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False


def remove_imports_from_models(model_str):
    start = model_str.index("class")
    return model_str[start:]


def get_sqlalchemy_models():
    with open("src/db/models.py", "r") as f:
        sqlalchemy_models = f.read()
    return remove_imports_from_models(sqlalchemy_models)


def get_connection():
    try:
        return pymysql.connect(
            host=os.getenv("PLANETSCALE_HOST"),
            user=os.getenv("PLANETSCALE_USERNAME"),
            password=os.getenv("PLANETSCALE_PASSWORD"),
            database=os.getenv("PLANETSCALE_DATABASE"),
            ssl=(
                {"ca": os.getenv("PLANETSCALE_CACERT")}
                if os.getenv("PLANETSCALE_CACERT")
                else None
            ),
        )
    except Exception as e:
        print(e)
        raise e


def is_read_only(query: str) -> bool:
    disallowed_words = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
    ]
    query_upper = query.upper()

    return not any(word in query_upper for word in disallowed_words)


def execute_sql(
    query: str, connection: pymysql.Connection = get_connection()
) -> Optional[pd.DataFrame]:
    if not is_read_only(query):
        return None

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]

            if result:
                df = pd.DataFrame(result, columns=column_names)
                return df
            else:
                return None
    except Exception as e:
        print(e)
        return None


def text_to_sql(query: str, advanced: bool = False) -> Tuple[Dict, str]:
    if advanced is True:
        model_name = "gpt-4"
        prompt_file = "src/db/query_prompt_advanced.txt"
    else:
        model_name = "gpt-3.5-turbo"
        prompt_file = "src/db/query_prompt.txt"

    chat = ChatOpenAI(
        temperature=0, model_name=model_name, openai_api_key=os.getenv("OPENAI_API_KEY")
    )  # type: ignore

    with open(prompt_file, "r") as f:
        initial_prompt_raw = f.read()

    initial_prompt_raw = initial_prompt_raw.replace("<QUERY>", query)
    initial_prompt_raw = initial_prompt_raw.replace("<MODELS>", get_sqlalchemy_models())

    system_prompt = SystemMessage(content="You are a SQL writer")
    initial_prompt = HumanMessage(content=initial_prompt_raw)
    ai_response = AIMessage(content="Yes.")
    query_message = HumanMessage(content=query)

    messages = [system_prompt, initial_prompt, ai_response, query_message]

    response = chat(messages)
    content = json.loads(response.content)
    thoughts = content["thoughts"] if advanced else None
    return thoughts, content["sql"]
