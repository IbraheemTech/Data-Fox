#step 1 Extract Schema
from sqlalchemy import create_engine, inspect
import json
import re
import sqlite3

db_url = "sqlite:///amazon.db"

def extract_schema(db_url):
    engine = create_engine(db_url)
    inspector = inspect(engine)
    schema = {}


    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        schema[table_name] = [col['name'] for col in columns]
    return json.dumps(schema)


# Step 2 Text to SQL Query Generation
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import OllamaLLM

def text_to_sql(schema, prompt):
    SYSTEM_PROMPT = """You are an expert SQL query generator. You will be given a natural language question and a database schema generate a valid SQL
    query to answer the prompt. Only use the tables and columns provided in the schema. Ensure the SQL syntax is correct and avoid using unsupported features.
    Output only the sql as your response will be directly used to query the data from the database. No preamble please."""

    prompt_template = ChatPromptTemplate.from_messages([
         ("system", SYSTEM_PROMPT),
         ("user", "Schema:\n{schema}\n\nQuestion:\n{user_prompt}\n\nSQL Query:") ]
        )
    
    model = OllamaLLM(model = "deepseek-r1:8b")
    chain = prompt_template | model

    raw = chain.invoke({
        "schema": schema, "user_prompt": prompt
    })

    return raw


def get_data_from_database(prompt):
    schema = extract_schema(db_url)
    sql_query = text_to_sql(schema, prompt)
    conn = sqlite3.connect("amazon.db")
    cursor = conn.cursor()
    res = cursor.execute(sql_query)
    results = res.fetchall()
    conn.close()

    return results