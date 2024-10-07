import os
import json

import pandas as pd
import requests
import streamlit as st

from snowflake.core import Root
from snowflake.snowpark import Session

env = os.environ['HOME']

NEBULA_API_KEY = None

try:
    if env == "/home/udf":
        from snowflake.snowpark.context import get_active_session
        import _snowflake
        session = get_active_session()
        try:
            NEBULA_API_KEY = _snowflake.get_generic_secret_string('NEBULA_API_KEY')
        except Exception as e:
            st.error(f"Error in accessing Nebula API key from secrets: {e}")

        try:
            session.use_schema("CONVERSATION_ANALYSIS")
        except Exception as e:
            st.error(f"Error setting schema: {e}")
    else:
        session = Session.builder.configs({
            "account": st.secrets.connections.snowflake.account,
            "user": st.secrets.connections.snowflake.user,
            "password": st.secrets.connections.snowflake.password,
            "database": st.secrets.connections.snowflake.database,
            "schema": st.secrets.connections.snowflake.schema,
            "warehouse": st.secrets.connections.snowflake.warehouse,
            "role": st.secrets.connections.snowflake.role,
        }).create()
        if 'symbl' in st.secrets and 'nebula_api_key' in st.secrets.symbl:
            NEBULA_API_KEY = st.secrets.symbl.nebula_api_key
        else:
            NEBULA_API_KEY = None
except Exception as e:
    print(f"Error connecting to Snowpark: {e}")
    st.toast(f"Error connecting to Snowpark: {e}", icon="❌")


@st.cache_data(show_spinner="Fetching data...")
def get_data(query, _session):
    # print(f"\nExecuting query: \n\n{query}\n")
    try:
        data = _session.sql(query).collect()
        df = pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        df = pd.DataFrame()
    # print(f"\nData fetched: \n{df}\n")
    return df


def init_service_metadata():
    if "service_metadata" not in st.session_state:
        services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
        service_metadata = []
        if services:
            for s in services:
                svc_name = s["name"]
                database_name = s["database_name"]
                schema_name = s["schema_name"]
                full_svc_name = f"{database_name}.{schema_name}.{svc_name}"
                search_svc_desc = session.sql(
                    f"DESC CORTEX SEARCH SERVICE {full_svc_name};"
                ).collect()[0]

                svc_search_col = search_svc_desc["search_column"]
                svc_search_cols = search_svc_desc["columns"]

                service_metadata.append(
                    {"name": svc_name, "search_column": svc_search_col, "search_service_columns": svc_search_cols}
                )

        st.session_state.service_metadata = service_metadata


def get_session():
    return session


@st.cache_data(show_spinner="Fetching data...")
def query_cortex_search_service(query, services=None, limit=5):
    if services is None or len(services) == 0:
        services = ["CONVERSATION_SEARCH_SERVICE", "CALL_SCORE_SEARCH_SERVICE"]
    db, schema = session.get_current_database(), session.get_current_schema()
    schema_split = schema.split(".")
    if len(schema_split) > 1:
        schema = schema_split[1]
    schema = schema.replace("\"", "")
    init_service_metadata()
    service_metadata = st.session_state.service_metadata
    context_str = ""
    _results = []
    root = Root(session)
    for idx, svc in enumerate(service_metadata):
        svc_name = st.session_state.service_metadata[idx]["name"]
        if svc_name not in services:
            continue
        cortex_search_service = (
            root.databases[db]
            .schemas[schema]
            .cortex_search_services[svc_name]
        )
        search_col = service_metadata[idx]["search_column"]
        svc_search_cols = service_metadata[idx]["search_service_columns"].split(',')
        # columns = [search_col] + svc_search_cols
        context_documents = cortex_search_service.search(
            query, columns=svc_search_cols, limit=limit
        )
        results = context_documents.results
        for i, r in enumerate(results):
            context_str += f"Context document {i + 1}: {r[search_col]} \n" + "\n"
            r["service_name"] = svc_name
            # r["columns"] = svc_search_cols
            _results.append(r)

    return context_str, _results


def get_ordinal_suffix(day):
    if 11 <= day <= 13:
        return f"{day}th"
    else:
        last_digit = day % 10
        if last_digit == 1:
            return f"{day}st"
        elif last_digit == 2:
            return f"{day}nd"
        elif last_digit == 3:
            return f"{day}rd"
        else:
            return f"{day}th"


def format_datetime(dt):
    day_with_suffix = get_ordinal_suffix(dt.day)
    formatted_date = dt.strftime(f"%b {day_with_suffix}, %Y - %I:%M %p")
    return formatted_date


def call_nebula(messages=[], context=None, api_key=NEBULA_API_KEY):
    if not api_key:
        raise ValueError("API key is required to call Nebula API")
    url = "https://api-nebula.symbl.ai/v1/model/chat"

    if context:
        messages = [{"role": m["role"], "text": m["text"]} for m in messages]
        messages[-1] = {"role": "human",
                        "text": context + "\n\nAnswer this question -\n\n" + \
                                messages[-1]['text']}

    payload = json.dumps({
        "max_new_tokens": 1024,
        "system_prompt": "You are a sales assistant \"Mira\". You help user to answer questions about sales calls. "
                         "You are respectful, professional and you always respond politely. "
                         "Your outputs are formatted in markdown to make them more readable. ",
        "messages": messages
    })
    headers = {
        'ApiKey': api_key,
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.json()
    return response


def score_colors(val):
    if val > 80:
        return 'color: #25c41f'
    elif 60 < val <= 80:
        return 'color: #eb8531'
    else:
        return 'color: #ed3528'
