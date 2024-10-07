import tomllib

import requests
import snowflake.connector
from dotenv import dotenv_values


def get_secrets():
    with open("secrets.toml", "rb") as f:
        data = tomllib.load(f)
        f.close()
    return data


def get_config():
    config = dotenv_values(".env")
    symbl_credentials = {
        "app_id": config.get("SYMBL_APP_ID"),
        "app_secret": config.get("SYMBL_APP_SECRET"),
        "nebula_api_key": config.get("SYMBL_NEBULA_API_KEY")
    }

    snowflake_credentials = {
        "account": config.get("SNOWFLAKE_ACCOUNT"),
        "user": config.get("SNOWFLAKE_USER"),
        "password": config.get("SNOWFLAKE_PASSWORD"),
        "database": config.get("SNOWFLAKE_DATABASE"),
        "schema": config.get("SNOWFLAKE_SCHEMA"),
        "warehouse": config.get("SNOWFLAKE_WAREHOUSE")
    }

    return symbl_credentials, snowflake_credentials


def symbl_token():
    secrets = get_secrets()

    app_id = secrets["symbl"]["app_id"]
    app_secret = secrets["symbl"]["app_secret"]
    payload = {
        "type": "application",
        "appId": app_id,
        "appSecret": app_secret
    }
    response = requests.post("https://api.symbl.ai/oauth2/token:generate", json=payload)
    return response.json()["accessToken"]


def snowflake_connection():
    secrets = get_secrets()
    snowflake_credentials = secrets["connections"]["snowflake"]
    return snowflake.connector.connect(
        user=snowflake_credentials["user"],
        password=snowflake_credentials["password"],
        account=snowflake_credentials["account"],
        warehouse=snowflake_credentials["warehouse"],
        database=snowflake_credentials["database"],
        schema=snowflake_credentials["schema"]
    )
