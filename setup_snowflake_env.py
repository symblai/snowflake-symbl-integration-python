import argparse
import shutil
from codecs import open

from utils import snowflake_connection, get_secrets

parser = argparse.ArgumentParser(description='Setup Snowflake environment.')
parser.add_argument('--install_streamlit_app',
                    default=False,
                    action='store_true',
                    help='Install streamlit app to Snowflake')

args = parser.parse_args()
install_streamlit_app = args.install_streamlit_app

print("Setting up Snowflake environment...")
print("Running sql/setup.sql...")
conn = snowflake_connection()
try:
    with open("sql/setup.sql", "r", encoding='utf-8') as f:
        for cur in conn.execute_stream(f):
            for ret in cur:
                print(ret)
finally:
    conn.close()
    print("Execution completed.")

print('Copying secrets.toml to streamlit/.streamlit/secrets.toml')
shutil.copyfile("secrets.toml", "streamlit/.streamlit/secrets.toml")

if install_streamlit_app:
    print("Installing streamlit app to Snowflake...")
    print("Running sql/install_streamlit_app.sql...")
    conn = snowflake_connection()
    try:
        with open("sql/install_streamlit_app.sql", "r", encoding='utf-8') as f:
            for cur in conn.execute_stream(f):
                for ret in cur:
                    print(ret)
        secrets = get_secrets()
        if 'symbl' in secrets and 'nebula_api_key' in secrets['symbl']:
            nebula_api_key = secrets["symbl"]["nebula_api_key"]
            conn.execute_string(
                f"CREATE OR REPLACE SECRET nebula_api_key TYPE = GENERIC_STRING SECRET_STRING = '{nebula_api_key}'")

        with open("sql/create_symbl_network_integration.sql", "r", encoding='utf-8') as f:
            for cur in conn.execute_stream(f):
                for ret in cur:
                    print(ret)
    finally:
        conn.close()
        print("Execution completed.")

print("Snowflake environment setup completed.")
