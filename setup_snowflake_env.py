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
    conn = snowflake_connection()
    try:
        secrets = get_secrets()
        nebula_api_key = 'xxx'
        if 'symbl' in secrets and 'nebula_api_key' in secrets['symbl']:
            nebula_api_key = secrets["symbl"]["nebula_api_key"]
        try:
            with open("sql/create_symbl_network_integration.sql", "r", encoding='utf-8') as f:
                for cur in conn.execute_stream(f, params={'nebula_api_key': nebula_api_key}):
                    for ret in cur:
                        print(ret)
        except Exception as e:
            print(f"Error creating Network integration: {e}")
            print("Nebula Functionality in Chat page will not work.")
            print("Please create the External Network Integration manually using "
                  "sql/create_symbl_network_integration.sql")

        print("Running sql/install_streamlit_app.sql...")
        try:
            with open("sql/install_streamlit_app.sql", "r", encoding='utf-8') as f:
                for cur in conn.execute_stream(f):
                    for ret in cur:
                        print(ret)
        except Exception as e:
            print(f"Error installing streamlit app: {e}")
            print("If there was error reported during external network integration creation, this error is expected.")
    finally:
        conn.close()
        print("Execution completed.")

print("Snowflake environment setup completed.")
