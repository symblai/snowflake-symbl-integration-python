# Overview

This is a sample project to demonstrate how to use Symbl's APIs to extract various insights from audio calls and
meetings, store them in the Snowflake database, and visualize them using a Streamlit application.

By merging unstructured conversational data with existing structured business data, this integration helps you uncover
deeper insights, enabling more informed decision-making across customer service, sales, recruitment, and operations.

The project has three main artifacts:

- [Setup SQL script](./sql/setup.sql): This script sets up the Snowflake database and tables required for the project.
- [Python processing script](./main.py): This script processes the audio files, extracts insights using Symbl's APIs,
  and stores them in the Snowflake database.
- [Streamlit application](./streamlit_app.py): This application visualizes the insights stored in the Snowflake
  database.

# Prerequisites

- Symbl.ai Account: Sign up for an account at [Symbl.ai](https://platform.symbl.ai) and retrieve your App ID and App
  Secret.
- Snowflake Account: Sign up for a [Snowflake](https://signup.snowflake.com/) account and gather the necessary
  credentials (account ID, username, and password).
- Python 3.8 or higher: Ensure Python is installed on your local machine.

# Setup and Run
- Run the [setup SQL script](./sql/setup.sql) in your Snowflake account to create the required tables.
- Install the required Python packages by running `pip install -r requirements.txt`.
- Copy `.env.default` file to `.env` file, and update with your Symbl and Snowflake credentials.
- Create new Streamlit app in Snowflake and copy/paste code from [streamlit_app.py](./streamlit_app.py) to the app.
- Run the Python processing script by running `python main.py`.

# Learn More
- [Symbl Docs](https://docs.symbl.ai/)
- [Snowflake Docs](https://docs.snowflake.com)
- [Streamlit Docs](https://docs.streamlit.io/)
