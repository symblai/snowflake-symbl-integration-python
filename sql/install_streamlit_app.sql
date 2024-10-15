USE DATABASE CONVERSATION_DB;

CREATE STAGE IF NOT EXISTS CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

PUT file://streamlit/app.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage overwrite = true auto_compress = false;
PUT file://streamlit/pages/overview.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage/pages/ overwrite = true auto_compress = false;
PUT file://streamlit/pages/accounts.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage/pages/ overwrite = true auto_compress = false;
PUT file://streamlit/pages/reps.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage/pages/ overwrite = true auto_compress = false;
PUT file://streamlit/pages/chat_bot.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage/pages/ overwrite = true auto_compress = false;
PUT file://streamlit/conversation_view.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage overwrite = true auto_compress = false;
PUT file://streamlit/search.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage overwrite = true auto_compress = false;
PUT file://streamlit/utils.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage overwrite = true auto_compress = false;
PUT file://streamlit/data_utils.py @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage overwrite = true auto_compress = false;
PUT file://streamlit/environment.yml @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage overwrite = true auto_compress = false;
PUT file://streamlit/.streamlit/config.toml @CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage/.streamlit/ overwrite = true auto_compress = false;

CREATE OR REPLACE STREAMLIT CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis
    ROOT_LOCATION = '@CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis_stage'
    TITLE = 'Sales Analysis with Cortex and Symbl.ai'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = COMPUTE_WH;

ALTER STREAMLIT CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis
    SET EXTERNAL_ACCESS_INTEGRATIONS = (CONVERSATION_DB.CONVERSATION_ANALYSIS.symbl_apis_access_integration)
    SECRETS = ('nebula_api_key' = CONVERSATION_DB.CONVERSATION_ANALYSIS.nebula_api_key);




