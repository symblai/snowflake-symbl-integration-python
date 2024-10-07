CREATE OR REPLACE NETWORK RULE symbl_apis_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.symbl.ai', 'api-nebula.symbl.ai');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION symbl_apis_access_integration
  ALLOWED_NETWORK_RULES = (symbl_apis_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (nebula_api_key)
  ENABLED = true;

ALTER STREAMLIT CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_analysis
SET EXTERNAL_ACCESS_INTEGRATIONS = (symbl_apis_access_integration)
    SECRETS = ('nebula_api_key' = CONVERSATION_DB.CONVERSATION_ANALYSIS.nebula_api_key);
