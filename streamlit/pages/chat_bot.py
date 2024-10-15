import os
import traceback

import pandas as pd
import streamlit as st

from data_utils import get_insight_urls, get_call_scores
from utils import call_nebula, query_cortex_search_service
from utils import get_session

session = get_session()

if os.environ['HOME'] == '/home/udf':
    from app import show_top_controls
    show_top_controls(session)


# def default_suggestions():
#     return pills("Label", ["What pain points are discussed?", "What can Jay Mishra improve in their calls?",
#                            "What are the needs in relation to real-time analytics product?"])


def get_metadata_df(results):
    # merged_df = merge_conversation_and_score_service(results)
    df = pd.DataFrame(results)
    conversation_ids = df['CONVERSATION_ID'].unique().tolist()
    urls_df = get_insight_urls(conversation_ids, session)
    conv_call_scores = get_call_scores(conversation_ids, session)

    for idx, row in df.iterrows():
        conversation_id = row['CONVERSATION_ID']
        url = urls_df[urls_df['CONVERSATION_ID'] == conversation_id]['SYMBL_INSIGHTS_URL'].values[0]
        df.at[idx, 'INSIGHT_URL'] = url
        conv_call_score = conv_call_scores[conv_call_scores['CONVERSATION_ID'] == conversation_id]
        df.at[idx, 'OVERALL_CALL_SCORE'] = conv_call_score['CALL_SCORE'].values[0]
        df.at[idx, 'DATETIME'] = conv_call_score['DATETIME'].values[0]
    df.drop_duplicates(inplace=True)
    return df


def write_metadata(conversations):
    drop_columns = []
    for col in conversations.columns:
        if col not in ['DATETIME', 'DEAL_STAGE', 'ACCOUNT_NAME', 'REP_NAME', 'OVERALL_CALL_SCORE', 'INSIGHT_URL']:
            drop_columns.append(col)

    conversations = conversations.drop(columns=drop_columns)

    st.dataframe(conversations, hide_index=True, column_config={
        "DATETIME": st.column_config.DatetimeColumn(
            "Date/Time",
            format="MMM DD YYYY, HH:mm:ss"),
        "DEAL_STAGE": st.column_config.TextColumn("Deal Stage"),
        "ACCOUNT_NAME": st.column_config.TextColumn("Account Name"),
        "REP_NAME": st.column_config.TextColumn("Rep Name"),
        "OVERALL_CALL_SCORE": st.column_config.ProgressColumn(
            "Call Score",
            format="%d",
            min_value=0,
            max_value=100,
            width="small"
        ),
        "INSIGHT_URL": st.column_config.LinkColumn(
            "Details",
            help="Dive deep with Symbl.ai Insights",
            display_text="View",
            max_chars=5,
            width="small"
        )
    })

    # st.dataframe(call_scores, hide_index=True, column_config={
    #     "criteria_name": st.column_config.TextColumn("Criteria Name"),
    # })


def show_chatbot():
    if "messages" not in st.session_state.keys():
        st.session_state.messages = [{"role": "human", "text": "Hello."},
                                     {"role": "assistant", "text": "Hi! I'm Mira, your sales assistant. How can I help you today?"}]

    # Prompt for user input and save
    if prompt := st.chat_input(key="prompt", placeholder="Ask a question about sales meetings..."):
        st.session_state.messages.append({"role": "human", "text": prompt})
    # display the existing chat messages
    for idx, message in enumerate(st.session_state.messages):
        if idx == 0 and message["role"] == "human":
            continue
        with st.chat_message(message["role"]):
            st.markdown(message["text"])
            if 'metadata' in message:
                write_metadata(message['metadata'])

    # If last message is not from assistant, we need to generate a new response
    if len(st.session_state.messages) >= 1 and st.session_state.messages[-1]["role"] != "assistant":
        # Call LLM
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    prompt_context, results = query_cortex_search_service(prompt)
                    # print(results)
                    response = call_nebula(messages=st.session_state.messages, context=prompt_context)
                    response = response["messages"][-1]["text"]
                    st.write(response)

                    for r in results:
                        del r['CHUNK']
                        if 'TRANSCRIPT' in r:
                            del r['TRANSCRIPT']
                        # if 'SUMMARY' in r:
                        #     del r['SUMMARY']
                        # if 'POSITIVE_FEEDBACK' in r:
                        #     del r['POSITIVE_FEEDBACK']
                        # if 'NEGATIVE_FEEDBACK' in r:
                        #     del r['NEGATIVE_FEEDBACK']

                    # print(results)

                    _metadata = get_metadata_df(results)
                    write_metadata(_metadata)

                    message = {"role": "assistant", "text": response, "metadata": _metadata}
                    st.session_state.messages.append(message)
                except ValueError as e:
                    st.write(
                        "You need to provide a valid API key in NEBULA_API_KEY configuration to use Nebula LLM.")


show_chatbot()
