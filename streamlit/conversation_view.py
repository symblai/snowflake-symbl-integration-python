from datetime import datetime

import streamlit as st

from data_utils import get_conversation_data, get_conversation_objections
from utils import get_session, format_datetime

session = get_session()


@st.dialog("Conversation Details", width="large")
def conversation_view():
    conversation_id = st.session_state.conversation_id
    if conversation_id is None:
        st.write("Please select a conversation to view")
        return

    with st.container():
        conversation_data = get_conversation_data(conversation_id, session)
        if len(conversation_data) > 0:
            conversation_data = conversation_data.iloc[0]
            with st.container():
                name = conversation_data['NAME']
                st.markdown(f"### {name}")
                date_time = conversation_data['DATETIME']
                st.markdown(f"##### {format_datetime(date_time)}")
                with st.container():
                    c1, c2 = st.columns(2)
                    with c1:
                        stage = conversation_data['STAGE']
                        st.markdown(f"#### Stage: {stage}")
                    with c2:
                        account_name = conversation_data['ACCOUNT_NAME']
                        st.markdown(f"#### Account: {account_name}")

            short_bullet_points = conversation_data['SHORT_BULLET_POINTS']

            st.markdown(f"#### Short Bullet Points")
            bullets = short_bullet_points.split("\n")
            for bullet in bullets:
                st.markdown(f"- {bullet}")

        objections = get_conversation_objections(conversation_id, session)

        if len(objections) > 0:
            st.markdown("### Objections")
            for index, row in objections.iterrows():
                st.markdown(f"#### {row['OBJECTION']}")
                st.markdown(row['OBJECTION_RESPONSE'])
        else:
            st.markdown("No objections found for this conversation")






