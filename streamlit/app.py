import streamlit as st


from data_utils import get_date_range, get_conversation_stats
from search import show_search
from utils import get_session


def show_top_controls(_session):
    date_range = get_date_range(_session)
    if date_range.empty:
        _to = datetime.datetime.now()
        _from = _to - datetime.timedelta(days=14)
    else:
        _from = date_range["MIN_DATE"][0]
        _to = date_range["MAX_DATE"][0]

    st.session_state.min_date = _from
    st.session_state.max_date = _to

    with st.container():
        _, __, ___, col1, col2 = st.columns([0.4, 0.1, 0.1, 0.2, 0.2])
        with _:
            show_search()
        with __:
            st.write("")
        with ___:
            st.write("")
        with col1:
            from_date = st.date_input("From", _from, min_value=_from, max_value=_to, key="from_date")
        with col2:
            to_date = st.date_input("To", _to, min_value=_from, max_value=_to, key="to_date")


def display(snowflake_session):
    # Display the date selector
    show_top_controls(snowflake_session)


st.set_page_config(layout="wide", page_title="Sales Calls Analysis")

# Streamlit title
st.title("Sales Calls Analysis")
session = get_session()
conversation_stats = get_conversation_stats(session).iloc[0]

if int(conversation_stats['COUNT']) <= 0:
    st.markdown("***No conversations found in database.***")
else:
    display(session)
    pg = st.navigation([
        st.Page("overview.py", title="Overview", default=True),
        st.Page("reps.py", title="Sales Reps"),
        st.Page("accounts.py", title="Accounts"),
        st.Page("chat_bot.py", title="Chat")])
    pg.run()
