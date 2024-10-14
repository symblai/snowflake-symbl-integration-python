import streamlit as st

from utils import query_cortex_search_service


def show_search():
    def search():
        if "search" not in st.session_state:
            st.session_state.search = ""
        query = st.session_state.search
        if query and len(query) > 0:
            with st.spinner("Searching..."):
                try:
                    _, results = query_cortex_search_service(query,
                                                             services=["CONVERSATION_SEARCH_SERVICE"],
                                                             limit=20)
                    _results = []
                    for r in results:
                        _results.append(r['CONVERSATION_ID'])
                    st.session_state.conversation_ids = list(set(_results))
                except ValueError as e:
                    st.toast("Error occurred while searching. Please try again.")

    st.text_input("Search",
                  key="search",
                  placeholder="Search (e.g. Nathan, DataOps, data security, pain points, etc.)",
                  on_change=search)
