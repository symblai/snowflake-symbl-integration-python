import os

import altair as alt
import streamlit as st
from data_utils import get_conversation_stats, get_member_stats, get_trackers_by_stage, get_call_score_criteria_scores, \
    get_conversation_overview, get_entities_stats, get_entities_data, get_trackers_by_call_score

from utils import get_session

session = get_session()

if os.environ['HOME'] == '/home/udf':
    from app import show_top_controls

    show_top_controls(session)


def display():
    conversation_stats = get_conversation_stats(session).iloc[0]
    member_stats = get_member_stats(session).iloc[0]

    # Display main statistics
    with st.container():
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Meetings", conversation_stats['COUNT'])
        with col2:
            st.metric("Average Call Score", round(float(conversation_stats['AVG_SCORE']), 2))
        with col3:
            st.metric("Average Pace (WPM)", round(float(member_stats['AVG_PACE']), 2))

    # sales_rep_df, trackers_by_rep_df, trackers_by_stage_df, correlation_df, call_score_criteria_scores_df = load_data(
    #     snowflake_session)

    trackers_by_stage_df = get_trackers_by_stage(session)
    call_score_criteria_scores_df = get_call_score_criteria_scores(session)

    with st.container():
        st.markdown("##### **Overall Scores**")
        criteria_count = len(call_score_criteria_scores_df)
        cols = st.columns(criteria_count)

        for index, row in call_score_criteria_scores_df.iterrows():
            with cols[index]:
                st.metric(row['CRITERIA_NAME'], round(row['AVG_SCORE'], 1))

    # Fetch the conversation summary
    conversation_overview = get_conversation_overview(session)

    # Convert the top_3_topics column (array) to a more readable format for display
    conversation_overview['OVERALL_SENTIMENT'] = conversation_overview['OVERALL_SENTIMENT'].apply(
        lambda x: "Positive" if x >= 0.3 else "Negative" if x <= -0.3 else "Neutral")

    def on_conversation_selected():
        try:
            from conversation_view import conversation_view
            conversation = st.session_state.conversation_summary.selection.rows
            filtered_df = conversation_overview.iloc[conversation]
            if len(filtered_df) > 0:
                st.session_state.conversation_id = filtered_df['CONVERSATION_ID'].values[0]
                conversation_view()
        except AttributeError:
            st.toast("Your Streamlit version does not support `st.dialog`", icon="❌")

    st.dataframe(conversation_overview,
                 key="conversation_overview",
                 selection_mode="single-row",
                 on_select=on_conversation_selected,
                 use_container_width=True,
                 hide_index=True,
                 column_config={
                     "CONVERSATION_ID": None,
                     "CONVERSATION_NAME": "Name",
                     "SYMBL_INSIGHTS_URL": st.column_config.LinkColumn(
                         "Details",
                         help="Dive deep with Symbl.ai Insights",
                         max_chars=5,
                         width="small",
                         display_text="View"
                     ),
                     "DATETIME": st.column_config.DatetimeColumn(
                         "Date/Time",
                         format="MMM DD YYYY, HH:mm a",
                         # width="small"
                     ),
                     "DEAL_STAGE": st.column_config.TextColumn(
                         "Deal Stage",
                         # width="small"
                     ),
                     "ACCOUNT_NAME": st.column_config.TextColumn(
                         "Account",
                         # width="small"
                     ),
                     "SALES_REP": st.column_config.TextColumn(
                         "Sales Rep",
                         width="small"
                     ),
                     "CALL_SCORE": st.column_config.ProgressColumn(
                         "Call Score",
                         min_value=0,
                         max_value=100,
                         format="%f.0",
                         # width="small"
                     ),
                     # "TALK_TIME_PERCENT": st.column_config.NumberColumn(
                     #     "Talk Time (%)",
                     #     min_value=0,
                     #     max_value=100,
                     #     format=f"%.2f"
                     # ),
                     "SENTIMENT_ARR": st.column_config.AreaChartColumn(
                         "Sentiment", y_min=-1, y_max=1
                     ),
                     # "TOP_3_TOPICS": st.column_config.ListColumn(
                     #     "Top Topics"
                     # ),
                     "OVERALL_SENTIMENT": st.column_config.TextColumn(
                         "Overall Sentiment",
                         width="small"
                     ),
                     "NUMBER_OF_OBJECTIONS": st.column_config.NumberColumn(
                         "# Objections",
                         width="small"
                     ),
                     "NUMBER_OF_NEXT_STEPS": st.column_config.NumberColumn(
                         "# Next Steps",
                         width="small"
                     )
                 })
    trackers_by_call_score = get_trackers_by_call_score(session)
    trackers_by_call_score['AVG_CALL_SCORE'] = trackers_by_call_score['AVG_CALL_SCORE'].astype(float)
    fig_trackers_call_score = alt.Chart(trackers_by_call_score).mark_circle().encode(
        x=alt.X('AVG_CALL_SCORE:Q', title='Call Score', scale=alt.Scale(domain=[0, 100])),
        y=alt.Y('AVG_OPPORTUNITY_AMOUNT:Q', title='Deal Amount ($)', ),
        color=alt.Color('TRACKER_NAME:N', title='Category'),
        size=alt.Size('TRACKER_COUNT:Q', title='# Instances', scale=alt.Scale(range=[50, 1000])),
    ).properties(
        title="Trackers by Call Score"
    )
    st.altair_chart(fig_trackers_call_score, use_container_width=True)

    # Visualization: Trackers by Opportunity Stage
    fig_trackers_stage = alt.Chart(trackers_by_stage_df).mark_bar().encode(
        y=alt.Y('STAGE', title='Opportunity Stage'),
        x=alt.X('TRACKER_COUNT', title='# Instances'),
        color=alt.Color('TRACKER_NAME', title='Category'),
    ).properties(
        title="Trackers by Opportunity Stage",
        height=300
    )
    st.altair_chart(fig_trackers_stage, use_container_width=True)

    # Topic Analysis
    # topics_data = get_topics_data(snowflake_session)
    #
    # topic_chart = alt.Chart(topics_data).mark_bar().encode(
    #     x='TOPIC:N',
    #     y='COUNT:Q',
    #     color='TOPIC:N'
    # ).properties(
    #     width=600,
    #     height=400,
    #     title="Top Topics Discussed"
    # )
    # st.altair_chart(topic_chart)
    #
    # Entities
    with st.container():
        col1, col2 = st.columns(2)
        with col1:
            entities_stats = get_entities_stats(session)
            entities_chart = alt.Chart(entities_stats).mark_arc().encode(
                theta=alt.Theta(field='COUNT', type='quantitative'),  # The size of each slice
                color=alt.Color(field='ENTITY_TYPE', type='nominal', legend=None),  # Color by ENTITY_TYPE
                tooltip=[alt.Tooltip('ENTITY_TYPE:N', title="Category"), alt.Tooltip('COUNT:Q', title="# Instances")]
                # Tooltip to show entity type and count

            ).properties(
                width=400,
                height=400,
                title="Entity Categories"
            )
            st.altair_chart(entities_chart, use_container_width=True)
        with col2:
            entities_data = get_entities_data(session)
            entities_data.sort_values(by='COUNT', ascending=False, inplace=True)
            bubble_chart = alt.Chart(entities_data).mark_bar().encode(
                y=alt.Y('ENTITY_TYPE:N', title='Entity Category', stack='zero'),
                x=alt.X('COUNT:Q', title='# Instances'),
                # size=alt.Size('COUNT:Q', title='Entity Count', scale=alt.Scale(range=[30, 1000])),
                # Bubble size based on entity count
                color=alt.Color('ENTITY_SUBTYPE:N', title='Entity Type'),
            ).properties(
                width=800,
                height=500,
                title="Entities by Category"
            )
            st.altair_chart(bubble_chart, use_container_width=True)


with st.container():
    display()
