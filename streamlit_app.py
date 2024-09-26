import streamlit as st
import pandas as pd
import altair as alt

from snowflake.snowpark.context import get_active_session

db_schema = "conversation_db.conversation_analysis"

# Streamlit title
st.title("Conversation Analytics Dashboard")

# Get the active Snowflake session
session = get_active_session()

# Function to fetch conversation stats
def get_conversation_stats(_session):
    query = f"""
    SELECT 
        COUNT(conversation_id) AS count,
        AVG(call_score) AS avg_score,
        SUM(total_silence_sec) AS total_silence,
        SUM(total_talk_time_sec) AS total_talk_time,
        SUM(total_overlap_sec) AS total_overlap
    FROM {db_schema}.Conversation
    """
    data = _session.sql(query).collect()
    return data

# Function to fetch member stats
def get_member_stats(_session):
    query = f"""
    SELECT 
        AVG(pace_wpm) AS avg_pace,
        SUM(talk_time_sec) AS total_talk_time,
        SUM(listen_time_sec) AS total_listen_time,
        SUM(overlap_duration_sec) AS total_overlap
    FROM {db_schema}.Members
    """
    data = _session.sql(query).collect()
    return data

# Function to fetch sentiment data
def get_sentiment_data(_session):
    query = f"""
    SELECT 
        sentiment_label, COUNT(*) AS count, AVG(polarity_score) AS avg_polarity
    FROM {db_schema}.Sentiment
    GROUP BY sentiment_label
    """
    data = _session.sql(query).collect()

    # Convert to DataFrame and set types
    df = pd.DataFrame(data)
    df['SENTIMENT_LABEL'] = df['SENTIMENT_LABEL'].astype(str)
    df['COUNT'] = df['COUNT'].astype(int)
    df['AVG_POLARITY'] = df['AVG_POLARITY'].astype(float)

    return df

# Function to fetch topics data
def get_topics_data(_session):
    query = f"""
    SELECT 
        text AS topic, COUNT(*) AS count
    FROM {db_schema}.Topics
    GROUP BY text
    ORDER BY count DESC
    """
    data = _session.sql(query).collect()

    # Convert to DataFrame and set types
    df = pd.DataFrame(data)
    df['TOPIC'] = df['TOPIC'].astype(str)
    df['COUNT'] = df['COUNT'].astype(int)

    return df

def get_entities_stats(_session):
    query = f"""
    SELECT 
        entity_type, COUNT(*) AS count
    FROM {db_schema}.Entities
    GROUP BY entity_type
    ORDER BY count DESC
    """
    data = _session.sql(query).collect()

    # Convert to DataFrame and set types
    df = pd.DataFrame(data)
    df['ENTITY_TYPE'] = df['ENTITY_TYPE'].astype(str)
    df['COUNT'] = df['COUNT'].astype(int)

    return df

def get_entities_data(_session):
    query = f"""
    SELECT 
        entity_type, entity_subtype, COUNT(*) AS count
    FROM {db_schema}.Entities
    GROUP BY entity_type, entity_subtype
    """
    data = _session.sql(query).collect()

    # Convert to DataFrame and set types
    df = pd.DataFrame(data)
    df['ENTITY_TYPE'] = df['ENTITY_TYPE'].astype(str)
    df['ENTITY_SUBTYPE'] = df['ENTITY_SUBTYPE'].astype(str)
    df['COUNT'] = df['COUNT'].astype(int)

    return df

# Function to fetch questions and next steps
def get_questions_data(_session):
    query = f"""
    SELECT COUNT(question_id) AS total_questions
    FROM {db_schema}.Questions
    """
    data = _session.sql(query).collect()
    return data[0]["TOTAL_QUESTIONS"]

def get_next_steps_data(_session):
    query = f"""
    SELECT COUNT(step_id) AS total_next_steps
    FROM {db_schema}.NextSteps
    """
    data = _session.sql(query).collect()
    return data[0]["TOTAL_NEXT_STEPS"]

# Function to fetch conversation summary
def get_conversation_summary(_session):
    query = f"""
    SELECT 
        conversation_id,
        symbl_insights_url,
        sentiment_arr,
        overall_sentiment,
        talk_time_percent,
        number_of_next_steps,
        top_3_topics,
    FROM {db_schema}.conversation_summary_view
    """
    data = _session.sql(query).collect()
    return pd.DataFrame(data)

conversation_stats = get_conversation_stats(session)[0]



def display():
    # Display main statistics
    with st.container():
        member_stats = get_member_stats(session)[0]
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Num. Conversations", conversation_stats['COUNT'])
        with col2:
            st.metric("Average Call Score", round(float(conversation_stats['AVG_SCORE']), 2))
        with col3:
            st.metric("Average Pace (Words per Minute)", round(float(member_stats['AVG_PACE']), 2))


    # Fetch the conversation summary
    conversation_summary = get_conversation_summary(session)

    # Convert the top_3_topics column (array) to a more readable format for display
    conversation_summary['OVERALL_SENTIMENT'] = conversation_summary['OVERALL_SENTIMENT'].apply(lambda x: "Positive" if x >= 0.3 else "Negative" if x <=-0.3 else "Neutral")
    # conversation_summary['URL'] = conversation_summary['CONVERSATION_ID'].apply(lambda x: get_experience_url(x))

    st.dataframe(conversation_summary,

                 column_config={
                     "CONVERSATION_ID": "ID",
                     "SYMBL_INSIGHTS_URL": st.column_config.LinkColumn(
                         "Details",
                         help="Dive deep with Symbl.ai Insights",
                         max_chars=50,
                         width="small",
                         display_text="View"
                     ),

                     "TALK_TIME_PERCENT": st.column_config.NumberColumn(
                         "Talk Time (%)",
                         min_value=0,
                         max_value=100,
                         format=f"%.2f"
                     ),
                     "SENTIMENT_ARR": st.column_config.AreaChartColumn(
                         "Sentiment", y_min=-1, y_max=1
                     ),
                     "TOP_3_TOPICS": st.column_config.ListColumn(
                         "Top Topics"
                     ),
                     "OVERALL_SENTIMENT": st.column_config.TextColumn(
                         "Overall Sentiment",
                         width="small"
                     ),
                     "NUMBER_OF_NEXT_STEPS": st.column_config.NumberColumn(
                         "# Next Steps",
                         width="small"
                     )
                 })


    # Topic Analysis
    topics_data = get_topics_data(session)

    topic_chart = alt.Chart(topics_data).mark_bar().encode(
        x='TOPIC:N',
        y='COUNT:Q',
        color='TOPIC:N'
    ).properties(
        width=600,
        height=400,
        title="Top Topics Discussed"
    )
    st.altair_chart(topic_chart)

    # Entities

    entities_stats = get_entities_stats(session)

    entities_chart = alt.Chart(entities_stats).mark_arc().encode(
        theta=alt.Theta(field='COUNT', type='quantitative'),  # The size of each slice
        color=alt.Color(field='ENTITY_TYPE', type='nominal', legend=None),  # Color by ENTITY_TYPE
        tooltip=['ENTITY_TYPE:N', 'COUNT:Q']  # Tooltip to show entity type and count

    ).properties(
        width=400,
        height=400,
        title="Entities by Type"
    )
    st.altair_chart(entities_chart)

    entities_data = get_entities_data(session)
    bubble_chart = alt.Chart(entities_data).mark_circle().encode(
        y=alt.Y('ENTITY_TYPE:N', title='Entity Type'),
        x=alt.X('ENTITY_SUBTYPE:N', title='Entity Subtype'),
        size=alt.Size('COUNT:Q', title='Entity Count', scale=alt.Scale(range=[30, 1000])),  # Bubble size based on entity count
        color='ENTITY_TYPE:N',
        tooltip=['ENTITY_TYPE', 'ENTITY_SUBTYPE', 'COUNT']
    ).properties(
        width=800,
        height=500,
        title="Entity Type by Entity Subtype Bubble Chart"
    ).configure_title(
        fontSize=20,
        anchor='start',
        color='gray'
    )
    st.altair_chart(bubble_chart)


if int(conversation_stats['COUNT']) <= 0:
    st.markdown("***No conversations found in database.***")
else:
    display()

