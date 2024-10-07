import pandas as pd
import streamlit as st

from utils import get_data


def add_date_and_search_where_clause(query, prefix=""):
    from_date = st.session_state.get("from_date", None)
    to_date = st.session_state.get("to_date", None)
    if from_date and to_date:
        query += f" WHERE {prefix}datetime BETWEEN '{from_date}' AND '{to_date}'"

    if st.session_state.get("conversation_ids", None):
        if 'WHERE' in query:
            query += " AND"
        else:
            query += " WHERE"
        query += f" {prefix}conversation_id IN ({','.join(st.session_state.conversation_ids)})"
    return query


def get_date_range(_session):
    query = f"""
    SELECT 
        MIN(datetime) AS min_date,
        MAX(datetime) AS max_date
    FROM Conversation
    """
    df = get_data(query, _session)
    return df


# Function to fetch conversation stats
def get_conversation_stats(_session):
    query = f"""
    SELECT 
        COUNT(conversation_id) AS count,
        AVG(call_score) AS avg_score,
        SUM(total_silence_sec) AS total_silence,
        SUM(total_talk_time_sec) AS total_talk_time,
        SUM(total_overlap_sec) AS total_overlap
    FROM Conversation
    """
    query = add_date_and_search_where_clause(query)
    return get_data(query, _session)


# Function to fetch member stats
def get_member_stats(_session):
    query = f"""
    SELECT 
        AVG(pace_wpm) AS avg_pace,
        SUM(talk_time_sec) AS total_talk_time,
        SUM(listen_time_sec) AS total_listen_time,
        SUM(overlap_duration_sec) AS total_overlap
    FROM Members m
    JOIN Conversation c ON m.conversation_id = c.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    return get_data(query, _session)


# Function to fetch sentiment data
def get_sentiment_data(_session):
    query = f"""
    SELECT 
        sentiment_label, COUNT(*) AS count, AVG(polarity_score) AS avg_polarity
    FROM Sentiment s
    JOIN Conversation c ON s.conversation_id = c.conversation_id
    GROUP BY sentiment_label
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    df = get_data(query, _session)

    df['SENTIMENT_LABEL'] = df['SENTIMENT_LABEL'].astype(str)
    df['COUNT'] = df['COUNT'].astype(int)
    df['AVG_POLARITY'] = df['AVG_POLARITY'].astype(float)

    return df


# Function to fetch topics data
def get_topics_data(_session):
    query = f"""
    SELECT 
        text AS topic, COUNT(*) AS count
    FROM Topics t
    JOIN Conversation c ON t.conversation_id = c.conversation_id
    GROUP BY text
    ORDER BY count DESC
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    df = get_data(query, _session)

    df['TOPIC'] = df['TOPIC'].astype(str)
    df['COUNT'] = df['COUNT'].astype(int)

    return df


def get_entities_stats(_session):
    query = f"""
    SELECT 
        entity_type, COUNT(*) AS count
    FROM Entities e
    JOIN Conversation c ON e.conversation_id = c.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY entity_type ORDER BY count DESC"
    df = get_data(query, _session)

    df['ENTITY_TYPE'] = df['ENTITY_TYPE'].astype(str)
    df['COUNT'] = df['COUNT'].astype(int)

    return df


def get_entities_data(_session):
    query = f"""
    SELECT 
        entity_type, entity_subtype, COUNT(*) AS count
    FROM Entities e
    JOIN Conversation c ON e.conversation_id = c.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY entity_type, entity_subtype"
    df = get_data(query, _session)

    df['ENTITY_TYPE'] = df['ENTITY_TYPE'].astype(str)
    df['ENTITY_SUBTYPE'] = df['ENTITY_SUBTYPE'].astype(str)
    df['COUNT'] = df['COUNT'].astype(int)

    return df


# Function to fetch questions and next steps
def get_questions_data(_session):
    query = f"""
    SELECT COUNT(question_id) AS total_questions
    FROM Questions q
    JOIN Conversation c ON q.conversation_id = c.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    df = get_data(query, _session)
    return df[0]["TOTAL_QUESTIONS"]


def get_next_steps_data(_session):
    query = f"""
    SELECT COUNT(step_id) AS total_next_steps
    FROM NextSteps ns
    JOIN Conversation c ON ns.conversation_id = c.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    df = get_data(query, _session)
    return df[0]["TOTAL_NEXT_STEPS"]


def get_insight_urls(conversation_ids=[], _session=None):
    query = f"""
    SELECT 
        conversation_id,
        SYMBL_INSIGHTS_URL
    FROM conversation_summary_view
    """
    if conversation_ids:
        query += f" WHERE conversation_id IN ({','.join(conversation_ids)})"
    return get_data(query, _session)


def get_call_scores(conversation_ids=[], _session=None):
    query = f"""
    SELECT 
        conversation_id,
        call_score,
        datetime
    FROM Conversation
    """
    if conversation_ids:
        query += f" WHERE conversation_id IN ({','.join(conversation_ids)})"
    return get_data(query, _session)


def get_conversation_overview(_session):
    query = f"""
    SELECT 
        conversation_id,
        conversation_name,
        SYMBL_INSIGHTS_URL,
        datetime,
        account_name,
        deal_stage,
        sales_rep,
        call_score,
        sentiment_arr,
        overall_sentiment,
        number_of_objections,
        number_of_next_steps
    FROM conversation_summary_view
    """
    query = add_date_and_search_where_clause(query)
    query += " ORDER BY datetime DESC"
    return get_data(query, _session)


def get_sales_reps(_session):
    query = f"""
        SELECT sr.name as rep_name, 
            cs.name as CRITERIA_NAME, 
            AVG(cs.score) as call_score,
            ARRAY_AGG(DISTINCT REPLACE(REPLACE(tr.tracker_name, 'Symbl.', ''), '_', ' ')) as trackers
        FROM SalesRep sr
            JOIN CommunicationHistory ch ON sr.sales_rep_id = ch.sales_rep_id
            JOIN Conversation c ON ch.communication_id = c.communication_id
            JOIN CallScoreCriteria cs ON c.conversation_id = cs.conversation_id
            JOIN Trackers tr ON c.conversation_id = tr.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY sr.name, cs.name"
    df = get_data(query, _session)
    df["CALL_SCORE"] = df["CALL_SCORE"].astype(float)
    trackers_df = df.groupby("REP_NAME")["TRACKERS"].first().reset_index()
    df_pivot = df.pivot(index="REP_NAME", columns="CRITERIA_NAME", values="CALL_SCORE").reset_index()
    df_pivot["AVG_CALL_SCORE"] = df_pivot.drop(columns=["REP_NAME"]).mean(axis=1)
    df_final = pd.merge(df_pivot, trackers_df, on="REP_NAME", how="left")
    return df_final


def get_sales_rep_call_scores(_session):
    query = f"""
        SELECT sr.name as rep_name, 
            cs.name as CRITERIA_NAME, 
            AVG(cs.score) as call_score,
        FROM SalesRep sr
            JOIN CommunicationHistory ch ON sr.sales_rep_id = ch.sales_rep_id
            JOIN Conversation c ON ch.communication_id = c.communication_id
            JOIN CallScoreCriteria cs ON c.conversation_id = cs.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY sr.name, cs.name"
    df = get_data(query, _session)
    df["CALL_SCORE"] = df["CALL_SCORE"].astype(float)
    return df


def get_trackers_by_rep(_session):
    query = f"""
        SELECT sr.name as sales_rep, REPLACE(REPLACE(tr.tracker_name, 'Symbl.', ''), '_', ' ') as tracker_name, COUNT(tr.tracker_id) as tracker_count
        FROM Trackers tr
        JOIN Conversation c ON tr.conversation_id = c.conversation_id
        JOIN CommunicationHistory ch ON c.communication_id = ch.communication_id
        JOIN SalesRep sr ON ch.sales_rep_id = sr.sales_rep_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY sr.name, tracker_name"
    return get_data(query, _session)


def get_trackers_by_stage(_session):
    query = f"""
        SELECT o.stage, REPLACE(REPLACE(tr.tracker_name, 'Symbl.', ''), '_', ' ') as tracker_name, COUNT(tr.tracker_id) as tracker_count
        FROM Trackers tr
        JOIN Conversation c ON tr.conversation_id = c.conversation_id
        JOIN CommunicationHistory ch ON c.communication_id = ch.communication_id
        JOIN Opportunity o ON ch.opportunity_id = o.opportunity_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY o.stage, tracker_name"
    return get_data(query, _session)


def get_call_score_criteria_scores(_session):
    query = f"""
        SELECT csc.name as criteria_name, AVG(csc.score) as avg_score
        FROM CallScoreCriteria csc
                 JOIN Conversation c ON csc.conversation_id = c.conversation_id
                 JOIN CommunicationHistory ch ON c.communication_id = ch.communication_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY csc.name"
    df = get_data(query, _session)
    df['AVG_SCORE'] = df['AVG_SCORE'].astype(float)
    return df


def get_account_criteria_scores(_session):
    query = f"""
    SELECT a.account_name AS ACCOUNT_NAME,
       csc.name       AS criteria_name,
       AVG(csc.score) AS avg_score
    FROM Account a
             JOIN Opportunity o ON a.account_id = o.account_id
             JOIN CommunicationHistory ch ON o.opportunity_id = ch.opportunity_id
             JOIN Conversation c ON ch.communication_id = c.communication_id
             JOIN CallScoreCriteria csc ON c.conversation_id = csc.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY a.account_name, csc.name"
    df = get_data(query, _session)
    df_pivot = df.pivot(index="ACCOUNT_NAME", columns="CRITERIA_NAME", values="AVG_SCORE").reset_index()
    return df_pivot


def get_trackers_by_call_score(_session):
    query = f"""
    SELECT 
        REPLACE(REPLACE(tr.tracker_name, 'Symbl.', ''), '_', ' ') AS TRACKER_NAME,
        AVG(csc.score) AS AVG_CALL_SCORE,
        COUNT(tr.tracker_id) AS TRACKER_COUNT,
        AVG(o.opportunity_amount) AS AVG_OPPORTUNITY_AMOUNT
    FROM Trackers tr
        JOIN CallScoreCriteria csc ON tr.conversation_id = csc.conversation_id
        JOIN Conversation c ON tr.conversation_id = c.conversation_id
        JOIN CommunicationHistory ch ON c.communication_id = ch.communication_id
        JOIN Opportunity o ON ch.opportunity_id = o.opportunity_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY tr.tracker_name"
    return get_data(query, _session)


def get_opportunity_details(_session):
    query = f"""
    SELECT
        a.account_name AS ACCOUNT_NAME,
        COUNT(DISTINCT c.conversation_id) AS num_meetings,
        o.next_step AS NEXT_STEP,
        o.stage AS STAGE,
        o.opportunity_amount AS OPPORTUNITY_AMOUNT,
        o.probability AS PROBABILITY,
        o.target_close_date AS TARGET_CLOSE_DATE,
        o.time_since_open AS TIME_SINCE_OPEN,
        sr.name AS SALES_REP,
        AVG(csc.score) AS avg_call_score,
        
    FROM Opportunity o
             JOIN Account a ON a.account_id = o.account_id
             JOIN SalesRep sr ON sr.sales_rep_id = o.sales_rep_id
             JOIN CommunicationHistory ch ON o.opportunity_id = ch.opportunity_id
             JOIN Conversation c ON ch.communication_id = c.communication_id
             JOIN CallScoreCriteria csc ON c.conversation_id = csc.conversation_id
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    query += " GROUP BY a.account_name, o.next_step, o.stage, o.opportunity_amount, o.probability, o.target_close_date, o.time_since_open, sr.name"
    return get_data(query, _session)


def get_conversation_data(conversation_id, _session):
    query = f"""
    SELECT 
        c.name AS name,
        c.datetime AS datetime,
        c.executive_summary AS executive_summary,
        c.short_bullet_points AS short_bullet_points,
        o.stage AS stage,
        a.account_name AS account_name,
    FROM Conversation AS c
        JOIN CommunicationHistory AS ch ON c.communication_id = ch.communication_id
        JOIN Opportunity AS o ON ch.opportunity_id = o.opportunity_id
        JOIN Account AS a ON o.account_id = a.account_id
    WHERE c.conversation_id = '{conversation_id}'
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    return get_data(query, _session)


def get_conversation_objections(conversation_id, _session):
    query = f"""
    SELECT 
        objection,
        objection_response
    FROM Objections
    WHERE conversation_id = '{conversation_id}'
    """
    query = add_date_and_search_where_clause(query, prefix="c.")
    return get_data(query, _session)
