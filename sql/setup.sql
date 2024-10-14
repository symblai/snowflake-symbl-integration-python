-- Create a new database for storing data
CREATE DATABASE IF NOT EXISTS conversation_db;

USE DATABASE conversation_db;

-- Create schema for storing conversation analysis data
CREATE SCHEMA IF NOT EXISTS conversation_analysis;

CREATE TABLE IF NOT EXISTS conversation_analysis.Account
(
    account_id   STRING PRIMARY KEY,
    account_name STRING
);

CREATE TABLE IF NOT EXISTS conversation_analysis.SalesRep
(
    sales_rep_id STRING PRIMARY KEY,
    name         STRING,
    email        STRING,
    phone        STRING
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Contact
(
    contact_id STRING PRIMARY KEY,
    name       STRING,
    email      STRING,
    phone      STRING
);

CREATE TABLE IF NOT EXISTS conversation_analysis.LeadSource
(
    lead_source_id   STRING PRIMARY KEY,
    lead_source_name STRING
);

CREATE TABLE IF NOT EXISTS conversation_analysis.StageTransition
(
    stage_transition_id STRING PRIMARY KEY,
    opportunity_id      STRING,
    stage               STRING,
    transition_date     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conversation_analysis.CommunicationHistory
(
    communication_id   STRING PRIMARY KEY,
    opportunity_id     STRING,
    channel            STRING,
    communication_date TIMESTAMP,
    content            STRING,
    sales_rep_id       STRING,
    contact_id         STRING,
    datetime           TIMESTAMP,
    FOREIGN KEY (sales_rep_id) REFERENCES conversation_analysis.SalesRep (sales_rep_id),
    FOREIGN KEY (contact_id) REFERENCES conversation_analysis.Contact (contact_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Opportunity
(
    opportunity_id     STRING PRIMARY KEY,
    account_id         STRING,
    sales_rep_id       STRING,
    contact_id         STRING,
    lead_source_id     STRING,
    opportunity_amount FLOAT,
    probability        FLOAT,
    status             STRING,
    stage              STRING,
    time_since_open    INT,
    target_close_date  TIMESTAMP,
    next_step          STRING,
    FOREIGN KEY (account_id) REFERENCES conversation_analysis.Account (account_id),
    FOREIGN KEY (sales_rep_id) REFERENCES conversation_analysis.SalesRep (sales_rep_id),
    FOREIGN KEY (contact_id) REFERENCES conversation_analysis.Contact (contact_id),
    FOREIGN KEY (lead_source_id) REFERENCES conversation_analysis.LeadSource (lead_source_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Conversation
(
    conversation_id     STRING PRIMARY KEY,
    communication_id    STRING,
    name                STRING,
    url                 STRING,
    datetime            TIMESTAMP,
    long_summary        STRING,
    call_score          INT,
    executive_summary   STRING,
    bullet_points       STRING,
    short_summary       STRING,
    short_bullet_points STRING,
    call_score_summary  STRING,
    total_silence_sec   FLOAT,
    total_talk_time_sec FLOAT,
    total_overlap_sec   FLOAT,
    experience_url      STRING,
    FOREIGN KEY (communication_id) REFERENCES conversation_analysis.CommunicationHistory (communication_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Members
(
    member_id            STRING PRIMARY KEY,
    conversation_id      STRING,
    name                 STRING,
    user_id              STRING,
    pace_wpm             INT,
    talk_time_sec        FLOAT,
    listen_time_sec      FLOAT,
    overlap_duration_sec FLOAT,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Transcript
(
    transcript_id      STRING PRIMARY KEY,
    conversation_id    STRING,
    member_id          STRING,
    start_time_offset  FLOAT,
    end_time_offset    FLOAT,
    content            TEXT,
    sentiment_polarity FLOAT,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id),
    FOREIGN KEY (member_id) REFERENCES conversation_analysis.Members (member_id)
);


CREATE TABLE IF NOT EXISTS conversation_analysis.Sentiment
(
    sentiment_id      STRING PRIMARY KEY,
    conversation_id   STRING,
    start_time        STRING,
    end_time          STRING,
    polarity_score    FLOAT,
    sentiment_label   STRING,
    sentiment_summary STRING,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Entities
(
    entity_id       STRING PRIMARY KEY,
    conversation_id STRING,
    text            STRING,
    category        STRING,
    entity_type     STRING,
    entity_subtype  STRING,
    entity_value    STRING,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Trackers
(
    tracker_id      STRING PRIMARY KEY,
    conversation_id STRING,
    tracker_name    STRING,
    text            STRING,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);


CREATE TABLE IF NOT EXISTS conversation_analysis.Questions
(
    question_id     STRING PRIMARY KEY,
    conversation_id STRING,
    question        STRING,
    answer          STRING,
    question_by     STRING,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.NextSteps
(
    step_id         STRING PRIMARY KEY,
    conversation_id STRING,
    next_step       STRING,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.CallScoreCriteria
(
    criteria_id       STRING PRIMARY KEY,
    conversation_id   STRING,
    name              STRING,
    score             INT,
    summary           STRING,
    positive_feedback STRING,
    negative_feedback STRING,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Topics
(
    topic_id        STRING PRIMARY KEY,
    conversation_id STRING,
    text            STRING,
    score           FLOAT,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

CREATE TABLE IF NOT EXISTS conversation_analysis.Objections
(
    objection_id       STRING PRIMARY KEY,
    conversation_id    STRING,
    objection          STRING,
    objection_response STRING,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

-- Create or replace a view that aggregates all required information
CREATE OR REPLACE VIEW conversation_analysis.conversation_summary_view AS
WITH RankedTopics AS (SELECT t.conversation_id,
                             t.text                                                                   AS topic,
                             ROW_NUMBER() OVER (PARTITION BY t.conversation_id ORDER BY t.score DESC) AS topic_rank
                      FROM conversation_analysis.Topics t)
SELECT DISTINCT c.conversation_id,
                c.name                                                           AS conversation_name,
                c.total_talk_time_sec                                            AS total_talk_time,
                c.total_silence_sec                                              AS total_silence_time,
                c.experience_url                                                 AS symbl_insights_url,
                c.call_score                                                     AS call_score,
                c.datetime                                                       AS datetime,
                sr.name                                                          AS sales_rep,
                ac.account_name                                                  AS account_name,
                o.stage                                                          AS deal_stage,
                (total_talk_time / (total_silence_time + total_talk_time)) * 100 AS talk_time_percent,
                (SELECT ARRAY_AGG(r.topic)
                 FROM RankedTopics r
                 WHERE r.conversation_id = c.conversation_id
                   AND r.topic_rank <= 3)                                        AS top_3_topics,
--         (SELECT sr.name as sales_rep
--          FROM conversation_analysis.SalesRep sr
--                  JOIN conversation_analysis.Opportunity o ON sr.sales_rep_id = o.sales_rep_id
--                  JOIN conversation_analysis.CommunicationHistory ch ON o.opportunity_id = ch.opportunity_id
--         WHERE ch.communication_id = c.communication_id)                   AS sales_rep,
--          (SELECT ac.account_name as account_name
--             FROM conversation_analysis.Account ac
--                     JOIN conversation_analysis.Opportunity o ON ac.account_id = o.account_id
--                     JOIN conversation_analysis.CommunicationHistory ch ON o.opportunity_id = ch.opportunity_id
--             WHERE ch.communication_id = c.communication_id)               AS account_name,
--          (SELECT o.stage as deal_stage
--             FROM conversation_analysis.Opportunity o
--                     JOIN conversation_analysis.CommunicationHistory ch ON o.opportunity_id = ch.opportunity_id
--             WHERE ch.communication_id = c.communication_id)               AS deal_stage,
                (SELECT AVG(s.polarity_score)
                 FROM conversation_analysis.Sentiment s
                 WHERE s.conversation_id = c.conversation_id)                    AS overall_sentiment,
                (SELECT ARRAY_AGG(tr.sentiment_polarity) WITHIN GROUP (ORDER BY tr.start_time_offset)
                 FROM conversation_analysis.Transcript tr
                 WHERE tr.conversation_id = c.conversation_id)                   AS sentiment_arr,


--                 (SELECT ARRAY_AGG(s.polarity_score)
--                  FROM conversation_analysis.Sentiment s
--                  WHERE s.conversation_id = c.conversation_id)                    AS sentiment_arr,
                (SELECT COUNT(ns.step_id)
                 FROM conversation_analysis.NextSteps ns
                 WHERE ns.conversation_id = c.conversation_id)                   AS number_of_next_steps,
                (SELECT COUNT(o.objection_id)
                 FROM conversation_analysis.Objections o
                 WHERE o.conversation_id = c.conversation_id)                    AS number_of_objections
FROM conversation_analysis.Conversation c
         LEFT JOIN conversation_analysis.CommunicationHistory ch
                   ON ch.communication_id = c.communication_id
         RIGHT JOIN conversation_analysis.Opportunity o
                    ON o.opportunity_id = ch.opportunity_id
         RIGHT JOIN conversation_analysis.SalesRep sr
                    ON sr.sales_rep_id = o.sales_rep_id
         RIGHT JOIN conversation_analysis.Account ac
                    ON ac.account_id = o.account_id
ORDER BY c.datetime DESC;


CREATE OR REPLACE FUNCTION CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_chunk(
    transcript string, name string, datetime datetime, rep_id string,
    rep_name string, account_name string, deal_stage string, conversation_id string
)
    returns table
            (
                chunk           string,
                name            string,
                datetime        timestamp,
                rep_id          string,
                rep_name        string,
                account_name    string,
                deal_stage      string,
                conversation_id string
            )
    language python
    runtime_version =
'3.9'
    handler =
'text_chunker'
    packages = (
'snowflake-snowpark-python',
'langchain')
as
$$
from langchain.text_splitter import RecursiveCharacterTextSplitter

class text_chunker:

    def process(self, transcript: str, name: str, datetime: str, rep_id: str, rep_name: str, account_name: str,
                deal_stage: str, conversation_id: str):
        if transcript == None:
            transcript = ""

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=2000,
            chunk_overlap=300,
            length_function=len
        )
        chunks = text_splitter.split_text(transcript)
        for chunk in chunks:
            yield (name + "\n" + rep_name + "\n" + account_name + "\n" + deal_stage + chunk,
                   name,
                   datetime,
                   rep_id,
                   rep_name,
                   account_name,
                   deal_stage,
                   conversation_id)
$$;


CREATE OR REPLACE TABLE CONVERSATION_DB.CONVERSATION_ANALYSIS.CONVERSATION_TRANSCRIPT_CHUNKS AS (SELECT tr.transcript      AS transcript,
                                                                                                           c.name             AS name,
                                                                                                           c.datetime         AS datetime,
                                                                                                           s.sales_rep_id     AS rep_id,
                                                                                                           s.name             AS rep_name,
                                                                                                           ac.account_name    AS account_name,
                                                                                                           o.stage            AS deal_stage,
                                                                                                           tr.conversation_id AS conversation_id,
                                                                                                           t.CHUNK            AS CHUNK
                                                                                                    FROM (SELECT ARRAY_TO_STRING(
                                                                                                                         ARRAY_AGG(messages) WITHIN GROUP ( ORDER BY start_time_offset ),
                                                                                                                         '\n') AS transcript,
                                                                                                                 conversation_id
                                                                                                          FROM (SELECT CONCAT(CONCAT(m.name, ': '), tr.content) as messages,
                                                                                                                       tr.start_time_offset                     as start_time_offset,
                                                                                                                       tr.conversation_id                       AS conversation_id
                                                                                                                FROM CONVERSATION_DB.CONVERSATION_ANALYSIS.Transcript tr
                                                                                                                         JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.Members m
                                                                                                                              ON tr.MEMBER_ID = m.MEMBER_ID
                                                                                                                GROUP BY tr.conversation_id,
                                                                                                                         tr.content,
                                                                                                                         m.name,
                                                                                                                         tr.start_time_offset
                                                                                                                ORDER BY tr.conversation_id, tr.start_time_offset)
                                                                                                          GROUP BY conversation_id) as tr
                                                                                                             JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.Conversation c
                                                                                                                  ON tr.conversation_id = c.conversation_id
                                                                                                             JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.CommunicationHistory ch
                                                                                                                  ON c.communication_id = ch.communication_id
                                                                                                             JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.Opportunity o
                                                                                                                  ON ch.opportunity_id = o.opportunity_id
                                                                                                             JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.SalesRep s
                                                                                                                  ON o.sales_rep_id = s.sales_rep_id
                                                                                                             JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.Account ac
                                                                                                                  ON o.account_id = ac.account_id,
                                                                                                         TABLE (CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_chunk(
                                                                                                                        tr.transcript,
                                                                                                                        c.name,
                                                                                                                        c.datetime,
                                                                                                                        s.sales_rep_id,
                                                                                                                        s.name,
                                                                                                                        ac.account_name,
                                                                                                                        o.stage,
                                                                                                                        tr.conversation_id)) as t);

CREATE OR REPLACE FUNCTION CONVERSATION_DB.CONVERSATION_ANALYSIS.call_score_chunk(
    summary string, positive_feedback string, negative_feedback string, criteria_name string, score float,
    rep_id string, rep_name string, account_name string, deal_stage string, conversation_id string, criteria_id string
)
    returns table
            (
                chunk           string,
                criteria_name   string,
                score           float,
                rep_id          string,
                rep_name        string,
                account_name    string,
                deal_stage      string,
                conversation_id string,
                criteria_id     string
            )
    language python
    runtime_version =
'3.9'
    handler =
'text_chunker'
    packages = (
'snowflake-snowpark-python',
'langchain')
as
$$
from langchain.text_splitter import RecursiveCharacterTextSplitter
import copy
from typing import Optional


class text_chunker:

    def process(self, summary: str, positive_feedback: str, negative_feedback: str, criteria_name: str, score: float,
                rep_id: str, rep_name: str, account_name: str, deal_stage: str, conversation_id: str, criteria_id: str):
        if not summary:
            summary = ""

        if not positive_feedback:
            positive_feedback = ""

        if not negative_feedback:
            negative_feedback = ""

        content = f"Summary: {summary}\nPositive: {positive_feedback}\nNegative: {negative_feedback}"

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=2000,
            chunk_overlap=300,
            length_function=len
        )
        chunks = text_splitter.split_text(content)
        for chunk in chunks:
            yield (criteria_name + "\n" + "Score: " + str(
                score) + "\n" + rep_name + "\n" + account_name + "\n" + deal_stage + "\n" + chunk,
                   criteria_name,
                   score,
                   rep_id,
                   rep_name,
                   account_name,
                   deal_stage,
                   conversation_id,
                   criteria_id)
$$;

CREATE OR REPLACE TABLE CONVERSATION_DB.CONVERSATION_ANALYSIS.CALL_SCORE_CHUNKS AS (SELECT csc.name              AS criteria_name,
                                                                                              csc.score             AS score,
                                                                                              csc.criteria_id       AS criteria_id,
                                                                                              s.sales_rep_id        AS rep_id,
                                                                                              s.name                AS rep_name,
                                                                                              ac.account_name       AS account_name,
                                                                                              o.stage               AS deal_stage,
                                                                                              c.conversation_id     AS conversation_id,
                                                                                              csc.summary           AS summary,
                                                                                              csc.positive_feedback AS positive_feedback,
                                                                                              csc.negative_feedback AS negative_feedback,
                                                                                              t.CHUNK               AS CHUNK
                                                                                       FROM CONVERSATION_DB.CONVERSATION_ANALYSIS.CallScoreCriteria csc
                                                                                                JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.Conversation c
                                                                                                     ON csc.conversation_id = c.conversation_id
                                                                                                JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.CommunicationHistory ch
                                                                                                     ON c.communication_id = ch.communication_id
                                                                                                JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.Opportunity o
                                                                                                     ON ch.opportunity_id = o.opportunity_id
                                                                                                JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.SalesRep s
                                                                                                     ON o.sales_rep_id = s.sales_rep_id
                                                                                                JOIN CONVERSATION_DB.CONVERSATION_ANALYSIS.Account ac
                                                                                                     ON o.account_id = ac.account_id,
                                                                                            TABLE (CONVERSATION_DB.CONVERSATION_ANALYSIS.call_score_chunk(
                                                                                                           csc.summary,
                                                                                                           csc.positive_feedback,
                                                                                                           csc.negative_feedback,
                                                                                                           csc.name,
                                                                                                           CAST(csc.score AS FLOAT),
                                                                                                           s.sales_rep_id,
                                                                                                           s.name,
                                                                                                           ac.account_name,
                                                                                                           o.stage,
                                                                                                           c.conversation_id,
                                                                                                           csc.criteria_id)) as t);

CREATE CORTEX SEARCH SERVICE IF NOT EXISTS CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_search_service
    ON CHUNK
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 minute'
    AS (
        SELECT *
        FROM CONVERSATION_DB.CONVERSATION_ANALYSIS.CONVERSATION_TRANSCRIPT_CHUNKS
    );

CREATE CORTEX SEARCH SERVICE IF NOT EXISTS CONVERSATION_DB.CONVERSATION_ANALYSIS.call_score_search_service
    ON CHUNK
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 minute'
    AS (
        SELECT *
        FROM CONVERSATION_DB.CONVERSATION_ANALYSIS.CALL_SCORE_CHUNKS
    );

CREATE OR REPLACE NETWORK RULE symbl_apis_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.symbl.ai', 'api-nebula.symbl.ai');
