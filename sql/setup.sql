-- Create a new database
CREATE DATABASE IF NOT EXISTS conversation_db;

-- Switch to the newly created database
USE DATABASE conversation_db;

-- Create schema for storing conversation analysis data
CREATE SCHEMA IF NOT EXISTS conversation_analysis;
-- Create Conversation Table
CREATE TABLE IF NOT EXISTS conversation_analysis.Conversation
(
    conversation_id     STRING PRIMARY KEY,
    name                STRING,
    url                 STRING,
    long_summary        STRING,
    call_score          INT,
    call_score_summary  STRING,
    total_silence_sec   FLOAT,
    total_talk_time_sec FLOAT,
    total_overlap_sec   FLOAT,
    experience_url      STRING
);

-- Create Members Table
CREATE TABLE IF NOT EXISTS conversation_analysis.Members
(
    member_id            STRING PRIMARY KEY,
    name                 STRING,
    user_id              STRING,
    pace_wpm             INT,
    talk_time_sec        FLOAT,
    listen_time_sec      FLOAT,
    overlap_duration_sec FLOAT
);

-- Create Transcript Table
CREATE TABLE IF NOT EXISTS conversation_analysis.Transcript
(
    transcript_id      STRING PRIMARY KEY,
    conversation_id    STRING,
    member_id          STRING, -- Reference to the Members table instead of speaker_name
    start_time_offset  FLOAT,  -- Start time in seconds relative to the start of the conversation
    end_time_offset    FLOAT,  -- End time in seconds relative to the start of the conversation
    content            TEXT,   -- Transcript content (text of the conversation)
    sentiment_polarity FLOAT,  -- Sentiment polarity of the transcript content
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id),
    FOREIGN KEY (member_id) REFERENCES conversation_analysis.Members (member_id)
);


-- Create Sentiment Table in the schema
CREATE TABLE IF NOT EXISTS conversation_analysis.Sentiment
(
    sentiment_id      STRING PRIMARY KEY,
    conversation_id   STRING,
    start_time        STRING, -- Time when the sentiment starts (stored as STRING)
    end_time          STRING, -- Time when the sentiment ends (stored as STRING)
    polarity_score    FLOAT,  -- Sentiment polarity score
    sentiment_label   STRING, -- Positive, neutral, negative, etc.
    sentiment_summary STRING, -- Summary of the sentiment
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

-- Create Sentiment Table in the schema
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

-- Create Questions Table
CREATE TABLE IF NOT EXISTS conversation_analysis.Questions
(
    question_id STRING PRIMARY KEY,
    question    STRING,
    answer      STRING,
    question_by STRING
);

-- Create NextSteps Table
CREATE TABLE IF NOT EXISTS conversation_analysis.NextSteps
(
    step_id         STRING PRIMARY KEY,
    conversation_id STRING,
    next_step       STRING,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

-- Create CallScoreCriteria Table
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

-- Create Topics Table
CREATE TABLE IF NOT EXISTS conversation_analysis.Topics
(
    topic_id        STRING PRIMARY KEY,
    conversation_id STRING,
    text            STRING,
    score           FLOAT,
    FOREIGN KEY (conversation_id) REFERENCES conversation_analysis.Conversation (conversation_id)
);

-- Create or replace a view that aggregates all required information
CREATE OR REPLACE VIEW conversation_analysis.conversation_summary_view AS
WITH RankedTopics AS (SELECT t.conversation_id,
                             t.text                                                                   AS topic,
                             ROW_NUMBER() OVER (PARTITION BY t.conversation_id ORDER BY t.score DESC) AS topic_rank
                      FROM conversation_analysis.Topics t)
SELECT c.conversation_id,
       c.total_talk_time_sec                                            as total_talk_time,
       c.total_silence_sec                                              as total_silence_time,
       c.experience_url                                                 as symbl_insights_url,
       (total_talk_time / (total_silence_time + total_talk_time)) * 100 AS talk_time_percent,
       (SELECT ARRAY_AGG(r.topic)
        FROM RankedTopics r
        WHERE r.conversation_id = c.conversation_id
          AND r.topic_rank <= 3)                                        AS top_3_topics,
       (SELECT AVG(s.polarity_score)
        FROM conversation_analysis.Sentiment s
        WHERE s.conversation_id = c.conversation_id)                    AS overall_sentiment,
       (SELECT ARRAY_AGG(s.polarity_score)
        FROM conversation_analysis.Sentiment s
        WHERE s.conversation_id = c.conversation_id)                    AS sentiment_arr,
       (SELECT COUNT(ns.step_id)
        FROM conversation_analysis.NextSteps ns
        WHERE ns.conversation_id = c.conversation_id)                   AS number_of_next_steps
FROM conversation_analysis.Conversation c;
