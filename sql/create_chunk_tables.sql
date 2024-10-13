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
                                                                                                                      ARRAY_AGG(messages)
                                                                                                                      WITHIN
                                                                                                                      GROUP
                                                                                                                      (ORDER BY start_time_offset),
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
                                                                                                      TABLE(CONVERSATION_DB.CONVERSATION_ANALYSIS.conversation_chunk(
                                                                                                              tr.transcript,
                                                                                                              c.name,
                                                                                                              c.datetime,
                                                                                                              s.sales_rep_id,
                                                                                                              s.name,
                                                                                                              ac.account_name,
                                                                                                              o.stage,
                                                                                                              tr.conversation_id)) as t);

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
                                                                                         TABLE(CONVERSATION_DB.CONVERSATION_ANALYSIS.call_score_chunk(
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
