from time import sleep
from typing import Dict

import requests
import snowflake.connector
from dotenv import dotenv_values

config = dotenv_values(".env")
app_id = config.get("SYMBL_APP_ID")
app_secret = config.get("SYMBL_APP_SECRET")

snowflake_credentials = {
    "account": config.get("SNOWFLAKE_ACCOUNT"),
    "user": config.get("SNOWFLAKE_USER"),
    "password": config.get("SNOWFLAKE_PASSWORD"),
    "database": config.get("SNOWFLAKE_DATABASE"),
    "schema": config.get("SNOWFLAKE_SCHEMA"),
    "warehouse": config.get("SNOWFLAKE_WAREHOUSE")
}


def symbl_token(app_id, app_secret):
    payload = {
        "type": "application",
        "appId": app_id,
        "appSecret": app_secret
    }
    response = requests.post("https://api.symbl.ai/oauth2/token:generate", json=payload)
    return response.json()["accessToken"]


def check_job_status(job_id, token):
    response = requests.get(f"https://api.symbl.ai/v1/job/{job_id}",
                            headers={"Authorization": f"Bearer {token}"})
    return response.json()["status"]


def check_call_score_status(conversation_id, token):
    response = requests.get(f"https://api.symbl.ai/v1/conversations/{conversation_id}/callscore/status",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    return response.json()["status"]


def check_insights_status(conversation_id, token):
    response = requests.get(f"https://api.symbl.ai/v1/conversations/{conversation_id}/lm-insights/status",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    return response.json()["status"]


def process_audio_file(audio_url, speaker_channels, score_card_id, token):
    payload = {
        "url": audio_url,
        "enableSeparateRecognitionPerChannel": True,
        "channelMetadata": speaker_channels,
        "detectEntities": True,
        "features": {
            "featureList": ["callScore", "insights"],
            "callScore": {"scorecardId": score_card_id}
        },
        "conversationType": "general"
    }
    response = requests.post(url="https://api.symbl.ai/v1/process/audio/url",
                             json=payload,
                             headers={"accept": "application/json",
                                      "Authorization": f"Bearer {token}"})
    if response.status_code >= 400:
        print(f"Error processing audio file: {response.json()}")
        return None, None, "failed"

    response = response.json()
    conversation_id, job_id = response["conversationId"], response["jobId"]

    while True:
        sleep(5)
        status = check_job_status(job_id, token)
        if status == "completed":
            status = check_insights_status(conversation_id, token)
            if status == "completed":
                status = check_call_score_status(conversation_id, token)
                if status == "completed":
                    break
    return conversation_id, job_id, status


def get_transcript(conversation_id, token):
    response = requests.get(url=f"https://api.symbl.ai/v1/conversations/{conversation_id}/messages?sentiment=true",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    messages = response.json()['messages']
    return messages


def get_conversation_analysis(conversation_id, token):
    response = requests.get(url=f"https://api.symbl.ai/v1/conversations/{conversation_id}/lm-insights",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    insights = response.json()
    insights = insights[0]

    response = requests.get(url=f"https://api.symbl.ai/v1/conversations/{conversation_id}/topics",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    topics = response.json()

    response = requests.get(url=f"https://api.symbl.ai/v1/conversations/{conversation_id}/entities",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    entities = response.json()

    response = requests.get(url=f"https://api.symbl.ai/v1/conversations/{conversation_id}/analytics",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    analytics = response.json()

    members = insights["attendees"]
    for member in members:
        for member_analytics in analytics["members"]:
            if member["id"] == member_analytics["id"]:
                member["pace_wpm"] = member_analytics["pace"]["wpm"]
                member["talk_time_sec"] = member_analytics["talkTime"]["seconds"]
                member["talk_time_percentage"] = member_analytics["talkTime"]["percentage"]
                member["listen_time_sec"] = member_analytics["listenTime"]["seconds"]
                member["listen_time_percentage"] = member_analytics["listenTime"]["percentage"]
                member["overlap_duration_sec"] = member_analytics["overlap"]["overlapDuration"]
                break
    entities = entities["entities"]
    _entities = []
    for entity in entities:
        entity_type = entity["type"]
        entity_subtype = entity.get("subType", "")
        category = entity.get("category", "")

        # Loop through all matches
        for match in entity["matches"]:
            entity_value = match["detectedValue"]
            for message_ref in match["messageRefs"]:
                text = message_ref["text"]
                _entities.append({
                    "text": text,
                    "entity_category": category,
                    "entity_type": entity_type,
                    "entity_subtype": entity_subtype,
                    "entity_value": entity_value
                })
    entities = _entities
    analysis = {
        "long_summary": insights["callSummary"]["longSummary"]["paragraphs"],
        "next_steps": [item["step"] for item in insights["callNextSteps"]["nextSteps"]],
        "question_answers": [item for item in insights["callQuestionAndAnswers"]["questionsAndAnswers"]],
        "sentiment": [chunk for chunk in insights["callSentiment"]["chunks"]],
        "name": insights["meetingName"],
        "url": insights["videoUrl"],
        "call_score": insights["callScore"]["score"],
        "call_score_summary": insights["callScore"]["summary"],
        "call_score_criteria": [criteria for criteria in insights["callScore"]["criteria"]],
        "topics": topics["topics"],
        "entities": entities,
        "talk_time_metrics": analytics["metrics"],
        "members": members
    }

    return analysis


def get_experience_url(conversation_id, token):
    response = requests.get(
        f"https://api.symbl.ai/v1/conversations/experiences/insights/details/{conversation_id}?includeCallScore=true",
        headers={"Authorization": f"Bearer {token}"})
    return response.json()["url"]


def get_urls():
    response = requests.get(url="https://sample-meeting-audio.s3.amazonaws.com/index.json")
    return response.json()


def create_score_card_if_not_exists(name="Default Score Card", token=None):
    # Check if score card exists
    response = requests.get(url="https://api.symbl.ai/v1/manage/callscore/scorecards",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    score_cards = response.json()
    score_card_id = None
    for score_card in score_cards:
        if score_card["name"] == name:
            score_card_id = score_card["id"]
            break
    if score_card_id is None:
        # Create score card
        response = requests.post(url="https://api.symbl.ai/v1/manage/callscore/scorecards",
                                 headers={"accept": "application/json",
                                          "Authorization": f"Bearer {token}"},
                                 json={
                                     "name": name,
                                     "tags": ["default"],
                                     "description": "Default Score Card for scoring calls on communication, "
                                                    "engagement and question handling.",
                                     "criteriaList": ["Symbl.Communication_And_Engagement",
                                                      "Symbl.Question_Handling"]
                                 })
        score_card_id = response.json()["id"]
    return score_card_id


# def insert_conversation_data(conversation_id, json_data: Dict, snowflake_conn):
#     """
#     Inserts data from the Symbl Outputs into Snowflake schema.
#     """
#     # Extract the main conversation fields
#     name = json_data.get("name", conversation_id)
#     url = json_data.get("url", "")
#     long_summary = json_data.get("long_summary", "")
#     call_score = json_data.get("call_score", 0)
#     call_score_summary = json_data.get("call_score_summary", "")
#     total_silence_sec = json_data["talk_time_metrics"][0]["seconds"]
#     total_talk_time_sec = json_data["talk_time_metrics"][1]["seconds"]
#     total_overlap_sec = json_data["talk_time_metrics"][2]["seconds"]
#     transcript = json_data.get("transcript", [])
#     experience_url = json_data.get("experience_url", "")
#
#     # Insert into Conversation table
#     insert_conversation_query = f"""
#         INSERT INTO conversation_analysis.Conversation
#         (conversation_id, name, url, long_summary, call_score, call_score_summary,
#         total_silence_sec, total_talk_time_sec, total_overlap_sec, experience_url)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """
#     conversation_values = (conversation_id, name, url, long_summary, call_score, call_score_summary,
#                            total_silence_sec, total_talk_time_sec, total_overlap_sec, experience_url)
#
#     cursor = snowflake_conn.cursor()
#     cursor.execute(insert_conversation_query, conversation_values)
#
#     # Insert members data
#     insert_member_query = f"""
#         INSERT INTO conversation_analysis.Members
#         (member_id, name, user_id, pace_wpm, talk_time_sec, listen_time_sec, overlap_duration_sec)
#         VALUES (%s, %s, %s, %s, %s, %s, %s)
#     """
#
#     for member in json_data["members"]:
#         member_values = (member["id"], member["name"], member["userId"], member["pace_wpm"],
#                          member["talk_time_sec"], member["listen_time_sec"], member["overlap_duration_sec"])
#         cursor.execute(insert_member_query, member_values)
#
#     # Insert data into the Transcript table
#     insert_transcript_query = f"""
#     INSERT INTO conversation_analysis.Transcript
#     (transcript_id, conversation_id, member_id, start_time_offset, end_time_offset, content, sentiment_polarity)
#     VALUES (%s, %s, %s, %s, %s, %s, %s)
#     """
#     for message in transcript:
#         transcript_id = message['id']
#         conversation_id = message['conversationId']
#         member_id = message['from']['id']  # Refers to the `member_id` in the Members table
#         start_time_offset = message['timeOffset']  # Relative start time
#         end_time_offset = start_time_offset + message['duration']  # Relative end time
#         content = message['text']
#         sentiment_polarity = message['sentiment']['polarity']['score']
#         cursor.execute(insert_transcript_query, (
#             transcript_id, conversation_id, member_id, start_time_offset, end_time_offset, content, sentiment_polarity))
#
#     # Insert questions data
#     insert_question_query = f"""
#         INSERT INTO conversation_analysis.Questions
#         (question_id, question, answer, question_by)
#         VALUES (%s, %s, %s, %s)
#     """
#
#     for idx, question in enumerate(json_data["question_answers"]):
#         question_id = f"q_{conversation_id}_{idx + 1}"
#         question_values = (question_id, question["question"], question["answer"],
#                            question["questionBy"])
#         cursor.execute(insert_question_query, question_values)
#
#     # Insert next steps data
#     insert_next_steps_query = f"""
#         INSERT INTO conversation_analysis.NextSteps
#         (step_id, conversation_id, next_step)
#         VALUES (%s, %s, %s)
#     """
#
#     for idx, step in enumerate(json_data["next_steps"]):
#         step_id = f"step_{conversation_id}_{idx + 1}"
#         step_values = (step_id, conversation_id, step)
#         cursor.execute(insert_next_steps_query, step_values)
#
#     # Insert call score criteria data
#     insert_call_score_query = f"""
#         INSERT INTO conversation_analysis.CallScoreCriteria
#         (criteria_id, conversation_id, name, score, summary, positive_feedback, negative_feedback)
#         VALUES (%s, %s, %s, %s, %s, %s, %s)
#     """
#
#     for idx, criteria in enumerate(json_data["call_score_criteria"]):
#         criteria_id = f"crit_{conversation_id}_{idx + 1}"
#         criteria_values = (criteria_id, conversation_id, criteria["name"], criteria["score"],
#                            criteria["summary"], criteria["feedback"]["positive"]["summary"],
#                            criteria["feedback"]["negative"]["summary"])
#         cursor.execute(insert_call_score_query, criteria_values)
#
#     # Insert topics data
#     insert_topics_query = f"""
#         INSERT INTO conversation_analysis.Topics
#         (topic_id, conversation_id, text, score)
#         VALUES (%s, %s, %s, %s)
#     """
#
#     for idx, topic in enumerate(json_data["topics"]):
#         topic_id = f"topic_{conversation_id}_{idx + 1}"
#         topic_values = (topic_id, conversation_id, topic["text"], topic["score"])
#         cursor.execute(insert_topics_query, topic_values)
#
#     # Insert sentiment data
#     insert_sentiment_query = f"""
#         INSERT INTO conversation_analysis.Sentiment
#         (sentiment_id, conversation_id, start_time, end_time, polarity_score, sentiment_label, sentiment_summary)
#         VALUES (%s, %s, %s, %s, %s, %s, %s)
#     """
#     for idx, sentiment in enumerate(json_data["sentiment"]):
#         sentiment_id = f"sent_{conversation_id}_{idx + 1}"
#         sentiment_values = (
#             sentiment_id,
#             conversation_id,
#             sentiment["startTime"],
#             sentiment["endTime"],
#             float(sentiment["polarityScore"]),  # Ensure it's a float for polarity score
#             sentiment["sentiment"],
#             sentiment["summary"]
#         )
#         cursor.execute(insert_sentiment_query, sentiment_values)
#
#     # Insert entities data
#     insert_entities_query = f"""
#         INSERT INTO conversation_analysis.Entities
#         (entity_id, conversation_id, text, category, entity_type, entity_value)
#         VALUES (%s, %s, %s, %s, %s, %s)
#     """
#
#     for idx, entity in enumerate(json_data["entities"]):
#         entity_id = f"entity_{conversation_id}_{idx + 1}"
#         entity_values = (entity_id, conversation_id, entity["text"], entity["category"], entity["type"],
#                          entity["value"])
#         cursor.execute(insert_entities_query, entity_values)
#
#     snowflake_conn.commit()
#     cursor.close()
#     print("Data inserted successfully!")

def insert_conversation_data(conversation_id, json_data: Dict, snowflake_conn):
    """
    Inserts data from the Symbl Outputs into Snowflake schema in batch mode.
    """
    cursor = snowflake_conn.cursor()

    # Extract the main conversation fields
    name = json_data.get("name", conversation_id)
    url = json_data.get("url", "")
    long_summary = json_data.get("long_summary", "")
    call_score = json_data.get("call_score", 0)
    call_score_summary = json_data.get("call_score_summary", "")
    total_silence_sec = json_data["talk_time_metrics"][0]["seconds"]
    total_talk_time_sec = json_data["talk_time_metrics"][1]["seconds"]
    total_overlap_sec = json_data["talk_time_metrics"][2]["seconds"]
    transcript = json_data.get("transcript", [])
    experience_url = json_data.get("experience_url", "")

    # Insert into Conversation table
    insert_conversation_query = f"""
        INSERT INTO conversation_analysis.Conversation 
        (conversation_id, name, url, long_summary, call_score, call_score_summary, 
        total_silence_sec, total_talk_time_sec, total_overlap_sec, experience_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conversation_values = [(conversation_id, name, url, long_summary, call_score, call_score_summary,
                            total_silence_sec, total_talk_time_sec, total_overlap_sec, experience_url)]
    cursor.executemany(insert_conversation_query, conversation_values)

    # Insert members data
    insert_member_query = f"""
        INSERT INTO conversation_analysis.Members 
        (member_id, name, user_id, pace_wpm, talk_time_sec, listen_time_sec, overlap_duration_sec)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    members_values = [
        (member["id"], member["name"], member["userId"], member["pace_wpm"],
         member["talk_time_sec"], member["listen_time_sec"], member["overlap_duration_sec"])
        for member in json_data["members"]
    ]
    cursor.executemany(insert_member_query, members_values)

    # Insert data into the Transcript table
    insert_transcript_query = f"""
    INSERT INTO conversation_analysis.Transcript 
    (transcript_id, conversation_id, member_id, start_time_offset, end_time_offset, content, sentiment_polarity)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    transcript_values = [
        (
            message['id'], message['conversationId'], message['from']['id'],  # member_id
            message['timeOffset'], message['timeOffset'] + message['duration'],  # start_time_offset, end_time_offset
            message['text'], message['sentiment']['polarity']['score']
        )
        for message in transcript
    ]
    cursor.executemany(insert_transcript_query, transcript_values)

    # Insert questions data
    insert_question_query = f"""
        INSERT INTO conversation_analysis.Questions 
        (question_id, question, answer, question_by)
        VALUES (%s, %s, %s, %s)
    """
    questions_values = [
        (f"q_{conversation_id}_{idx + 1}", question["question"], question["answer"], question["questionBy"])
        for idx, question in enumerate(json_data["question_answers"])
    ]
    cursor.executemany(insert_question_query, questions_values)

    # Insert next steps data
    insert_next_steps_query = f"""
        INSERT INTO conversation_analysis.NextSteps 
        (step_id, conversation_id, next_step)
        VALUES (%s, %s, %s)
    """
    next_steps_values = [
        (f"step_{conversation_id}_{idx + 1}", conversation_id, step)
        for idx, step in enumerate(json_data["next_steps"])
    ]
    cursor.executemany(insert_next_steps_query, next_steps_values)

    # Insert call score criteria data
    insert_call_score_query = f"""
        INSERT INTO conversation_analysis.CallScoreCriteria 
        (criteria_id, conversation_id, name, score, summary, positive_feedback, negative_feedback)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    call_score_values = [
        (f"crit_{conversation_id}_{idx + 1}", conversation_id, criteria["name"], criteria["score"],
         criteria["summary"], criteria["feedback"]["positive"]["summary"], criteria["feedback"]["negative"]["summary"])
        for idx, criteria in enumerate(json_data["call_score_criteria"])
    ]
    cursor.executemany(insert_call_score_query, call_score_values)

    # Insert topics data
    insert_topics_query = f"""
        INSERT INTO conversation_analysis.Topics 
        (topic_id, conversation_id, text, score)
        VALUES (%s, %s, %s, %s)
    """
    topics_values = [
        (f"topic_{conversation_id}_{idx + 1}", conversation_id, topic["text"], topic["score"])
        for idx, topic in enumerate(json_data["topics"])
    ]
    cursor.executemany(insert_topics_query, topics_values)

    # Insert sentiment data
    insert_sentiment_query = f"""
        INSERT INTO conversation_analysis.Sentiment 
        (sentiment_id, conversation_id, start_time, end_time, polarity_score, sentiment_label, sentiment_summary)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    sentiment_values = [
        (
            f"sent_{conversation_id}_{idx + 1}", conversation_id,
            sentiment["startTime"], sentiment["endTime"], float(sentiment["polarityScore"]),
            sentiment["sentiment"], sentiment["summary"]
        )
        for idx, sentiment in enumerate(json_data["sentiment"])
    ]
    cursor.executemany(insert_sentiment_query, sentiment_values)

    # Insert entities data
    insert_entities_query = f"""
        INSERT INTO conversation_analysis.Entities 
        (entity_id, conversation_id, text, category, entity_type, entity_subtype, entity_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    entities_values = [
        (f"entity_{conversation_id}_{idx + 1}", conversation_id, entity["text"], entity["entity_category"],
         entity["entity_type"], entity['entity_subtype'], entity["entity_value"])
        for idx, entity in enumerate(json_data["entities"])
    ]

    cursor.executemany(insert_entities_query, entities_values)

    # Commit the transaction and close the cursor
    snowflake_conn.commit()
    cursor.close()


def main():
    conn = snowflake.connector.connect(
        user=snowflake_credentials["user"],
        password=snowflake_credentials["password"],
        account=snowflake_credentials["account"],
        warehouse=snowflake_credentials["warehouse"],
        database=snowflake_credentials["database"],
        schema=snowflake_credentials["schema"]
    )

    token = symbl_token(app_id, app_secret)
    score_card_id = create_score_card_if_not_exists(token=token)

    url_data = get_urls()[2:]
    print(f"Total number of audio files: {len(url_data)}")
    ids = [] #["6176586485858304"]
    idx = 0
    for item in url_data:
        if idx < len(ids):
            conversation_id = ids[idx]
            print(f"Processing conversation ID: {conversation_id}")
        else:
            audio_url = item["file_name"]
            speaker_channel_file = item["speaker_channel_file"]
            response = requests.get(url=speaker_channel_file)
            speaker_channels_metadata = response.json()
            print(f"Processing audio file: {audio_url}")
            conversation_id, job_id, status = process_audio_file(audio_url, speaker_channels_metadata, score_card_id,
                                                                 token)
            print(f"Conversation ID: {conversation_id}, Job ID: {job_id}, Status: {status}")
            if status != "completed":
                print(f"Processing of audio file {audio_url} failed.")
                continue

        print(f"Fetching data for conversation ID: {conversation_id}")
        transcript = get_transcript(conversation_id, token)
        analysis = get_conversation_analysis(conversation_id, token)
        analysis["transcript"] = transcript
        experience_url = get_experience_url(conversation_id, token)
        analysis["experience_url"] = experience_url
        print(f"Inserting data for conversation ID: {conversation_id}")
        insert_conversation_data(conversation_id, analysis, conn)
        print(f"Data inserted successfully for conversation ID: {conversation_id}")
        idx += 1
    conn.close()


if __name__ == "__main__":
    main()
