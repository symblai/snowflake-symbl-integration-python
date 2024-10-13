import json
import os
from time import sleep
from typing import Dict

import requests
from tqdm import tqdm

from utils import symbl_token, snowflake_connection


def load_crm_data():
    # Load the crm-sample.json
    with open('data/crm-sample.json', 'r') as crm_file:
        crm_data = json.load(crm_file)
    return crm_data


# Function to load all transcript files and organize data
def load_all_transcripts(directory_path):
    transcripts = {}

    # Iterate through all files in the directory
    for filename in os.listdir(directory_path):
        if filename.startswith('transcript_') and filename.endswith('.json'):
            file_path = os.path.join(directory_path, filename)

            # Open and load the transcript JSON data
            with open(file_path, 'r') as transcript_file:
                transcript_data = json.load(transcript_file)

                # Extract opportunity_id and interaction_id from the JSON object
                opportunity_id = transcript_data['opportunity_id']
                interaction_id = transcript_data['interaction_id']
                messages = transcript_data['messages']

                # Organize the data into the transcripts dictionary
                if opportunity_id not in transcripts:
                    transcripts[opportunity_id] = {}
                transcripts[opportunity_id][interaction_id] = messages
    return transcripts


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


def wait_for_job_to_complete(job_id, conversation_id, token):
    while True:
        sleep(5)
        status = check_job_status(job_id, token)
        if status == "completed":
            status = check_insights_status(conversation_id, token)
            if status == "completed":
                status = check_call_score_status(conversation_id, token)
                if status == "completed":
                    break
        if status == "failed":
            break
    return conversation_id, job_id, status


def submit_transcript(transcript_messages, score_card_id, details=None, token=None):
    if details is None:
        details = {}
    prospect_name = details.get("prospect_name")
    stage = details.get("stage")
    meeting_name = details.get("meeting_name", f"Sales Meeting with {prospect_name}")
    datetime = details.get("datetime")

    payload = {
        "name": meeting_name,
        "startTime": datetime,
        "messages": transcript_messages,
        "detectEntities": True,
        "features": {
            "featureList": ["callScore", "insights"],
            "callScore": {"scorecardId": score_card_id}
        },
        "conversationType": "sales",
        "metadata": {
            "prospectName": prospect_name,
            "salesStage": stage
        }
    }
    response = requests.post(url="https://api.symbl.ai/v1/process/text",
                             json=payload,
                             headers={"accept": "application/json",
                                      "Authorization": f"Bearer {token}"})
    if response.status_code >= 400:
        print(f"Error processing transcript: {response.json()}")
        raise Exception(f"Error processing transcript: {response.json()}")

    response = response.json()
    conversation_id, job_id = response["conversationId"], response["jobId"]
    return conversation_id, job_id


def submit_audio_file(audio_url, speaker_channels, score_card_id, details=None, token=None):
    if details is None:
        details = {}
    prospect_name = details.get("prospect_name")
    stage = details.get("stage")
    meeting_name = details.get("meeting_name", f"Sales Meeting with {prospect_name}")
    payload = {
        "name": meeting_name,
        "url": audio_url,
        "enableSeparateRecognitionPerChannel": True,
        "channelMetadata": speaker_channels,
        "detectEntities": True,
        "features": {
            "featureList": ["callScore", "insights"],
            "callScore": {"scorecardId": score_card_id}
        },
        "conversationType": "sales",
        "metadata": {
            "prospectName": prospect_name,
            "salesStage": stage
        }
    }
    response = requests.post(url="https://api.symbl.ai/v1/process/audio/url",
                             json=payload,
                             headers={"accept": "application/json",
                                      "Authorization": f"Bearer {token}"})
    if response.status_code >= 400:
        print(f"Error processing audio file: {response.json()}")
        raise Exception(f"Error processing audio file: {response.json()}")

    response = response.json()
    conversation_id, job_id = response["conversationId"], response["jobId"]
    return conversation_id, job_id


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

    response = requests.get(url=f"https://api.symbl.ai/v1/conversations/{conversation_id}/trackers",
                            headers={"accept": "application/json",
                                     "Authorization": f"Bearer {token}"})
    trackers = response.json()

    members = insights["attendees"]
    for member in members:
        for member_analytics in analytics["members"]:
            if member["id"] == member_analytics["id"]:
                member["pace_wpm"] = member_analytics["pace"]["wpm"]
                member["talk_time_sec"] = member_analytics["talkTime"]["seconds"]
                member["talk_time_percentage"] = member_analytics["talkTime"]["percentage"]
                member["listen_time_sec"] = member_analytics["listenTime"]["seconds"]
                member["listen_time_percentage"] = member_analytics["listenTime"]["percentage"]
                member["overlap_duration_sec"] = member_analytics["overlap"].get("overlapDuration", 0)
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

    _trackers = []
    for tracker in trackers:
        tracker_name = tracker["name"]
        for match in tracker["matches"]:
            for message_ref in match["messageRefs"]:
                text = message_ref["text"]
                _trackers.append({
                    "tracker_name": tracker_name,
                    "text": text
                })
    trackers = _trackers

    analysis = {
        "name": insights["meetingName"],
        "url": insights.get("videoUrl"),
        "datetime": insights["startTime"],
        "executive_summary": insights["callSummary"]["executiveSummary"],
        "bullet_points": insights["callSummary"]["longSummary"]["bulletPoints"],
        "short_summary": insights["callSummary"]["smallSummary"]["paragraphs"],
        "short_bullet_points": insights["callSummary"]["smallSummary"]["bulletPoints"],
        "long_summary": insights["callSummary"]["longSummary"]["paragraphs"],
        "next_steps": [item["step"] for item in insights["callNextSteps"]["nextSteps"]],
        "question_answers": [item for item in insights["callQuestionAndAnswers"]["questionsAndAnswers"]],
        "sentiment": [chunk for chunk in insights["callSentiment"]["chunks"]],
        "call_score": insights["callScore"]["score"],
        "call_score_summary": insights["callScore"]["summary"],
        "call_score_criteria": [criteria for criteria in insights["callScore"]["criteria"]],
        "objections": [objection for objection in insights["callObjections"]["objections"]],
        "topics": topics["topics"],
        "entities": entities,
        "talk_time_metrics": analytics["metrics"],
        "members": members,
        "trackers": trackers
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


def create_score_card_if_not_exists(name="Default Sales Score Card", token=None):
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
                                                    "engagement, question handling, forward motion, and sales process.",
                                     "criteriaList": ["Symbl.Communication_And_Engagement",
                                                      "Symbl.Question_Handling",
                                                      "Symbl.Forward_Motion",
                                                      "Symbl.Sales_Process"]
                                 })
        score_card_id = response.json()["id"]
    return score_card_id


def insert_crm_data(crm_data, snowflake_conn):
    account_query = """
        INSERT INTO conversation_analysis.Account (account_id, account_name)
        VALUES (%s, %s)
    """
    sales_rep_query = """
        INSERT INTO conversation_analysis.SalesRep (sales_rep_id, name, email, phone)
        VALUES (%s, %s, %s, %s)
    """
    contact_query = """
        INSERT INTO conversation_analysis.Contact (contact_id, name, email, phone)
        VALUES (%s, %s, %s, %s)
    """
    lead_source_query = """
        INSERT INTO conversation_analysis.LeadSource (lead_source_id, lead_source_name)
        VALUES (%s, %s)
    """
    opportunity_query = """
        INSERT INTO conversation_analysis.Opportunity (opportunity_id, account_id, sales_rep_id, contact_id, lead_source_id, 
                                 opportunity_amount, probability, status, stage, time_since_open, 
                                 target_close_date, next_step)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    stage_transition_query = """
        INSERT INTO conversation_analysis.StageTransition (stage_transition_id, opportunity_id, stage, transition_date)
        VALUES (%s, %s, %s, %s)
    """
    communication_history_query = """
        INSERT INTO conversation_analysis.CommunicationHistory (communication_id, opportunity_id, channel, communication_date, content, 
                                          sales_rep_id, contact_id, datetime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    account_data, sales_rep_data, contact_data = [], [], []
    lead_source_data, opportunity_data, stage_transition_data, communication_history_data = [], [], [], []

    cursor = snowflake_conn.cursor()

    for entry in crm_data:
        account_id = entry['id']
        account_name = entry['accountName']

        sales_rep_id = f"rep_{entry['salesRep']['email']}"
        sales_rep_name = entry['salesRep']['name']
        sales_rep_email = entry['salesRep']['email']
        sales_rep_phone = entry['salesRep']['phone']

        contact_id = f"contact_{entry['contact']['email']}"
        contact_name = entry['contact']['name']
        contact_email = entry['contact']['email']
        contact_phone = entry['contact']['phone']

        lead_source_id = f"lead_{account_id}"
        lead_source_name = entry['leadSource']

        opportunity_id = f"opportunity_{account_id}"
        opportunity_amount = entry['opportunityAmount']
        probability = entry['probability']
        status = entry['status']
        stage = entry['stage']
        time_since_open = entry['timeSinceOpen']
        target_close_date = entry['targetCloseDate']
        next_step = entry['nextStep']

        account_data.append((account_id, account_name))
        sales_rep_data.append((sales_rep_id, sales_rep_name, sales_rep_email, sales_rep_phone))
        contact_data.append((contact_id, contact_name, contact_email, contact_phone))
        lead_source_data.append((lead_source_id, lead_source_name))
        opportunity_data.append((opportunity_id, account_id, sales_rep_id, contact_id, lead_source_id,
                                 opportunity_amount, probability, status, stage, time_since_open,
                                 target_close_date, next_step))

        for idx, transition in enumerate(entry['stageTransitions'], start=1):
            stage_transition_id = f"transition_{account_id}_{idx}"
            stage = transition['stage']
            transition_date = transition['date']
            stage_transition_data.append((stage_transition_id, opportunity_id, stage, transition_date))

        for communication in entry['communicationHistory']:
            communication_id = communication['interactionId']
            channel = communication['channel']
            communication_date = communication['datetime']
            content = communication['content']
            datetime = communication['datetime']
            communication_history_data.append((communication_id, opportunity_id, channel, communication_date,
                                               content, sales_rep_id, contact_id, datetime))

    cursor.executemany(account_query, account_data)
    cursor.executemany(sales_rep_query, sales_rep_data)
    cursor.executemany(contact_query, contact_data)
    cursor.executemany(lead_source_query, lead_source_data)
    cursor.executemany(opportunity_query, opportunity_data)
    cursor.executemany(stage_transition_query, stage_transition_data)
    cursor.executemany(communication_history_query, communication_history_data)

    snowflake_conn.commit()
    cursor.close()


def insert_conversation_data(conversation_id, communication_id, json_data: Dict, snowflake_conn):
    """
    Inserts data from the Symbl Outputs into Snowflake schema in batch mode.
    """
    cursor = snowflake_conn.cursor()

    # Extract the main conversation fields
    name = json_data.get("name", conversation_id)
    url = json_data.get("url", "")
    call_score = json_data.get("call_score", 0)
    conversation_date_time = json_data.get("datetime", "")
    executive_summary = json_data.get("executive_summary", "")
    long_summary = json_data.get("long_summary", "")
    bullet_points = json_data.get("bullet_points", [])
    short_summary = json_data.get("short_summary", "")
    short_bullet_points = json_data.get("short_bullet_points", [])
    call_score_summary = json_data.get("call_score_summary", "")
    objections = json_data.get("objections", [])
    total_silence_sec = json_data["talk_time_metrics"][0]["seconds"]
    total_talk_time_sec = json_data["talk_time_metrics"][1]["seconds"]
    total_overlap_sec = json_data["talk_time_metrics"][2]["seconds"]
    transcript = json_data.get("transcript", [])
    experience_url = json_data.get("experience_url", "")

    # Insert into Conversation table
    insert_conversation_query = f"""
        INSERT INTO conversation_analysis.Conversation 
        (conversation_id, communication_id, name, url, datetime, long_summary, call_score, executive_summary,
        short_summary, call_score_summary, 
        total_silence_sec, total_talk_time_sec, total_overlap_sec, experience_url, bullet_points, short_bullet_points)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conversation_values = [
        (conversation_id, communication_id, name, url, conversation_date_time, long_summary, call_score,
         executive_summary, short_summary, call_score_summary,
         total_silence_sec, total_talk_time_sec, total_overlap_sec, experience_url,
         '\n'.join(bullet_points), '\n'.join(short_bullet_points))]

    cursor.executemany(insert_conversation_query, conversation_values)

    # Insert members data
    insert_member_query = f"""
        INSERT INTO conversation_analysis.Members 
        (member_id, conversation_id, name, user_id, pace_wpm, talk_time_sec, listen_time_sec, overlap_duration_sec)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    members_values = [
        (member["id"], conversation_id, member["name"], member["userId"], member["pace_wpm"],
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

    # Insert trackers data
    insert_trackers_query = f"""
        INSERT INTO conversation_analysis.Trackers
        (tracker_id, conversation_id, tracker_name, text)
        VALUES (%s, %s, %s, %s)
    """
    trackers_values = [
        (f"tracker_{conversation_id}_{idx + 1}", conversation_id, tracker["tracker_name"], tracker["text"])
        for idx, tracker in enumerate(json_data["trackers"])
    ]

    cursor.executemany(insert_trackers_query, trackers_values)

    # Insert questions data
    insert_question_query = f"""
        INSERT INTO conversation_analysis.Questions
        (question_id, conversation_id, question, answer, question_by)
        VALUES (%s, %s, %s, %s, %s)
    """
    questions_values = [
        (f"q_{conversation_id}_{idx + 1}", conversation_id, question["question"], question["answer"],
         question["questionBy"])
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

    # Insert objections data
    insert_objections_query = f"""
        INSERT INTO conversation_analysis.Objections
        (objection_id, conversation_id, objection, objection_response)
        VALUES (%s, %s, %s, %s)
    """
    objections_values = [
        (f"obj_{conversation_id}_{idx + 1}", conversation_id, obj["objection"], obj["response"])
        for idx, obj in enumerate(objections)
    ]

    cursor.executemany(insert_objections_query, objections_values)

    # Commit the transaction and close the cursor
    snowflake_conn.commit()
    cursor.close()


def process_opportunity(opportunity, interaction_transcripts, score_card_id, token, conn):
    # Extract the opportunity details
    account_name = opportunity['accountName']
    stage = opportunity['stage']
    # Iterate through communication history
    for index, comm_entry in enumerate(opportunity.get("communicationHistory", [])):
        if comm_entry["channel"] in ["Phone", "Meeting"]:
            meeting_name = f"{account_name} / InfiniMeld"
            interaction_id = comm_entry["interactionId"]
            datetime = comm_entry["datetime"]
            transcript_messages = interaction_transcripts[interaction_id]
            conversation_id, job_id = submit_transcript(transcript_messages, score_card_id, details={
                "meeting_name": meeting_name,
                "prospect_name": account_name,
                "stage": stage,
                "datetime": datetime
            }, token=token)
            conversation_id, job_id, status = wait_for_job_to_complete(job_id, conversation_id, token)
            if status != "completed":
                print(f"Processing of conversation ID {conversation_id} failed.")
                continue

            print(f"Fetching data for conversation ID: {conversation_id}")
            transcript = get_transcript(conversation_id, token)
            analysis = get_conversation_analysis(conversation_id, token)
            analysis["transcript"] = transcript
            experience_url = get_experience_url(conversation_id, token)
            analysis["experience_url"] = experience_url
            print(f"Inserting data for conversation ID: {conversation_id}")
            insert_conversation_data(conversation_id, interaction_id, analysis, conn)
            print(f"Data inserted successfully for conversation ID: {conversation_id}")


def main():
    conn = snowflake_connection()
    token = symbl_token()
    score_card_id = create_score_card_if_not_exists(token=token)

    # Load CRM data
    crm_data = load_crm_data()
    print(f"Total number of opportunities: {len(crm_data)}")
    print("Inserting CRM data into Snowflake")
    insert_crm_data(crm_data, conn)
    print("CRM data inserted successfully")

    # Load all transcripts
    transcripts_data = load_all_transcripts("data")

    try:
        # Process each opportunity
        for opportunity in tqdm(crm_data):
            opportunity_id = opportunity["id"]
            process_opportunity(opportunity, transcripts_data[opportunity_id], score_card_id, token, conn)
    except Exception as e:
        print(e)
    finally:
        from codecs import open
        with open("sql/create_chunk_tables.sql", "r", encoding='utf-8') as f:
            for cur in conn.execute_stream(f):
                for ret in cur:
                    print(ret)
        print('Chunk tables created.')

    conn.close()


if __name__ == "__main__":
    main()
