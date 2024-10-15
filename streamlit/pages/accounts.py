import os

import altair as alt
import streamlit as st

from data_utils import get_account_criteria_scores, get_opportunity_details
from utils import get_session

session = get_session()
if os.environ['HOME'] == '/home/udf':
    from app import show_top_controls
    show_top_controls(session)

def accounts(_session):
    details = get_opportunity_details(_session)
    st.dataframe(details,
                 hide_index=True,
                 use_container_width=True,
                 column_config={
                     "ACCOUNT_NAME": "Account",
                     "NUM_MEETINGS": st.column_config.NumberColumn(
                         "# Meetings",
                         width="small"
                     ),
                     "NEXT_STEP": st.column_config.TextColumn(
                         "Next Step"
                     ),
                     "STAGE": st.column_config.TextColumn(
                         "Stage"
                     ),
                     "OPPORTUNITY_AMOUNT": st.column_config.NumberColumn(
                         "Amount",
                         format="$%d",
                         width="small"
                     ),
                     "PROBABILITY": st.column_config.NumberColumn(
                         "Probability",
                         format="%d%%",
                         width="small"
                     ),
                     "TARGET_CLOSE_DATE": st.column_config.DateColumn(
                         "Close Date",
                         width="small"
                     ),
                     "TIME_SINCE_OPEN": st.column_config.NumberColumn(
                         "Days Open",
                         format="%d days",
                         width="small"
                     ),
                     "AVG_CALL_SCORE": st.column_config.ProgressColumn(
                         "Call Score",
                         format="%.1f",
                         min_value=0,
                         max_value=100,
                         width="small"
                     ),
                     "NUMBER_OF_NEXT_STEPS": st.column_config.NumberColumn(
                         "# Next Steps",
                         width="small"
                     )
                 })


def account_score_criteria_view(_session):
    account_criteria_scores = get_account_criteria_scores(_session)
    df_melt = account_criteria_scores.melt(id_vars="ACCOUNT_NAME", var_name="CRITERIA", value_name="SCORE")
    df_melt['SCORE'] = df_melt['SCORE'].astype(float)
    df_melt = df_melt.dropna()

    bars = alt.Chart(df_melt).mark_bar().encode(
        x=alt.X('SCORE:Q', title='Score', stack="zero"),
        y=alt.Y('ACCOUNT_NAME:N', title='Account'),
        color=alt.Color('CRITERIA:N', title='Criteria'),
        tooltip=[alt.Tooltip('ACCOUNT_NAME:N', title='Account'),
                 alt.Tooltip('CRITERIA:N', title='Criteria'),
                 alt.Tooltip('SCORE:Q', title='Score')]
    ).properties(
        title='Call Score Criteria by Account',
        width=1200,
        height=600
    )
    text = bars.mark_text(
        align='left',
        baseline='middle',
        dx=3  # Adjust the position of the text
    ).encode(
        # color=alt.value('black'),
        text=alt.Text('SCORE:Q', format='.0f')  # Format the score values to show on the bars
    )

    chart = bars + text

    st.altair_chart(chart)


accounts(session)
account_score_criteria_view(session)
