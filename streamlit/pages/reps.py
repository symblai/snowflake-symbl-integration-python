import os

import altair as alt
import streamlit as st
from data_utils import get_sales_reps, get_trackers_by_rep, get_sales_rep_call_scores

from utils import get_session, score_colors

session = get_session()

if os.environ['HOME'] == '/home/udf':
    from app import show_top_controls
    show_top_controls(session)


def rep_by_score_criteria_view(_session):
    reps_by_call_scores = get_sales_rep_call_scores(_session)
    reps_by_call_scores['CALL_SCORE'] = reps_by_call_scores['CALL_SCORE'].astype(float)
    # df_melt = reps_by_call_scores.melt(id_vars="REP_NAME", var_name="CRITERIA", value_name="SCORE")
    bars = alt.Chart(reps_by_call_scores).mark_bar().encode(
        x=alt.X('CALL_SCORE:Q', title='Score', stack="zero"),
        y=alt.Y('REP_NAME:N', title='Account', sort='-x'),
        color=alt.Color('CRITERIA_NAME:N', title='Criteria'),
        tooltip=[alt.Tooltip('REP_NAME:N', title='Representative'),
                 alt.Tooltip('CRITERIA_NAME:N', title='Criteria'),
                 alt.Tooltip('CALL_SCORE:Q', title='Score')]
    ).properties(
        title='Call Scores by Sales Rep',
        height=500
    )
    text = bars.mark_text(
        align='left',
        baseline='middle',
        dx=3  # Adjust the position of the text
    ).encode(
        # color=alt.value('black'),
        text=alt.Text('CALL_SCORE:Q', format='.1f')  # Format the score values to show on the bars
    )

    chart = bars + text

    st.altair_chart(chart, use_container_width=True)


sales_rep_df = get_sales_reps(session)
sales_rep_df = sales_rep_df.sort_values(by='AVG_CALL_SCORE', ascending=False)

with st.container():
    score_col_configs = {}
    for col in sales_rep_df.columns:
        if col not in ['REP_NAME', 'AVG_CALL_SCORE', 'TRACKERS']:
            score_col_configs[col] = st.column_config.ProgressColumn(
                col,
                format="%d",
                min_value=0,
                max_value=100,
                width="small"
            )
    column_config = {
        "REP_NAME": "Sales Rep",
        "TRACKERS": st.column_config.ListColumn(
            "Trackers",
            width="large"
        ),
        "AVG_CALL_SCORE": st.column_config.ProgressColumn(
            "Call Score",
            format="%d",
            min_value=0,
            max_value=100,
            width="small"
        ),
        **score_col_configs
    }

    st.markdown("**Top Performers**")
    top_perf_df = sales_rep_df.head(5)
    top_perf_df = top_perf_df.style.applymap(score_colors, subset=['AVG_CALL_SCORE', *score_col_configs.keys()])
    st.dataframe(top_perf_df, use_container_width=True, hide_index=True,
                 column_config=column_config)
    st.markdown("**Low Performers**")
    low_perf_df = sales_rep_df.tail(5)[::-1]
    low_perf_df = low_perf_df.style.applymap(score_colors, subset=['AVG_CALL_SCORE', *score_col_configs.keys()])
    st.dataframe(low_perf_df, use_container_width=True, hide_index=True,
                 column_config=column_config)
    rep_by_score_criteria_view(session)

with st.container():
    trackers_by_rep_df = get_trackers_by_rep(session)

    fig_trackers_rep = alt.Chart(trackers_by_rep_df).mark_bar().encode(
        x=alt.X('TRACKER_COUNT', title='# Instances', stack="zero"),
        y=alt.Y('SALES_REP', title='Sales Rep'),

        color=alt.Color('TRACKER_NAME', title='Category'),
    ).properties(
        title="Categories by Sales Reps",
        height=500
    )
    st.altair_chart(fig_trackers_rep, use_container_width=True)
