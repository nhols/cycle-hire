import requests
import psycopg2
import streamlit as st
import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure

st.set_page_config(layout="wide")


@st.cache_resource
def get_conn():
    return psycopg2.connect(dbname="cyclehire")


def plot_rides(df: pd.DataFrame) -> Figure:
    fig = px.bar(df, x="date", y="n", color="category", hover_data=["name"])
    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="YTD", step="year", stepmode="todate"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            ),
            rangeslider=dict(visible=True),
            type="date",
        )
    )
    return fig


@st.cache_data
def load_data() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql(
        """
                    SELECT
                        rides_by_day.*,
                        bank_hols.name,
                        CASE
                            WHEN name IS NOT NULL THEN 'bank_holiday'
                            WHEN  EXTRACT(DOW FROM date) IN (0,6) THEN 'weekend'
                            ELSE 'weekday'
                        END AS category
                    FROM
                        rides_by_day
                        LEFT JOIN bank_hols USING (date)
                    WHERE
                        date > '2012-01-01'
                    ORDER BY
                        date;
        """,
        conn,
    )
    return df


df = load_data()
fig = plot_rides(df)
st.plotly_chart(fig, theme=None, use_container_width=True)
