from datetime import datetime
import requests
import psycopg2
import streamlit as st
import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure
import pydeck as pdk

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


@st.cache_data
def load_stations_date(date) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql(
        f"""
        SELECT
            s.lat lat_s,
            s.lng lng_s,
            s.station_name name_s,
            e.lat lat_e,
            e.lng lng_e,
            e.station_name name_e,
            count(*) n
        FROM
            rides
            LEFT JOIN stations s ON rides.start_station_id = s.station_id
            LEFT JOIN stations e ON rides.end_station_id = e.station_id
            WHERE start_time::DATE = '{date}'
            GROUP BY 1,2,3,4,5,6;
        """,
        conn,
    )
    return df


def get_arc_plot(df: pd.DataFrame):
    df = df.dropna()
    GREEN_RGB = [0, 255, 0, 40]
    RED_RGB = [240, 100, 0, 40]
    arc_layer = pdk.Layer(
        "ArcLayer",
        data=df,
        get_width="n * 2",
        get_source_position=["lng_s", "lat_s"],
        get_target_position=["lng_e", "lat_e"],
        get_tilt=15,
        get_source_color=RED_RGB,
        get_target_color=GREEN_RGB,
        pickable=True,
        auto_highlight=True,
    )
    view_state = pdk.ViewState(
        latitude=51.5072,
        longitude=0.1276,
        bearing=45,
        pitch=50,
        zoom=8,
    )
    TOOLTIP_TEXT = {"html": "{n} rides <br /> Start in red at {name_s}; End in green at {name_e}"}
    return pdk.Deck(arc_layer, initial_view_state=view_state, tooltip=TOOLTIP_TEXT)


def arc_plot():
    fmt = "%Y-%m-%d"
    with st.form("Submit"):
        date = st.date_input(
            "Date",
            min_value=datetime.strptime("2012-01-04", fmt),
            max_value=datetime.strptime("2023-06-01", fmt),
            value=datetime.strptime("2012-01-04", fmt),
        )
        clicked = st.form_submit_button("Generate")
    if clicked:
        df = load_stations_date(date)
        st.pydeck_chart(get_arc_plot(df))
        st.write(df.sort_values("n", ascending=False))


arc_plot()
st.plotly_chart(fig, theme=None, use_container_width=True)
