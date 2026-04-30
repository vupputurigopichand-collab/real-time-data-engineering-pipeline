import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pymysql

st.set_page_config(
    page_title="NYC Taxi Streaming Pipeline",
    page_icon="🚕",
    layout="wide"
)

def get_connection():
    return pymysql.connect(
        host="localhost",
        port=3307,
        user="root",
        password="root",
        database="taxi_db"
    )

def run_query(query):
    try:
        conn = get_connection()
        result = pd.read_sql(query, conn)
        conn.close()
        return result
    except Exception:
        return pd.DataFrame()

st.title("🚕 NYC Taxi Real-Time Streaming Pipeline")
st.markdown("**University of Greenwich | COMP1682 FYP | Gopi Chand Vupputuri**")
st.divider()

st.header("📊 Streaming Scenario Evaluation")

latency_df = run_query("""
    SELECT 
        stream_mode,
        COUNT(*) as records,
        ROUND(AVG(TIMESTAMPDIFF(SECOND, event_time, inserted_at)), 2) as avg_latency,
        MIN(TIMESTAMPDIFF(SECOND, event_time, inserted_at)) as min_latency,
        MAX(TIMESTAMPDIFF(SECOND, event_time, inserted_at)) as max_latency
    FROM clean_taxi_trips
    GROUP BY stream_mode
    ORDER BY FIELD(stream_mode, 'HIGH', 'NORMAL', 'LOW', 'BURST')
""")

if latency_df.empty:
    st.warning("⏳ No data yet — waiting for pipeline to write records. Refresh in a few seconds...")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
colors = {"HIGH": "🔴", "NORMAL": "🟡", "LOW": "🟢", "BURST": "🔵"}

for i, row in latency_df.iterrows():
    col = [col1, col2, col3, col4][i % 4]
    with col:
        st.metric(
            label=f"{colors.get(row['stream_mode'], '⚪')} {row['stream_mode']} Mode",
            value=f"{row['avg_latency']}s avg latency",
            delta=f"{row['records']} records"
        )

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("⏱️ Latency Comparison")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Avg Latency",
        x=latency_df["stream_mode"],
        y=latency_df["avg_latency"],
        marker_color=["#FF4B4B", "#FFA500", "#00CC96", "#636EFA"][:len(latency_df)]
    ))
    fig.add_trace(go.Bar(
        name="Max Latency",
        x=latency_df["stream_mode"],
        y=latency_df["max_latency"],
        marker_color=["#FF9999", "#FFD580", "#99EED3", "#A3B8FF"][:len(latency_df)]
    ))
    fig.update_layout(
        barmode="group",
        xaxis_title="Stream Mode",
        yaxis_title="Latency (seconds)",
        legend=dict(orientation="h")
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("📦 Records Processed")
    fig = px.bar(
        latency_df,
        x="stream_mode",
        y="records",
        color="stream_mode",
        color_discrete_sequence=["#FF4B4B", "#FFA500", "#00CC96", "#636EFA"],
        labels={"stream_mode": "Stream Mode", "records": "Records Processed"}
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("🏪 Vendor Revenue Summary")
    vendor_df = run_query("""
        SELECT VendorID, trip_count, avg_fare, total_revenue
        FROM vendor_revenue_summary
        ORDER BY VendorID
    """)
    if not vendor_df.empty:
        vendor_df["VendorID"] = vendor_df["VendorID"].astype(str)
        fig = px.bar(
            vendor_df,
            x="VendorID",
            y="total_revenue",
            color="VendorID",
            text="trip_count",
            color_discrete_sequence=["#636EFA", "#EF553B", "#00CC96"],
            labels={
                "VendorID": "Vendor ID",
                "total_revenue": "Total Revenue ($)"
            }
        )
        fig.update_layout(showlegend=False, xaxis_type="category")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No vendor data available yet")

with col2:
    st.subheader("👥 Passenger Demand Analysis")
    passenger_df = run_query("""
        SELECT passenger_count, trip_count, avg_fare
        FROM passenger_demand
        ORDER BY passenger_count
    """)
    if not passenger_df.empty:
        passenger_df["passenger_count"] = passenger_df["passenger_count"].astype(str)
        fig = px.bar(
            passenger_df,
            x="passenger_count",
            y="trip_count",
            color="passenger_count",
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={
                "passenger_count": "Passenger Count",
                "trip_count": "Number of Trips"
            }
        )
        fig.update_layout(showlegend=False, xaxis_type="category")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No passenger data available yet")

st.divider()

st.subheader("📈 Latency Range per Scenario")
fig = go.Figure()
colors_box = {"HIGH": "#FF4B4B", "NORMAL": "#FFA500", "LOW": "#00CC96", "BURST": "#636EFA"}
for _, row in latency_df.iterrows():
    fig.add_trace(go.Bar(
        name=row["stream_mode"],
        x=[row["stream_mode"]],
        y=[row["max_latency"] - row["min_latency"]],
        base=[row["min_latency"]],
        marker_color=colors_box.get(row["stream_mode"], "#888"),
        text=f"Avg: {row['avg_latency']}s",
        textposition="inside"
    ))
fig.update_layout(
    yaxis_title="Latency (seconds)",
    xaxis_title="Stream Mode",
    showlegend=False,
    barmode="group"
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("🗃️ Raw Evaluation Data")
st.dataframe(latency_df, use_container_width=True)

st.markdown("---")
st.markdown("*Real-Time Data Engineering Pipeline | Apache Kafka + Spark + Airflow + MySQL*")
