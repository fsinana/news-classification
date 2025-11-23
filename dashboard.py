import streamlit as st
import pandas as pd
import glob
import os
from datetime import datetime

OUT_DIR = r"data/predictions"

st.set_page_config(page_title="Real-Time News Sentiment", layout="wide")
st.title("📰 Real-Time News Sentiment Dashboard")

# ---------------------------------------------------
# Load prediction JSON files (auto-refresh every 5 seconds)
# ---------------------------------------------------
@st.cache_data(ttl=5)
def load_predictions(out_dir=OUT_DIR, limit=1000):
    files = sorted(glob.glob(os.path.join(out_dir, "*.json")))
    if not files:
        return pd.DataFrame()

    rows = []
    for f in files[-limit:]:
        try:
            df = pd.read_json(f, lines=False)
            if isinstance(df, dict):
                df = pd.DataFrame([df])
            rows.append(df)
        except:
            try:
                df = pd.read_json(f, lines=True)
                rows.append(df)
            except:
                pass

    if not rows:
        return pd.DataFrame()

    df = pd.concat(rows, ignore_index=True, sort=False)
    return df

# ------------------------- UI -------------------------
df = load_predictions()

if df.empty:
    st.warning("No prediction files found yet. Wait for streaming...")
else:
    df = df.sort_values("timestamp", ascending=False)

    # Convert timestamp string to datetime
    if "timestamp" in df.columns:
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        except:
            pass

    sentiments = ["All", "Positive", "Negative"]
    selected = st.selectbox("Filter by sentiment:", sentiments)

    if selected != "All":
        df = df[df["sentiment"] == selected]

    st.subheader("Latest Predictions")
    st.dataframe(df[["timestamp", "source", "text", "sentiment", "url"]], use_container_width=True)

    # Count plot
    st.subheader("Sentiment Counts")
    st.bar_chart(df["sentiment"].value_counts())

st.info("Dashboard auto-refreshes every 5 seconds.")
