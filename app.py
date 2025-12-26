import json
from pathlib import Path

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from transformers import pipeline

DATA_PATH = Path("data.json")

st.set_page_config(page_title="Brand Reputation Monitor", layout="wide")
st.title("Brand Reputation Monitor – 2023 Reviews Sentiment")


@st.cache_data
def load_data():
    if not DATA_PATH.exists():
        return None
    return json.loads(DATA_PATH.read_text(encoding="utf-8"))


@st.cache_resource
def get_sentiment_model():
    # Hugging Face Transformer model (NO NLTK/TextBlob)
    return pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english",
    )


data = load_data()
if data is None:
    st.error("Ni data.json. Najprej zaženi: python scraper.py")
    st.stop()

section = st.sidebar.radio("Navigate", ["Products", "Testimonials", "Reviews"])

if section == "Products":
    st.subheader("Products")
    st.dataframe(pd.DataFrame(data.get("products", [])), use_container_width=True)

elif section == "Testimonials":
    st.subheader("Testimonials")
    st.dataframe(pd.DataFrame(data.get("testimonials", [])), use_container_width=True)

else:
    st.subheader("Reviews – Filter by Month (2023) + Sentiment Analysis")

    reviews = pd.DataFrame(data.get("reviews", []))
    if reviews.empty:
        st.warning("Reviews so prazni (data.json nima reviewev).")
        st.stop()

    reviews["date"] = pd.to_datetime(reviews.get("date"), errors="coerce")
    reviews["text"] = reviews.get("text").fillna("").astype(str)
    reviews = reviews.dropna(subset=["date"])
    reviews = reviews[reviews["text"].str.len() > 0]

    if reviews.empty:
        st.warning("V data.json ni nobenega veljavnega review-a (datum ali text manjka).")
        st.stop()

    months = [f"2023-{m:02d}" for m in range(1, 13)]
    selected_month = st.select_slider("Select month (2023)", options=months, value="2023-01")
    year, month = map(int, selected_month.split("-"))

    month_reviews = reviews[
        (reviews["date"].dt.year == year) &
        (reviews["date"].dt.month == month)
    ].copy()

    st.caption(f"Found **{len(month_reviews)}** reviews in **{selected_month}**.")

    # handle 0 reviews
    if month_reviews.empty:
        st.info("Za izbran mesec ni reviewev. Izberi drug mesec.")

        fig, ax = plt.subplots()
        ax.bar(["Positive", "Negative"], [0, 0])
        ax.set_ylabel("Count")
        ax.set_title(f"Positive vs Negative – {selected_month} (n=0)")
        st.pyplot(fig, clear_figure=True)
        st.stop()

    # === REQUIRED: Sentiment Analysis (Hugging Face pipeline) ===
    st.markdown("### Sentiment Analysis (Hugging Face)")
    st.write("Model: `distilbert-base-uncased-finetuned-sst-2-english`")
    st.write("Classifying every review in the selected month as Positive or Negative...")

    analyze_df = month_reviews.sort_values("date", ascending=False).copy()

    model = get_sentiment_model()
    preds = model(analyze_df["text"].tolist())

    analyze_df["sentiment"] = [
        "Positive" if p["label"].upper() == "POSITIVE" else "Negative"
        for p in preds
    ]

    # (Optional to keep, not displayed anymore)
    analyze_df["confidence"] = [float(p["score"]) for p in preds]

    # Summary: counts only (NO avg confidence)
    summary = (
        analyze_df.groupby("sentiment")
        .agg(count=("sentiment", "size"))
        .reindex(["Positive", "Negative"])
        .fillna(0)
    )

    pos_count = int(summary.loc["Positive", "count"])
    neg_count = int(summary.loc["Negative", "count"])

    col1, col2 = st.columns(2)
    col1.metric("Positive", pos_count)
    col2.metric("Negative", neg_count)

    # === REQUIRED: Visualization (bar chart) ===
    st.markdown("### Visualization")

    labels = ["Positive", "Negative"]
    counts = [pos_count, neg_count]

    fig, ax = plt.subplots()
    ax.bar(labels, counts)
    ax.set_ylabel("Count")
    ax.set_title(f"Positive vs Negative – {selected_month} (n={len(analyze_df)})")
    st.pyplot(fig, clear_figure=True)

    st.markdown("### Detailed Results")
    show_cols = ["date", "sentiment", "text"]
    if "product_id" in analyze_df.columns:
        show_cols.insert(1, "product_id")

    st.dataframe(analyze_df[show_cols], use_container_width=True)
