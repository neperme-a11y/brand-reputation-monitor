import json
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from transformers import pipeline

st.set_page_config(page_title="Brand Reputation Monitor", layout="wide")
st.title("Brand Reputation Monitor – 2023 Reviews Sentiment")

# ---- load data ----
try:
    with open("data.json", "r", encoding="utf-8") as f:
        data = json.load(f)
except FileNotFoundError:
    st.error("Ni data.json. Najprej zaženi: python scraper.py")
    st.stop()

# ---- load model once ----
@st.cache_resource
def load_model():
    return pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

# ---- navigation ----
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

    # clean
    reviews["date"] = pd.to_datetime(reviews["date"], errors="coerce")
    reviews["text"] = reviews["text"].fillna("").astype(str)
    reviews = reviews.dropna(subset=["date"])
    reviews = reviews[reviews["text"].str.len() > 0]

    if reviews.empty:
        st.warning("V data.json ni nobenega veljavnega review-a (datum ali text manjka).")
        st.stop()

    # month picker
    months = [f"2023-{m:02d}" for m in range(1, 13)]
    selected = st.select_slider("Select month (2023)", options=months, value="2023-01")
    y, m = map(int, selected.split("-"))

    # filter
    month_reviews = reviews[(reviews["date"].dt.year == y) & (reviews["date"].dt.month == m)].copy()
    st.caption(f"Found **{len(month_reviews)}** reviews in **{selected}**.")

    if month_reviews.empty:
        st.info("Za izbran mesec ni reviewev. Izberi drug mesec.")
        fig, ax = plt.subplots()
        ax.bar(["Positive", "Negative"], [0, 0])
        ax.set_ylabel("Count")
        ax.set_title(f"Positive vs Negative – {selected} (n=0)")
        st.pyplot(fig)
        st.stop()

    # sentiment
    st.markdown("### Sentiment Analysis (Hugging Face)")
    st.write("Model: `distilbert-base-uncased-finetuned-sst-2-english`")

    model = load_model()
    preds = model(month_reviews["text"].tolist())

    month_reviews["sentiment"] = [
        "Positive" if p["label"].upper() == "POSITIVE" else "Negative"
        for p in preds
    ]

    # counts
    pos = int((month_reviews["sentiment"] == "Positive").sum())
    neg = int((month_reviews["sentiment"] == "Negative").sum())

    c1, c2 = st.columns(2)
    c1.metric("Positive", pos)
    c2.metric("Negative", neg)

    # chart
    st.markdown("### Visualization")
    fig, ax = plt.subplots()
    ax.bar(["Positive", "Negative"], [pos, neg])
    ax.set_ylabel("Count")
    ax.set_title(f"Positive vs Negative – {selected} (n={len(month_reviews)})")
    st.pyplot(fig)

    # table
    st.markdown("### Detailed Results")
    cols = ["date", "sentiment", "text"]
    if "product_id" in month_reviews.columns:
        cols.insert(1, "product_id")
    st.dataframe(month_reviews[cols], use_container_width=True)
