
import os
import glob
import pandas as pd
import streamlit as st
import plotly.express as px

# Page configuration
st.set_page_config(
    page_title="MovieLens Analytics",
    page_icon="🎬",
    layout="wide"
)

# Project paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
RAW_DIR = os.path.join(BASE_DIR, "ml-latest-small")


# Read Spark output folders
@st.cache_data
def load_spark_output(folder):
    path = os.path.join(PROCESSED_DIR, folder)
    files = glob.glob(os.path.join(path, "part-*.csv"))

    if not files:
        return pd.DataFrame()

    return pd.concat(
        [pd.read_csv(file) for file in files],
        ignore_index=True
    )


@st.cache_data
def load_raw_data():
    ratings = pd.read_csv(os.path.join(RAW_DIR, "ratings.csv"))
    movies = pd.read_csv(os.path.join(RAW_DIR, "movies.csv"))
    return ratings, movies


# Load data
summary = load_spark_output("summary")
top_users = load_spark_output("top_users")
top_movies = load_spark_output("top_movies")
genre_stats = load_spark_output("genre_stats")
rating_distribution = load_spark_output("rating_distribution")
ratings, movies = load_raw_data()

# Dashboard heading
st.title("🎬 MovieLens Movie Rating Analytics")
st.caption("Big Data Analytics using Hadoop, Apache Spark and Streamlit")

st.divider()

# KPI metrics
total_ratings = len(ratings)
total_movies = len(movies)
total_users = ratings["userId"].nunique()
average_rating = ratings["rating"].mean()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Ratings", f"{total_ratings:,}")
c2.metric("Total Movies", f"{total_movies:,}")
c3.metric("Total Users", f"{total_users:,}")
c4.metric("Average Rating", f"{average_rating:.2f} / 5")

st.divider()

# Genre analysis
st.subheader("⭐ Genre-wise Rating Analysis")

if not genre_stats.empty:
    genre_stats = genre_stats.sort_values("avg_rating", ascending=False)

    fig = px.bar(
        genre_stats,
        x="genre",
        y="avg_rating",
        color="avg_rating",
        title="Average Rating by Genre",
        labels={
            "genre": "Movie Genre",
            "avg_rating": "Average Rating"
        }
    )

    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

# Rating distribution
st.subheader("📊 Rating Distribution")

if not rating_distribution.empty:
    fig = px.bar(
        rating_distribution,
        x="rating",
        y="total_ratings",
        text="total_ratings",
        title="Number of Ratings for Each Rating Value",
        labels={
            "rating": "Rating",
            "total_ratings": "Number of Ratings"
        }
    )

    st.plotly_chart(fig, use_container_width=True)

# Top movies
st.subheader("🏆 Highest-Rated Movies")

if not top_movies.empty:
    min_count = st.slider(
        "Minimum number of ratings",
        min_value=50,
        max_value=300,
        value=50,
        step=10
    )

    search = st.text_input("Search for a movie")

    filtered_movies = top_movies[
        top_movies["rating_count"] >= min_count
    ]

    if search:
        filtered_movies = filtered_movies[
            filtered_movies["title"].str.contains(
                search, case=False, na=False
            )
        ]

    st.dataframe(
        filtered_movies.sort_values(
            "avg_rating", ascending=False
        ),
        use_container_width=True,
        hide_index=True
    )

# Most active users
st.subheader("👥 Top 10 Most Active Users")

if not top_users.empty:
    fig = px.bar(
        top_users.sort_values("total_ratings", ascending=True),
        x="total_ratings",
        y="userId",
        orientation="h",
        title="Users Who Submitted the Most Ratings",
        labels={
            "userId": "User ID",
            "total_ratings": "Number of Ratings"
        }
    )

    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.caption("Developed using MovieLens, Hadoop HDFS, PySpark and Streamlit.")
