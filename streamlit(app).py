import streamlit as st
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
import plotly.express as px

# Initialize SparkSession
spark = SparkSession.builder.appName("PlayStoreAnalysis").getOrCreate()

# Load data
df = spark.read.csv('googleplaystore.csv', header=True, inferSchema=True)

# Data preprocessing
df = df.withColumn("Reviews", col("Reviews").cast(IntegerType())) \
    .withColumn("Installs", regexp_replace(col("Installs"), "[^0-9]", "")) \
    .withColumn("Installs", col("Installs").cast(IntegerType())) \
    .withColumn("Price", regexp_replace(col("Price"), "[$]", "")) \
    .withColumn("Price", col("Price").cast(IntegerType()))

# Create temporary view
df.createOrReplaceTempView("PlayStoreAnalysis")

# Define Streamlit app


def main():

    st.image("play.png", width=50)
    st.title("Google Play Store Analysis")
    # Sidebar
    st.sidebar.subheader("Navigation")
    page = st.sidebar.radio("Go to", ["Home", "Top 10 Reviewed Apps", "Category-wise Distribution", "Most Installed Apps",
                                      "App Versions Comparison", "Average Rating by Category", "Top 10 Paid Apps",
                                      "Content Rating Distribution", "Price vs Rating", "Size vs Installs",
                                      "Genre-wise Distribution", "Free vs Paid Apps", "Largest App by size",
                                      "Apps with the Most Reviews in Each Category", "Installs vs. Ratings Scatter Plot"])

    if page == "Home":
        st.write("Welcome to Google Play Store Analysis App!")

    elif page == "Top 10 Reviewed Apps":
        st.subheader("Top 10 Reviewed Apps")
        top_10_reviewed_apps = spark.sql("""
            SELECT App, SUM(Reviews) AS ReviewCount
            FROM PlayStoreAnalysis
            GROUP BY App
            ORDER BY ReviewCount DESC
            LIMIT 10
        """).toPandas()
        st.bar_chart(top_10_reviewed_apps.set_index("App"))

    elif page == "Category-wise Distribution":
        st.subheader("Category-wise Distribution")
        category_apps = spark.sql("""
            SELECT Category, COUNT(APP) AS AppCount
            FROM PlayStoreAnalysis
            GROUP BY Category
            ORDER BY AppCount DESC
        """).toPandas()
        st.bar_chart(category_apps.set_index("Category"))

    # Add other pages similarly...
    elif page == "Most Installed Apps":
        st.subheader("Most Installed Apps")
        most_installed_apps = spark.sql("""
            SELECT App, SUM(Installs) AS InstallCount
            FROM PlayStoreAnalysis
            GROUP BY App
            ORDER BY InstallCount DESC
            LIMIT 10
        """).toPandas()

        # Create pie chart
        st.plotly_chart(px.pie(most_installed_apps, values='InstallCount',
                        names='App', title='Most Installed Apps'))

    elif page == "App Versions Comparison":
        st.subheader("App Versions Comparison")
        app_versions = spark.sql("""
            SELECT App, COUNT(DISTINCT `Android Ver`) AS VersionCount
            FROM PlayStoreAnalysis
            GROUP BY App
            ORDER BY VersionCount DESC
            LIMIT 10
        """).toPandas()
        st.bar_chart(app_versions.set_index("App"))

    elif page == "Average Rating by Category":
        st.subheader("Average Rating by Category")
        avg_rating_by_category = spark.sql("""
            SELECT Category, AVG(Rating) AS AvgRating
            FROM PlayStoreAnalysis
            GROUP BY Category
            ORDER BY AvgRating DESC
        """).toPandas()
        st.bar_chart(avg_rating_by_category.set_index("Category"))

    elif page == "Top 10 Paid Apps":
        st.subheader("Top 10 Paid Apps")
        top_10_paid_apps = spark.sql("""
            SELECT App, Price
            FROM PlayStoreAnalysis
            WHERE Price > 0
            ORDER BY Price DESC
            LIMIT 10
        """).toPandas()
        st.bar_chart(top_10_paid_apps.set_index("App"))

    elif page == "Content Rating Distribution":
        st.subheader("Content Rating Distribution")
        content_rating = spark.sql("""
            SELECT `Content Rating`, COUNT(`Content Rating`) AS RatingCount
            FROM PlayStoreAnalysis
            GROUP BY `Content Rating`
        """).toPandas()
        st.bar_chart(content_rating.set_index("Content Rating"))

    elif page == "Price vs Rating":
        st.subheader("Price vs Rating")
        price_rating = spark.sql("""
            SELECT Price, AVG(Rating) AS AvgRating
            FROM PlayStoreAnalysis
            WHERE Price > 0
            GROUP BY Price
            ORDER BY Price
        """).toPandas()
        st.line_chart(price_rating.set_index("Price"))

    elif page == "Size vs Installs":
        st.subheader("Size vs Installs")
        size_installs = spark.sql("""
            SELECT Size, SUM(Installs) AS TotalInstalls
            FROM PlayStoreAnalysis
            GROUP BY Size
            ORDER BY Size
        """).toPandas()
        st.line_chart(size_installs.set_index("Size"))

    elif page == "Genre-wise Distribution":
        st.subheader("Genre-wise Distribution")
        genre_apps = spark.sql("""
            SELECT Genres, COUNT(APP) AS AppCount
            FROM PlayStoreAnalysis
            GROUP BY Genres
            ORDER BY AppCount DESC
            LIMIT 10
        """).toPandas()
        st.bar_chart(genre_apps.set_index("Genres"))

    elif page == "Free vs Paid Apps":
        st.subheader("Free vs Paid Apps")
        free_vs_paid = spark.sql("""
            SELECT Type, COUNT(*) AS Count
            FROM PlayStoreAnalysis
            GROUP BY Type
        """).toPandas()
        st.pie_chart(free_vs_paid.set_index("Type"))

    elif page == "Largest App by size":
        st.subheader("Largest App by Size")
        largest_app = spark.sql("""
            SELECT App, Size
            FROM PlayStoreAnalysis
            ORDER BY Size DESC
            LIMIT 1
        """).toPandas()

        # Create bar chart
        fig = px.bar(largest_app, x='Size', y='App',
                     orientation='h', title='Largest App by Size')
        st.plotly_chart(fig)

    elif page == "Apps with the Most Reviews in Each Category":
        st.subheader("Apps with the Most Reviews in Each Category")
        top_reviews_per_category = spark.sql("""
            SELECT Category, App, Reviews AS MaxReviews
            FROM (
                SELECT Category, App, Reviews, ROW_NUMBER() OVER (PARTITION BY Category ORDER BY Reviews DESC) AS rank
                FROM PlayStoreAnalysis
            ) ranked
            WHERE rank = 1
        """).toPandas()

        # Create bar chart
        fig = px.bar(top_reviews_per_category, x='Category', y='MaxReviews',
                     title='Apps with the Most Reviews in Each Category')
        st.plotly_chart(fig)

    elif page == "Installs vs. Ratings Scatter Plot":
        st.subheader("Installs vs. Ratings Scatter Plot")
        installs_vs_ratings = spark.sql("""
            SELECT Installs, Rating
            FROM PlayStoreAnalysis
        """).toPandas()

        # Create scatter plot
        fig = px.scatter(installs_vs_ratings, x='Installs', y='Rating',
                         title='Installs vs. Ratings Scatter Plot')
        st.plotly_chart(fig)


# Run Streamlit app
if __name__ == "__main__":
    main()
