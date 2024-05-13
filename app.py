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

    st.sidebar.subheader("Navigation")
    page = st.sidebar.radio("Go to", ["Home", "Top 10 Reviewed Apps", "Category-wise Distribution", "Most Installed Apps",
                                      "App Versions Comparison", "Average Rating by Category", "Top 10 Paid Apps",
                                      "Content Rating Distribution", "Price vs Rating", "Size vs Installs",
                                      "Genre-wise Distribution", "Free vs Paid Apps", "Largest App by size",
                                      "Apps with the Most Reviews in Each Category", "Installs vs. Ratings Scatter Plot", "Apps Sizes and Average Sizes by Category",
                                      "Top 10 Categories by Number of Apps", "Distribution of Ratings", "Price Distribution",
                                      "Box Plot of Ratings by Content Rating", "Price vs Rating by Category",
                                      "Installs Distribution", "Box Plot of Ratings by Type", "Size Distribution",
                                      "Content Rating Pie Chart"])

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
        cleaned_data = spark.sql("""
            SELECT Category, CAST(Rating AS FLOAT) AS Rating
            FROM PlayStoreAnalysis
            WHERE Rating IS NOT NULL
            AND Rating != ''
            AND Rating RLIKE '^[0-9]+\\.?[0-9]*$'
            AND Category != '1.9' -- Remove the category '1.9'
        """)
        category_avg_rating = cleaned_data.groupBy('Category').avg('Rating').orderBy('avg(Rating)', ascending=False).toPandas()
        st.bar_chart(category_avg_rating.set_index("Category"))

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
        
        fig = px.pie(free_vs_paid, values='Count', names='Type', title='Free vs Paid Apps', hole=0.4)
        st.plotly_chart(fig)


    elif page == "Largest App by size":
        st.subheader("Top 10 Largest Apps by Size")
        largest_apps = spark.sql("""
            SELECT App, Size,
                REGEXP_REPLACE(Size, '[^A-Za-z]', '') AS Unit
            FROM (
                SELECT App,
                    CAST(REGEXP_REPLACE(Size, '[A-Za-z]+', '') AS FLOAT) AS Size
                FROM PlayStoreAnalysis
            ) t
            ORDER BY Size DESC
            LIMIT 10
        """).toPandas()

        # Concatenate size with unit
        largest_apps['Size'] = largest_apps.apply(lambda row: f"{row['Size']} {row['Unit']}", axis=1)

        # Adjust the index to start from 1
        largest_apps.index += 1

        # Display the list of largest apps with size unit
        st.table(largest_apps.drop(columns='Unit'))


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

    elif page == "Apps Sizes and Average Sizes by Category":
        st.subheader("Apps Sizes and Average Sizes by Category")
        apps_sizes_avg_category_sizes = spark.sql("""
            SELECT Category, AVG(CAST(REGEXP_REPLACE(Size, '[A-Za-z]+', '') AS FLOAT)) AS AvgCategorySize
            FROM PlayStoreAnalysis
            GROUP BY Category
            ORDER BY AvgCategorySize DESC
        """).toPandas()
        st.line_chart(apps_sizes_avg_category_sizes.set_index("Category"))

    elif page == "Top 10 Categories by Number of Apps":
        st.subheader("Top 10 Categories by Number of Apps")
        top_categories = spark.sql("""
            SELECT Category, COUNT(*) AS AppCount
            FROM PlayStoreAnalysis
            GROUP BY Category
            ORDER BY AppCount DESC
            LIMIT 10
        """).toPandas()
        st.plotly_chart(px.pie(top_categories, values='AppCount', names='Category', title='Top 10 Categories by Number of Apps'))

    elif page == "Distribution of Ratings":
        st.subheader("Distribution of Ratings")
        rating_distribution = spark.sql("""
            SELECT Rating
            FROM PlayStoreAnalysis
            WHERE Rating IS NOT NULL AND Rating != ''
            AND Rating RLIKE '^[0-9]+\\.?[0-9]*$'
        """).toPandas()
        st.plotly_chart(px.histogram(rating_distribution, x='Rating', title='Distribution of Ratings'))

    elif page == "Price Distribution":
        st.subheader("Price Distribution")
        price_distribution = spark.sql("""
            SELECT Price
            FROM PlayStoreAnalysis
            WHERE Price > 0
        """).toPandas()
        st.plotly_chart(px.histogram(price_distribution, x='Price', title='Price Distribution'))

    elif page == "Box Plot of Ratings by Content Rating":
        st.subheader("Box Plot of Ratings by Content Rating")
        ratings_by_content_rating = spark.sql("""
            SELECT `Content Rating`, CAST(Rating AS FLOAT) AS Rating
            FROM PlayStoreAnalysis
            WHERE Rating IS NOT NULL AND Rating != ''
            AND Rating RLIKE '^[0-9]+\\.?[0-9]*$'
        """)
        st.plotly_chart(px.box(ratings_by_content_rating.toPandas(), x='Content Rating', y='Rating', title='Box Plot of Ratings by Content Rating'))

    elif page == "Price vs Rating by Category":
        st.subheader("Price vs Rating by Category")
        price_rating_category = spark.sql("""
            SELECT Category, Price, AVG(Rating) AS AvgRating
            FROM PlayStoreAnalysis
            WHERE Price > 0
            GROUP BY Category, Price
        """)
        st.plotly_chart(px.scatter(price_rating_category.toPandas(), x='Price', y='AvgRating', color='Category', title='Price vs Rating by Category'))

    elif page == "Installs Distribution":
        st.subheader("Installs Distribution")
        installs_distribution = spark.sql("""
            SELECT Installs
            FROM PlayStoreAnalysis
        """).toPandas()
        st.plotly_chart(px.histogram(installs_distribution, x='Installs', title='Installs Distribution'))

    elif page == "Box Plot of Ratings by Type":
        st.subheader("Box Plot of Ratings by Type")
        ratings_by_type = spark.sql("""
            SELECT Type, CAST(Rating AS FLOAT) AS Rating
            FROM PlayStoreAnalysis
            WHERE Rating IS NOT NULL AND Rating != ''
            AND Rating RLIKE '^[0-9]+\\.?[0-9]*$'
        """)
        st.plotly_chart(px.box(ratings_by_type.toPandas(), x='Type', y='Rating', title='Box Plot of Ratings by Type'))

    elif page == "Size Distribution":
        st.subheader("Size Distribution")
        size_distribution = spark.sql("""
            SELECT Size
            FROM PlayStoreAnalysis
        """).toPandas()
        st.plotly_chart(px.histogram(size_distribution, x='Size', title='Size Distribution'))

    elif page == "Content Rating Pie Chart":
        st.subheader("Content Rating Pie Chart")
        content_rating_distribution = spark.sql("""
            SELECT `Content Rating`, COUNT(*) AS Count
            FROM PlayStoreAnalysis
            GROUP BY `Content Rating`
        """)
        st.plotly_chart(px.pie(content_rating_distribution.toPandas(), values='Count', names='Content Rating', title='Content Rating Distribution'))

    elif page == "Average Rating by Type":
        st.subheader("Average Rating by Type")
        avg_rating_by_type = spark.sql("""
            SELECT Type, AVG(Rating) AS AvgRating
            FROM PlayStoreAnalysis
            WHERE Rating IS NOT NULL AND Rating != ''
            AND Rating RLIKE '^[0-9]+\\.?[0-9]*$'
            GROUP BY Type
        """)
        st.plotly_chart(px.bar(avg_rating_by_type.toPandas(), x='Type', y='AvgRating', title='Average Rating by Type'))

# Run Streamlit app
if __name__ == "__main__":
    main()
