import glob
import os
import zipfile

import requests

from neo4j import GraphDatabase


class MovieGraphLoader:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def setup_constraints(self):
        # Create constraints to ensure unique nodes for Movie and Actor
        with self.driver.session() as session:
            session.execute_write(self.create_constraints)

    @staticmethod
    def create_constraints(tx):
        # Create unique constraint for Movie based on 'id'
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE")
        # Create unique constraint for Actor based on 'name'
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Actor) REQUIRE a.name IS UNIQUE")

    def download_and_extract_csv(self, url, output_dir):
        # Download the dataset
        print("Downloading the movie dataset from Kaggle...")
        response = requests.get(url)
        zip_file_path = os.path.join(output_dir, "archive.zip")

        with open(zip_file_path, "wb") as f:
            f.write(response.content)

        print("Unzipping the archive...")
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        os.remove(zip_file_path)  # Clean up the zip file

    def process_and_load_csvs(self, csv_directory):
        # Load all CSV files in the directory
        csv_files = glob.glob(os.path.join(csv_directory, "*.csv"))
        print(csv_files)

        for csv_file in csv_files:
            print(f"Processing and loading data from {csv_file}")
            self.load_csvs(csv_file)

    def load_csvs(self, csv_file):
        with self.driver.session() as session:
            session.execute_write(self.create_movies_and_actors, csv_file)

    @staticmethod
    def create_movies_and_actors(tx, csv_file):
        # Load Movie nodes
        load_movies_query = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{csv_file}' AS row
        MERGE (m:Movie {{id: row.id}})
        SET m.title = row.title,
            m.vote_average = toFloat(row.vote_average),
            m.vote_count = toInteger(row.vote_count),
            m.status = row.status,
            m.release_date = row.release_date,
            m.revenue = toFloat(row.revenue),
            m.runtime = toFloat(row.runtime),
            m.budget = toFloat(row.budget),
            m.imdb_id = row.imdb_id,
            m.original_language = row.original_language,
            m.original_title = row.original_title,
            m.overview = row.overview,
            m.popularity = toFloat(row.popularity),
            m.tagline = row.tagline,
            m.genres = split(row.genres, ","),
            m.poster_path = row.poster_path
        """
        tx.run(load_movies_query)

        # Load Actors and Relationships
        # load_actors_query = f"""
        # LOAD CSV WITH HEADERS FROM 'file:///{csv_file}' AS row
        # WITH row, split(row.cast, ",") AS actors
        # UNWIND actors AS actor_name
        # MERGE (a:Actor {{name: trim(actor_name)}})
        # MERGE (m:Movie {{id: row.id}})
        # MERGE (a)-[:ACTED_IN]->(m)
        # """
        # tx.run(load_actors_query)


if __name__ == "__main__":
    # Provide Neo4j connection details
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

    # Directory to download and extract CSV files
    OUTPUT_DIRECTORY = "src/neo4j/csv/chunks"  # Update to the correct path if needed
    CSV_URL = "https://www.kaggle.com/api/v1/datasets/download/alanvourch/tmdb-movies-daily-updates"

    loader = MovieGraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        loader.download_and_extract_csv(CSV_URL, OUTPUT_DIRECTORY)
        loader.setup_constraints()
        loader.process_and_load_csvs(OUTPUT_DIRECTORY)
    finally:
        loader.close()
