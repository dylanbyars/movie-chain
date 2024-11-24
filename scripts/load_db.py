import argparse
import csv
import glob
import logging
import os
import zipfile
from typing import List, Optional

import requests
from pydantic import BaseModel

from neo4j import GraphDatabase

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Define the Pydantic model to represent each row of the CSV file
class RawMovieData(BaseModel):
    id: int
    title: str
    vote_average: float
    vote_count: float
    status: str
    release_date: str
    revenue: float
    runtime: Optional[float]  # runtime can be optional
    budget: Optional[float]  # budget can be optional
    imdb_id: str
    original_language: str
    original_title: str
    overview: Optional[str]
    popularity: float
    tagline: Optional[str]
    genres: List[str]  # genres can be a list of strings
    production_companies: List[str]  # production companies can be a list of strings
    production_countries: List[str]  # production countries can be a list of strings
    spoken_languages: List[str]  # spoken languages can be a list of strings
    cast: List[str]  # cast can be a list of strings
    director: str
    director_of_photography: Optional[str]
    writers: List[str]  # writers can be a list of strings
    producers: List[str]  # producers can be a list of strings
    music_composer: Optional[str]
    imdb_rating: float
    imdb_votes: float
    poster_path: Optional[str]


class MovieGraphLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def setup_constraints(self):
        logging.info("Setting up constraints in the database.")
        with self.driver.session() as session:
            session.execute_write(self.create_constraints)

    @staticmethod
    def create_constraints(tx):
        logging.info("Creating constraints for Movie and Actor nodes.")
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE")
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Actor) REQUIRE a.name IS UNIQUE")

    def download_csv(self, url, output_dir):
        logging.info("Downloading the dataset from Kaggle...")
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(
                f"Failed to download file. HTTP Status Code: {response.status_code}"
            )
            raise Exception(
                f"Failed to download file. HTTP Status Code: {response.status_code}"
            )

        zip_file_path = os.path.join(output_dir, "archive.zip")

        with open(zip_file_path, "wb") as f:
            f.write(response.content)
        logging.info("Download complete.")

    def extract_csv(self, output_dir):
        zip_file_path = os.path.join(output_dir, "archive.zip")
        if not os.path.exists(zip_file_path):
            logging.error("Zip file does not exist for extraction.")
            raise FileNotFoundError("Zip file does not exist for extraction.")

        logging.info("Unzipping the archive...")
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)
        os.remove(zip_file_path)  # Clean up the zip file
        logging.info("Extraction complete.")

    def process_and_load_csvs(self, csv_directory):
        logging.info("Loading all CSV files in the directory.")
        csv_files = glob.glob(os.path.join(csv_directory, "*.csv"))
        if not csv_files:
            logging.error("No CSV files found in the directory.")
            raise FileNotFoundError("No CSV files found in the directory.")

        logging.info(f"Found CSV files: {csv_files}")

        for csv_file in csv_files:
            logging.info(f"Processing and loading data from {csv_file}")
            self.load_csvs(csv_file)

    def load_csvs(self, csv_file):
        with self.driver.session() as session:
            session.execute_write(self.create_movies_and_actors, csv_file)

    @staticmethod
    def create_movies_and_actors(tx, csv_file):
        logging.info(f"Loading movies from {csv_file}.")
        load_movies_query = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{csv_file}' AS row
        MERGE (m:Movie {{id: row.id}})
        SET m.title = row.title,
            m.vote_average = toFloat(row.vote_average),
            m.vote_count = toInteger(row.vote_count),
            m.status = row.status,
            m.release_date = row.release_date,
            m.revenue = toFloat(row.revenue),
            m.runtime = CASE row.runtime WHEN '' THEN null ELSE toFloat(row.runtime) END,
            m.budget = CASE row.budget WHEN '' THEN null ELSE toFloat(row.budget) END,
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

    def chunk_csv_by_decade(self, csv_directory):
        logging.info("Chunking data by decade.")
        MEGA_FILE = os.path.join(csv_directory, "TMDB_all_movies.csv")
        csv_files = glob.glob(os.path.join(csv_directory, "*.csv"))
        if not csv_files:
            logging.error("No CSV files found for chunking.")
            raise FileNotFoundError("No CSV files found for chunking.")

        for csv_file in csv_files:
            logging.info(f"Chunking data from {csv_file} by decade")
            decade_chunks = {}

            with open(csv_file, mode="r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    release_year = row["release_date"][
                        :4
                    ]  # Extract the year from the release date
                    if release_year.isdigit():
                        decade = str(
                            int(release_year) // 10 * 10
                        )  # Calculate the decade
                        if decade not in decade_chunks:
                            decade_chunks[decade] = []
                        decade_chunks[decade].append(row)

            for decade, rows in decade_chunks.items():
                output_file = os.path.join(csv_directory, f"{decade}s_movies.csv")
                with open(output_file, mode="w", newline="", encoding="utf-8") as out_f:
                    writer = csv.DictWriter(out_f, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                logging.info(f"Created {output_file} with {len(rows)} records.")

    def parse_csv_with_pydantic(self, csv_file: str) -> List[RawMovieData]:
        validated_movies = []

        with open(csv_file, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    # Create a RawMovieData instance, filling in defaults for missing fields
                    movie_data = RawMovieData(
                        id=int(row.get("id")),
                        title=row.get("title", "Unknown Title"),
                        vote_average=float(row.get("vote_average", 0.0)),
                        vote_count=float(row.get("vote_count", 0)),
                        status=row.get("status", "Unknown"),
                        release_date=row.get("release_date", "Unknown"),
                        revenue=float(row.get("revenue", 0.0)),
                        runtime=(
                            float(row.get("runtime", 0.0))
                            if row.get("runtime")
                            else None
                        ),
                        budget=(
                            float(row.get("budget", 0.0)) if row.get("budget") else None
                        ),
                        imdb_id=row.get("imdb_id", ""),
                        original_language=row.get("original_language", "en"),
                        original_title=row.get("original_title", "Unknown"),
                        overview=row.get("overview", ""),
                        popularity=float(row.get("popularity", 0.0)),
                        tagline=row.get("tagline", ""),
                        genres=(
                            row.get("genres", "").split(",")
                            if row.get("genres")
                            else []
                        ),
                        production_companies=(
                            row.get("production_companies", "").split(",")
                            if row.get("production_companies")
                            else []
                        ),
                        production_countries=(
                            row.get("production_countries", "").split(",")
                            if row.get("production_countries")
                            else []
                        ),
                        spoken_languages=(
                            row.get("spoken_languages", "").split(",")
                            if row.get("spoken_languages")
                            else []
                        ),
                        cast=row.get("cast", "").split(",") if row.get("cast") else [],
                        director=row.get("director", "Unknown"),
                        director_of_photography=row.get(
                            "director_of_photography", None
                        ),
                        writers=(
                            row.get("writers", "").split(",")
                            if row.get("writers")
                            else []
                        ),
                        producers=(
                            row.get("producers", "").split(",")
                            if row.get("producers")
                            else []
                        ),
                        music_composer=row.get("music_composer", None),
                        imdb_rating=float(row.get("imdb_rating", 0.0)),
                        imdb_votes=float(row.get("imdb_votes", 0)),
                        poster_path=row.get("poster_path", None),
                    )
                    validated_movies.append(movie_data)
                except Exception as e:
                    logging.error(f"Error processing row {row}: {e}")

        return validated_movies


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Load movie data into Neo4j.")
    parser.add_argument(
        "operations",
        nargs="+",
        type=str,
        help="Specify one or more operations to perform, separated by spaces.",
    )
    args = parser.parse_args()

    # Provide Neo4j connection details
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

    # Directory to download and extract CSV files
    OUTPUT_DIRECTORY = "src/neo4j/csv/chunks"  # Update to the correct path if needed
    CSV_URL = "https://www.kaggle.com/api/v1/datasets/download/alanvourch/tmdb-movies-daily-updates"

    loader = MovieGraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    try:
        if "download" in args.operations:
            loader.download_csv(CSV_URL, OUTPUT_DIRECTORY)

        if "extract" in args.operations:
            loader.extract_csv(OUTPUT_DIRECTORY)

        if "chunk" in args.operations:
            loader.chunk_csv_by_decade(OUTPUT_DIRECTORY)

        if "load" in args.operations:
            loader.setup_constraints()
            loader.process_and_load_csvs(OUTPUT_DIRECTORY)

    finally:
        loader.close()
