import argparse
import csv
import glob
import logging
import os
import sys
import traceback
import zipfile
from typing import List, Optional

import requests
from pydantic import BaseModel, ValidationError

from neo4j import GraphDatabase

# Set up enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("movie_loader_errors.log"),
    ],
)


class ValidationException(Exception):
    """Custom exception for validation errors with detailed context"""

    def __init__(self, message: str, row_data: dict, original_error: Exception):
        self.message = message
        self.row_data = row_data
        self.original_error = original_error
        super().__init__(self.message)


class RawMovieData(BaseModel):
    id: int
    title: str
    vote_average: float
    vote_count: float
    status: str
    release_date: str
    revenue: float
    runtime: Optional[float]
    budget: Optional[float]
    imdb_id: str
    original_language: str
    original_title: str
    overview: Optional[str]
    popularity: float
    tagline: Optional[str]
    genres: List[str]
    production_companies: List[str]
    production_countries: List[str]
    spoken_languages: List[str]
    cast: List[str]
    director: str
    director_of_photography: Optional[str]
    writers: List[str]
    producers: List[str]
    music_composer: Optional[str]
    imdb_rating: float
    imdb_votes: float
    poster_path: Optional[str]


class MovieGraphLoader:
    # TODO: I don't need to connect to the db all the time. sometimes I'm just processing data
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def setup_constraints(self):
        logging.info("Setting up constraints in the database.")
        with self.driver.session() as session:
            session.run(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE"
            )
            session.run(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Actor) REQUIRE a.name IS UNIQUE"
            )

    def download_csv(self, url, output_dir):
        logging.info("Downloading the dataset from Kaggle...")
        try:
            response = requests.get(url)
            response.raise_for_status()  # Will raise an HTTPError for bad responses
        except requests.exceptions.RequestException as e:
            logging.critical(f"Failed to download dataset: {str(e)}")
            raise

        zip_file_path = os.path.join(output_dir, "archive.zip")

        try:
            with open(zip_file_path, "wb") as f:
                f.write(response.content)
            logging.info("Download complete.")
        except IOError as e:
            logging.critical(f"Failed to write zip file: {str(e)}")
            raise

    def extract_csv(self, output_dir):
        zip_file_path = os.path.join(output_dir, "archive.zip")
        if not os.path.exists(zip_file_path):
            error_msg = "Zip file does not exist for extraction."
            logging.critical(error_msg)
            raise FileNotFoundError(error_msg)

        logging.info("Unzipping the archive...")
        try:
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(output_dir)
            os.remove(zip_file_path)
            logging.info("Extraction complete.")
        except (zipfile.BadZipFile, IOError) as e:
            logging.critical(f"Failed to extract zip file: {str(e)}")
            raise

    def chunk_csv_by_decade(self, csv_directory):
        logging.info("Starting chunking process by decade.")
        MEGA_FILE = os.path.join(csv_directory, "TMDB_all_movies.csv")

        if not os.path.exists(MEGA_FILE):
            error_msg = f"Source file not found: {MEGA_FILE}"
            logging.critical(error_msg)
            raise FileNotFoundError(error_msg)

        logging.info(f"Processing data from {MEGA_FILE}")
        decade_chunks = {}

        try:
            # Validate and parse data with Pydantic before chunking
            validated_movies = self.parse_csv_with_pydantic(MEGA_FILE)

            # Process validated movies into decade chunks
            for movie in validated_movies:
                release_year = movie.release_date[:4]
                if release_year.isdigit():
                    decade = str(int(release_year) // 10 * 10)
                    if decade not in decade_chunks:
                        decade_chunks[decade] = []
                    decade_chunks[decade].append(movie.dict())
                else:
                    logging.error(
                        f"Invalid release year for movie {movie.title}: {movie.release_date}"
                    )

            # Write decade chunks to files
            self._write_decade_chunks(decade_chunks, csv_directory)

        except Exception as e:
            logging.critical(f"Failed during chunking process: {str(e)}")
            raise

    def _write_decade_chunks(self, decade_chunks: dict, csv_directory: str):
        """Write the chunked data to separate files by decade"""
        for decade, rows in decade_chunks.items():
            output_file = os.path.join(csv_directory, f"{decade}s_movies.csv")
            try:
                with open(output_file, mode="w", newline="", encoding="utf-8") as out_f:
                    writer = csv.DictWriter(
                        out_f, fieldnames=RawMovieData.__fields__.keys()
                    )
                    writer.writeheader()
                    writer.writerows(rows)
                logging.info(f"Created {output_file} with {len(rows)} records.")
            except IOError as e:
                logging.critical(f"Failed to write chunk file {output_file}: {str(e)}")
                raise

    def parse_csv_with_pydantic(self, csv_file: str) -> List[RawMovieData]:
        validated_movies = []
        total_rows = (
            sum(1 for _ in open(csv_file, encoding="utf-8")) - 1
        )  # Subtract header

        with open(csv_file, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, start=1):
                try:
                    # Pre-validation data cleaning
                    cleaned_row = self._clean_row_data(row)

                    # Attempt to create RawMovieData instance
                    movie_data = RawMovieData(**cleaned_row)
                    validated_movies.append(movie_data)

                    # Log progress
                    if row_num % 1000 == 0:
                        logging.info(
                            f"Processed {row_num}/{total_rows} rows successfully"
                        )

                except ValidationError as ve:
                    error_msg = self._format_validation_error(ve, row, row_num)
                    logging.critical(error_msg)
                    raise ValidationException(
                        f"Validation error in row {row_num}", row, ve
                    )
                except Exception as e:
                    error_msg = self._format_general_error(e, row, row_num)
                    logging.critical(error_msg)
                    raise ValidationException(
                        f"Unexpected error in row {row_num}", row, e
                    )

        return validated_movies

    def _clean_row_data(self, row: dict) -> dict:
        """Clean and prepare row data before validation"""
        cleaned = row.copy()

        # Log suspicious values
        self._log_suspicious_values(cleaned)

        # Clean numerical fields
        numerical_fields = [
            "vote_average",
            "vote_count",
            "revenue",
            "runtime",
            "budget",
            "popularity",
            "imdb_rating",
            "imdb_votes",
        ]

        for field in numerical_fields:
            if field in cleaned:
                cleaned[field] = self._clean_numerical_value(field, cleaned[field])

        # Clean list fields
        list_fields = [
            "genres",
            "production_companies",
            "production_countries",
            "spoken_languages",
            "cast",
            "writers",
            "producers",
        ]

        for field in list_fields:
            if field in cleaned:
                cleaned[field] = self._clean_list_value(field, cleaned[field])

        return cleaned

    def _clean_numerical_value(self, field: str, value: str) -> float:
        """Clean and convert numerical values"""
        if not value or value.strip() == "":
            if field in ["runtime", "budget"]:
                return None
            return 0.0

        try:
            cleaned_value = float(value.replace(",", "").strip())
            if cleaned_value < 0:
                logging.warning(f"Negative value found in {field}: {value}")
                return 0.0
            return cleaned_value
        except ValueError:
            logging.error(f"Invalid numerical value in {field}: {value}")
            return 0.0

    def _clean_list_value(self, field: str, value: str) -> List[str]:
        """Clean and convert list values"""
        if not value or value.strip() == "":
            return []

        items = [item.strip() for item in value.split(",") if item.strip()]
        if not items:
            logging.warning(f"Empty list found in {field}: {value}")
        return items

    def _log_suspicious_values(self, row: dict) -> None:
        """Log suspicious or potentially problematic values"""
        if float(row.get("vote_average", 0) or 0) > 10:
            logging.warning(
                f"Suspicious vote_average: {row['vote_average']} for movie {row.get('title')}"
            )

        if float(row.get("runtime", 0) or 0) > 500:
            logging.warning(
                f"Suspicious runtime: {row['runtime']} for movie {row.get('title')}"
            )

        if row.get("release_date"):
            if not self._is_valid_date(row["release_date"]):
                logging.warning(
                    f"Suspicious release_date: {row['release_date']} for movie {row.get('title')}"
                )

    def _is_valid_date(self, date_str: str) -> bool:
        """Validate date format and range"""
        try:
            year = int(date_str[:4])
            return 1880 <= year <= 2025
        except ValueError:
            return False

    def _format_validation_error(
        self, ve: ValidationError, row: dict, row_num: int
    ) -> str:
        """Format validation error details"""
        error_details = []
        error_details.append(f"\n{'='*80}")
        error_details.append(f"VALIDATION ERROR IN ROW {row_num}")
        error_details.append(f"{'='*80}")
        error_details.append("\nError Details:")

        for error in ve.errors():
            error_details.append(f"\nField: {' -> '.join(error['loc'])}")
            error_details.append(f"Error: {error['msg']}")
            error_details.append(f"Type: {error['type']}")

        error_details.append("\nRow Data:")
        for key, value in row.items():
            error_details.append(f"{key}: {value}")

        error_details.append(f"\n{'='*80}\n")
        return "\n".join(error_details)

    def _format_general_error(self, e: Exception, row: dict, row_num: int) -> str:
        """Format general error details"""
        error_details = []
        error_details.append(f"\n{'='*80}")
        error_details.append(f"GENERAL ERROR IN ROW {row_num}")
        error_details.append(f"{'='*80}")
        error_details.append(f"\nError Type: {type(e).__name__}")
        error_details.append(f"Error Message: {str(e)}")
        error_details.append("\nTraceback:")
        error_details.append(traceback.format_exc())
        error_details.append("\nRow Data:")
        for key, value in row.items():
            error_details.append(f"{key}: {value}")
        error_details.append(f"\n{'='*80}\n")
        return "\n".join(error_details)

    def process_and_load_csvs(self, csv_directory):
        logging.info("Loading all CSV files in the directory.")
        csv_files = glob.glob(os.path.join(csv_directory, "[0-9]*0s_movies.csv"))
        if not csv_files:
            error_msg = "No CSV files found in the directory."
            logging.critical(error_msg)
            raise FileNotFoundError(error_msg)

        logging.info(f"Found CSV files: {'\n'.join(csv_files)}")

        for csv_file in csv_files:
            logging.info(f"Processing and loading data from {csv_file}")
            self.load_csvs(csv_file)

    def load_csvs(self, csv_file):
        with self.driver.session() as session:
            session.execute_write(self.create_movies_and_actors, csv_file)

    @staticmethod
    def create_movies_and_actors(tx, csv_file):
        filename = os.path.basename(csv_file)
        logging.info(f"Loading movies from {filename}.")

        BATCH_SIZE = 100

        # Define the query once
        query = """
        UNWIND $rows AS row
        MERGE (m:Movie {id: row.id})
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
        
        // Create actors and relationships
        WITH m, row
        UNWIND split(row.cast, ',') AS actor_name
        WITH m, trim(actor_name) AS actor_name
        WHERE actor_name <> ''
        MERGE (a:Actor {name: actor_name})
        MERGE (a)-[:ACTED_IN]->(m)
        """

        with open(
            os.path.join("src/neo4j/csv/chunks", filename), "r", encoding="utf-8"
        ) as f:
            reader = csv.DictReader(f)
            batch = []

            for i, row in enumerate(reader, 1):
                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    try:
                        tx.run(query, rows=batch)
                        logging.info(f"Processed {i} rows from {filename}")
                    except Exception as e:
                        logging.error(
                            f"Failed to process batch ending at row {i} in {filename}: {str(e)}"
                        )
                    finally:
                        batch = []

            # Process remaining rows
            if batch:
                try:
                    tx.run(query, rows=batch)
                except Exception as e:
                    logging.error(
                        f"Failed to process final batch in {filename}: {str(e)}"
                    )


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Load movie data into Neo4j.")
    parser.add_argument(
        "operations",
        nargs="+",
        type=str,
        help="Specify one or more operations to perform: download, extract, chunk, load",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing when non-critical errors occur",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="movie_loader_errors.log",
        help="Specify the log file path (default: movie_loader_errors.log)",
    )
    args = parser.parse_args()

    # Validate operations
    valid_operations = {"download", "extract", "chunk", "load"}
    invalid_ops = set(args.operations) - valid_operations
    if invalid_ops:
        logging.critical(f"Invalid operations specified: {invalid_ops}")
        sys.exit(1)

    # Provide Neo4j connection details
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

    # Directory to download and extract CSV files
    OUTPUT_DIRECTORY = os.getenv("OUTPUT_DIRECTORY", "src/neo4j/csv/chunks")
    CSV_URL = os.getenv(
        "CSV_URL",
        "https://www.kaggle.com/api/v1/datasets/download/alanvourch/tmdb-movies-daily-updates",
    )

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    # Initialize loader
    loader = None
    try:
        loader = MovieGraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

        for operation in args.operations:
            try:
                logging.info(f"Starting operation: {operation}")

                if operation == "download":
                    loader.download_csv(CSV_URL, OUTPUT_DIRECTORY)

                elif operation == "extract":
                    loader.extract_csv(OUTPUT_DIRECTORY)

                elif operation == "chunk":
                    loader.chunk_csv_by_decade(OUTPUT_DIRECTORY)

                elif operation == "load":
                    loader.setup_constraints()
                    loader.process_and_load_csvs(OUTPUT_DIRECTORY)

                logging.info(f"Completed operation: {operation}")

            except ValidationException as ve:
                logging.critical(f"Validation error during {operation}: {ve.message}")
                logging.critical("Original error: %s", str(ve.original_error))
                if not args.continue_on_error:
                    sys.exit(1)

            except Exception as e:
                logging.critical(f"Error during {operation}: {str(e)}")
                logging.critical("Traceback: %s", traceback.format_exc())
                if not args.continue_on_error:
                    sys.exit(1)

    except Exception as e:
        logging.critical(f"Fatal error during initialization: {str(e)}")
        logging.critical("Traceback: %s", traceback.format_exc())
        sys.exit(1)

    finally:
        if loader:
            try:
                loader.close()
                logging.info("Successfully closed Neo4j connection")
            except Exception as e:
                logging.error(f"Error while closing Neo4j connection: {str(e)}")

    logging.info("Movie data processing completed successfully")
