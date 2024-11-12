import glob
import os

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
        # TODO: should it be name? normalized some kind of way?
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE")

        # Create unique constraint for Actor based on 'name'
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Actor) REQUIRE a.name IS UNIQUE")

    def load_csvs(self, csv_directory):
        # Load all CSV files in the directory
        csv_files = glob.glob(os.path.join(csv_directory, "*.csv"))
        print(csv_files)
        test_file = list(filter(lambda x: "movies_2000s.csv" in x, csv_files))[0]
        with self.driver.session() as session:
            print(f"Loading data from test file {test_file}")
            session.execute_write(self.create_movies_and_actors, test_file)
            # for csv_file in csv_files:
            #     decade = self.get_decade_from_filename(csv_file)
            #     print(f"Loading data from {csv_file} for decade: {decade}")
            #     session.execute_write(self.create_movies_and_actors, csv_file, decade)

    @staticmethod
    def get_decade_from_filename(filename):
        # Extract decade from the filename assuming it follows the pattern "movies_YYYYs.csv"
        return filename.split("_")[-1].replace("s.csv", "")

    @staticmethod
    def create_movies_and_actors(tx, csv_file):
        # Split by 'csv' and take the part after
        chunk = csv_file.split("csv", 1)[ -1 ]  # "-1" gets the part after the first occurrence
        # Remove leading slashes if needed
        chunk = chunk.lstrip("/")
        print(f"Loading data from {chunk}")
        # Load Movie nodes
        load_movies_query = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{chunk}' AS row
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
        load_actors_query = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{chunk}' AS row
        WITH row, split(row.cast, ",") AS actors
        UNWIND actors AS actor_name
        MERGE (a:Actor {{name: trim(actor_name)}})
        MERGE (m:Movie {{id: row.id}})
        MERGE (a)-[:ACTED_IN]->(m)
        """
        tx.run(load_actors_query)


if __name__ == "__main__":
    # Provide Neo4j connection details
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

    # Directory containing the CSV files
    CSV_DIRECTORY = "src/neo4j/csv/chunks"  # Update to the correct path if needed

    loader = MovieGraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        loader.setup_constraints()
        loader.load_csvs(CSV_DIRECTORY)
    finally:
        loader.close()
