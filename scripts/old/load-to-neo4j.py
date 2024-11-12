from neo4j import GraphDatabase
import csv
import sys

# Function to create Neo4j nodes and relationships from a CSV file
def load_data_to_neo4j(uri, user, password, csv_file):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        # Read CSV and create nodes and relationships
        with open(csv_file, mode='r', newline='') as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                movie_data = dict(zip(headers, row))
                session.write_transaction(create_movie_node, movie_data)

    driver.close()

# Function to create a Movie node
def create_movie_node(tx, movie_data):
    query = (
        "MERGE (m:Movie {id: $id}) "
        "SET m.title = $title, m.vote_average = $vote_average, m.vote_count = $vote_count, "
        "m.status = $status, m.release_date = $release_date, m.revenue = $revenue, "
        "m.runtime = $runtime, m.budget = $budget, m.imdb_id = $imdb_id, "
        "m.original_language = $original_language, m.original_title = $original_title, "
        "m.overview = $overview, m.popularity = $popularity, m.tagline = $tagline, "
        "m.genres = $genres, m.production_companies = $production_companies, "
        "m.production_countries = $production_countries, m.spoken_languages = $spoken_languages, "
        "m.cast = $cast, m.director = $director, m.director_of_photography = $director_of_photography, "
        "m.writers = $writers, m.producers = $producers, m.music_composer = $music_composer, "
        "m.imdb_rating = $imdb_rating, m.imdb_votes = $imdb_votes, m.poster_path = $poster_path"
    )
    tx.run(query, **movie_data)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python load_to_neo4j.py <neo4j_uri> <username> <password> <input_csv_file>")
        sys.exit(1)

    neo4j_uri = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    input_csv_file = sys.argv[4]

    load_data_to_neo4j(neo4j_uri, username, password, input_csv_file)

