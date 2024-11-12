import sqlite3

# Connect to SQLite Database
conn = sqlite3.connect('movies.db')
cursor = conn.cursor()

# Drop Movies table if it exists
cursor.execute('DROP TABLE IF EXISTS Movies')

# Recreate the Movies table with all required columns
cursor.execute('''
CREATE TABLE IF NOT EXISTS Movies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    year TEXT,
    genre TEXT,
    vote_average REAL,
    vote_count INTEGER,
    status TEXT,
    release_date TEXT,
    revenue REAL,
    runtime REAL,
    budget REAL,
    imdb_id TEXT,
    original_language TEXT,
    overview TEXT,
    popularity REAL,
    tagline TEXT
);
''')

# Commit changes and close the connection
conn.commit()
conn.close()

print("Movies table dropped and recreated successfully.")

