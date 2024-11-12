import os
from contextlib import asynccontextmanager
from pprint import pprint
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.responses import HTMLResponse

from neo4j import GraphDatabase
from neo4j import Query as Neo4jQuery

# --- Pydantic Models ---


# PathStep model to represent each step in the movie path.
class PathStep(BaseModel):
    movie_title: str
    actor_name: str
    next_movie_title: Optional[str] = None


class Actor(BaseModel):
    name: str


class Movie(BaseModel):
    title: Optional[str] = None
    year: Optional[int] = None  # Can be derived from release_date if needed
    imdb_id: Optional[str] = None
    overview: Optional[str] = None
    original_language: Optional[str] = None
    runtime: Optional[float] = None
    poster_path: Optional[str] = None
    revenue: Optional[float] = None
    release_date: Optional[str] = None
    genres: Optional[List[str]] = None
    popularity: Optional[float] = None
    vote_average: Optional[float] = None
    tagline: Optional[str] = None
    vote_count: Optional[int] = None
    budget: Optional[float] = None
    status: Optional[str] = None
    actors: Optional[List[Actor]] = []  # New field for associated actors


# --- Neo4j Client ---
class Neo4jClient:
    def __init__(self):
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")
        if not uri or not user or not password:
            raise ValueError("Missing required environment variables for Neo4j")

        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def search_suggestions(self, search_term: str, limit: int = 15) -> List[dict]:
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (n:Movie)
                WHERE toLower(n.title) CONTAINS toLower($search_term)
                RETURN n.title AS title, n.release_date as release_date, n.overview AS overview
                ORDER BY n.popularity DESC, n.title ASC
                LIMIT $limit
                """,
                search_term=search_term,
                limit=limit,
            )
            suggestions = []
            for record in result:
                suggestions.append(
                    {
                        "title": record["title"],
                        "release_date": record.get("release_date"),
                        "overview": record.get("overview"),
                    }
                )
            return suggestions

    def find_movie_paths(self, start_movie_title: str, min_movies=2, max_movies=3):
        # NOTE: the size of the path is fixed for now
        base_query = """
            MATCH path = (start:Movie {title: $start_movie_title})
                         -[:ACTED_IN|ACTED_IN*1..4]-(connected:Movie)
            WHERE start <> connected
              AND ALL(n IN nodes(path) WHERE 
                (n:Movie AND size([x IN nodes(path) WHERE x:Movie AND x = n]) = 1) OR 
                (n:Actor AND size([x IN nodes(path) WHERE x:Actor AND x = n]) = 1)
              )
            WITH path, connected, rand() AS random_order
            WHERE size([n IN nodes(path) WHERE n:Movie]) > 1
            WITH COLLECT({
              path: path,
              random: random_order
            }) AS paths
            UNWIND paths AS p
            WITH p.path AS path
            ORDER BY size(nodes(path)), rand()
            RETURN DISTINCT path
            LIMIT 10
        """

        with self.driver.session() as session:
            print("Running query")
            result = session.run(
                base_query, start_movie_title=start_movie_title, result_limit=5
            )
            # print(result.data())
            print("Query complete")

            return result.data()

    # def get_custom_path(self, start_name: str, path_size: int) -> List[PathStep]:
    #     with self.driver.session() as session:
    #         # Modified query to select a path based on user-specified start point and path length
    #         # BUG: query doesn't do what I want
    #         result = session.run(
    #             """
    #             MATCH path = (start:Movie)
    #             WITH start
    #             MATCH path = (start)<-[:ACTED_IN|ACTED_IN*1..8]-(end)
    #             WHERE all(n IN nodes(path) WHERE (n:Movie OR n:Actor))
    #               AND all(i IN range(0, size(nodes(path))-2)
    #                   WHERE NOT (nodes(path)[i]:Movie AND nodes(path)[i+1]:Movie)
    #                   AND NOT (nodes(path)[i]:Actor AND nodes(path)[i+1]:Actor))
    #             RETURN path
    #             LIMIT 5
    #             """,
    #             start_name=start_name,
    #             path_size=path_size,
    #         )
    #
    #         print(result.data())
    #
    #         path_steps = []
    #         for record in result:
    #             path = record["path"]
    #             nodes = path.nodes
    #             relationships = path.relationships
    #
    #             for i in range(len(nodes) - 1):
    #                 start_node = nodes[i]
    #                 end_node = nodes[i + 1]
    #
    #                 if "Movie" in start_node.labels and "Actor" in end_node.labels:
    #                     path_steps.append(
    #                         PathStep(
    #                             movie_title=start_node["title"],
    #                             actor_name=end_node["name"],
    #                             next_movie_title=(
    #                                 end_node["title"] if i + 1 < len(nodes) else None
    #                             ),
    #                         )
    #                     )
    #                 elif "Actor" in start_node.labels and "Movie" in end_node.labels:
    #                     path_steps.append(
    #                         PathStep(
    #                             movie_title=end_node["title"],
    #                             actor_name=start_node["name"],
    #                             next_movie_title=(
    #                                 end_node["title"] if i + 1 < len(nodes) else None
    #                             ),
    #                         )
    #                     )
    #
    #         return path_steps


# --- Utility Functions ---


def transform_paths_data(data):
    result = []

    for item in data:
        path = item.get("path", [])
        if not path:
            continue

        # Create a dict to hold movie and its connections
        movie_dict = {
            "movie": {
                "title": path[0].get("title"),
                "release_date": path[0].get("release_date"),
                "genres": path[0].get("genres"),
                "overview": path[0].get("overview"),
                "poster_path": path[0].get("poster_path"),
            },
            "connections": [],
        }

        # Iterate over path to extract relationships and linked movies or actors
        for i in range(1, len(path), 2):
            relationship = path[i]
            next_node = path[i + 1]

            # Handle actor relationships (e.g., "ACTED_IN")
            if relationship == "ACTED_IN" and "name" in next_node:
                movie_dict["connections"].append(
                    {"relationship": "ACTED_IN", "actor": {"name": next_node["name"]}}
                )
            # Handle other movie connections
            elif "title" in next_node:
                movie_dict["connections"].append(
                    {
                        "relationship": relationship,
                        "movie": {
                            "title": next_node["title"],
                            "release_date": next_node.get("release_date"),
                            "genres": next_node.get("genres"),
                            "overview": next_node.get("overview"),
                            "poster_path": next_node.get("poster_path"),
                        },
                    }
                )

        # Append to result
        result.append(movie_dict)

    return result


# --- FastAPI App ---


app = FastAPI(title="MovieChain API")
app.mount("/static", StaticFiles(directory="src/app/static"), name="static")
db = Neo4jClient()


templates = Jinja2Templates(directory="src/app/templates")


# Route for serving the main page
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/movies")
def get_movies(
    start_name: str = Query(..., min_length=1), path_size: int = Query(1, ge=1)
):
    try:
        print(f"Getting path for {start_name} with length {path_size}")
        response = db.find_movie_paths(start_name, path_size)
        return response
        payload = transform_paths_data(response)
        pprint(f"Payload: {payload}")
        return payload
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/suggestions")
def get_suggestions(query: str = Query(..., min_length=2), limit: int = 15):
    try:
        return {"suggestions": db.search_suggestions(query, limit)}
    except Exception as e:
        return {"error": str(e)}


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    db.close()
