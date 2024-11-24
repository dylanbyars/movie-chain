# What is this?

`movie-chain` is a webapp that shows a graph of movies originating from a single movie and moving away by edges of actors that are in each movie.

![Screenshot of the webapp](docs/screenshot.png)

Starting from a single movie, a graph is made going from the starting movie to a set of movies that have some actor from the first movie. Then each of those movies is connected to another set of movies by some other actor. And so on. The hard limit of path size is 3 so 1 starting movie going out to a bunch of related movies connected by some set of actors from the starting movie.

# How to run

1. start the containers to start the neo4j db and the fastapi server:
```sh
docker compose up --build --detach
```
2. load the csvs in the neo4j db. This will download the csvs in a zip, extract the zip to a fil, chunk that file into a few files by decase, and load them into the db. It takes less than 10 minutes to run right now:
```sh
uv run ./scripts/load_db.py download extract chunk load
```
3. check the logs of the fastapi container to see the local address where the server is running (should be http://0.0.0.0:8000)
4. enter a movie title and press `Find Path` to see

# Dev Tips

The community edition of Neo4j only allows 1 db. To make new dbs for testing, update the `volumes:` line of the `docker-compose.yaml` file and map a new empty `./data__foo` dir to `/data` in the neo4j container.

# TODO:

- [ ] improve rendering of graph
- [ ] document how to clean the movie csv 
- [ ] document how to load the db with the movie chunks
- [ ] use some other metadata about the movies and actors to augment the kinds of paths that are returned. 
- [ ] get actor data + clean it + ingest it
- [X] script the csv loading process more
- [ ] make path size configurable
- [ ] add more search criteria
