"""
Name: Tristan Hilbert
Date: 11/5/2023
Filename: insert_graph.py
Desc: Insert data into constrained neo4j graph
"""

from neo4j import GraphDatabase
from data.ingress import open_1_file, open_movie_file
from utils import read_dotenv_file

soft_limit = 10


def neo4j_insert_graph(
    connection_string, username, password, movies_data, ratings_data
):
    movie_query = (
        "CREATE (:NetflixMovie {"
        + "uuid: $uuid,"
        + "release_year: $release_year,"
        + "title: $title"
        + "});"
    )

    movie_params = []
    for movie_data in movies_data:
        title = movie_data[2].replace("'", "\\'")
        movie_params.append(
            {
                "uuid": movie_data[0],
                "release_year": movie_data[1],
                "title": title,
            }
        )

    user_query = "CREATE (:User { id: $id });"
    rating_query = (
        "MATCH (m:NetflixMovie { uuid: $movie_id }), (u:User { id: $user_id })"
        + " CREATE (u)-[:RATED { rating: $rating, award_date: $award_date}]->(m) "
    )

    users_set = set()
    user_params = []
    rating_params = []

    ratings_limit_list = list(ratings_data.items())[:soft_limit]
    print(f"PREPARING {len(ratings_limit_list)} queries")

    for movie_id, ratings in ratings_limit_list:
        for rating in ratings:
            if rating["user_id"] not in users_set:
                users_set.add(rating["user_id"])
                user_params.append({"id": rating["user_id"]})

            rating_params.append(
                {
                    "user_id": rating["user_id"],
                    "movie_id": movie_id,
                    "rating": rating["rating"],
                    "award_date": rating["date"],
                }
            )

        print(f"PREPARED QUERY {len(rating_params)}")

    auth = (username, password)
    with GraphDatabase.driver(connection_string, auth=auth) as driver:
        print(f"COMMITTING {len(movie_params)} QUERIES")
        i = 0
        for movie_param in movie_params:
            driver.execute_query(movie_query, parameters_=movie_param)
            i += 1
            print(f"COMMITTED {i} queries")

        print(f"COMMITTING {len(user_params)} QUERIES")
        i = 0
        for user_param in user_params:
            driver.execute_query(user_query, parameters_=user_param)
            i += 1
            print(f"COMMITTED {i} queries")

        print(f"COMMITTING {len(rating_params)} QUERIES")
        i = 0
        for rating_param in rating_params:
            driver.execute_query(rating_query, parameters_=rating_param)
            i += 1
            print(f"COMMITTED {i} queries")


def main():
    environ = read_dotenv_file()

    if not (
        "NEO4J_CONN_STR" in environ
        and "NEO4J_USERNAME" in environ
        and "NEO4J_PASSWORD" in environ
    ):
        raise RuntimeError("Please Find .env file in slack!")

    movies_data = open_movie_file()
    ratings_data = open_1_file()
    neo4j_insert_graph(
        environ["NEO4J_CONN_STR"],
        environ["NEO4J_USERNAME"],
        environ["NEO4J_PASSWORD"],
        movies_data,
        ratings_data,
    )


if __name__ == "__main__":
    main()
