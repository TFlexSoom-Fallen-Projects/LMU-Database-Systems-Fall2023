"""
Name: Tristan Hilbert
Date: 11/5/2023
Filename: create_graph.py
Desc: A python script to create the graph constraints and indexes for the neo4j db
"""

from neo4j import GraphDatabase
from utils import read_dotenv_file


def neo4j_create_graph(connection_string, username, password):
    # Connect to an existing database
    auth = (username, password)
    with GraphDatabase.driver(connection_string, auth=auth) as conn:
        print(f"Neo4js Connection: {conn}")

        conn.execute_query(
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:NetflixMovie) REQUIRE (m.uuid) IS UNIQUE;"
        )
        conn.execute_query(
            "CREATE INDEX IF NOT EXISTS FOR (m:NetflixMovie) ON (m.title);"
        )
        conn.execute_query(
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE (u.id) IS UNIQUE;"
        )


def main():
    environ = read_dotenv_file()

    if not (
        "NEO4J_CONN_STR" in environ
        and "NEO4J_USERNAME" in environ
        and "NEO4J_PASSWORD" in environ
    ):
        raise RuntimeError("Please Find .env file in slack!")

    neo4j_create_graph(
        environ["NEO4J_CONN_STR"],
        environ["NEO4J_USERNAME"],
        environ["NEO4J_PASSWORD"],
    )


if __name__ == "__main__":
    main()
