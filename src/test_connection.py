"""
Name: Tristan Hilbert
Date: 9/26/2023
Filename: test_connection.py
Desc: A python script to test the connection between different cloud databases.
"""

import os
import pymongo
import psycopg2
from neo4j import GraphDatabase, RoutingControl
from dotenv import load_dotenv


def read_dotenv_file():
    load_dotenv()
    return os.environ


def pymongo_test(connection_string):
    client = pymongo.MongoClient(connection_string)
    print(f"Mongo Client: {client}")


def psycopg2_test(connection_string):
    conn = psycopg2.connect(connection_string)
    print(f"Postgres Connection: {conn}")


def neo4j_test(connection_string, username, password):
    auth = (username, password)
    with GraphDatabase.driver(connection_string, auth=auth) as conn:
        print(f"Neo4j GraphDB Connection {conn}")


def main():
    environ = read_dotenv_file()

    if (
        (not "MONGO_CONN_STR" in environ)
        or (not "POSTGRES_CONN_STR" in environ)
        or (not "NEO4J_CONN_STR" in environ)
        or (not "NEO4J_USERNAME" in environ)
        or (not "NEO4J_PASSWORD" in environ)
    ):
        raise RuntimeError("Please Find .env file in slack!")

    pymongo_test(environ["MONGO_CONN_STR"])
    psycopg2_test(environ["POSTGRES_CONN_STR"])
    neo4j_test(
        environ["NEO4J_CONN_STR"], environ["NEO4J_USERNAME"], environ["NEO4J_PASSWORD"]
    )


if __name__ == "__main__":
    main()
