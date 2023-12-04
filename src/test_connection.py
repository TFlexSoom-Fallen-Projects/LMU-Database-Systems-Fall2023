"""
Name: Tristan Hilbert
Date: 9/26/2023
Filename: test_connection.py
Desc: A python script to test the connection between different cloud databases.
"""

import os
import pymongo
import psycopg
from neo4j import GraphDatabase
from redis import Redis
from dotenv import load_dotenv


def read_dotenv_file():
    load_dotenv()
    return os.environ


def pymongo_test(connection_string):
    client = pymongo.MongoClient(connection_string)
    print(f"Mongo Client: {client}")


def psycopg_test(connection_string):
    conn = psycopg.connect(connection_string)
    print(f"Postgres Connection: {conn}")


def neo4j_test(connection_string, username, password):
    auth = (username, password)
    with GraphDatabase.driver(connection_string, auth=auth) as conn:
        print(f"Neo4j GraphDB Connection {conn}")


def redis_test(host, username, password, port=6379):
    boi = Redis(host=host, port=port, db=0, username=username, password=password)
    print(f"Redis Connection: {boi} and Redis Ping Result: {boi.ping()}")


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
    psycopg_test(environ["POSTGRES_CONN_STR"])
    neo4j_test(
        environ["NEO4J_CONN_STR"], environ["NEO4J_USERNAME"], environ["NEO4J_PASSWORD"]
    )

    if ("USE_REDIS" in environ and environ["USE_REDIS"] == "true") and (
        (not "REDIS_HOST" in environ)
        or (not "REDIS_USERNAME" in environ)
        or (not "REDIS_PASSWORD" in environ)
    ):
        raise RuntimeError("Redis is used but .env does not have proper values")

    redis_test(
        environ["REDIS_HOST"], environ["REDIS_USERNAME"], environ["REDIS_PASSWORD"]
    )


if __name__ == "__main__":
    main()
