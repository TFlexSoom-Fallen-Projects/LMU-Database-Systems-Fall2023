"""
Name: Tristan Hilbert
Date: 11/22/2023
Filename: netflix_testbench.py
Desc: A python script to time different operations most efficiently per database.
"""

from psycopg import connect as postgres_connect
from pymongo import MongoClient
from neo4j import GraphDatabase
from redis import Redis

from timeit import timeit
import timeit as timeit_module
from dataclasses import dataclass

from data.ingress import open_1_file, open_movie_file
from dotenv import load_dotenv


@dataclass
class Result:
    time_seconds: int
    result: str


# https://stackoverflow.com/questions/24812253/how-can-i-capture-return-value-with-python-timeit-module
def _template_func(setup, func):
    """Create a timer function. Used if the "statement" is a callable."""

    def inner(_it, _timer, _func=func):
        setup()
        _t0 = _timer()
        for _i in _it:
            retval = _func()
        _t1 = _timer()
        return _t1 - _t0, retval

    return inner


timeit_module._template_func = _template_func
######


def is_timed(callable):
    def on_call(*args, **kwargs):
        time, result = timeit(lambda: callable(*args, **kwargs), number=1)
        return Result(time_seconds=time, result=result)

    return on_call


class ResultsMonad:
    def __init__(
        self,
        open_connection,
        set_up,
        num_of_ratings,
        user_most_ratings,
        title_most_ratings,
        cummulative_ratings_sum_award_date,
        drop_table,
    ):
        self._open_connection = open_connection
        self._set_up = set_up
        self._num_of_ratings = num_of_ratings
        self._user_most_ratings = user_most_ratings
        self._title_most_ratings = title_most_ratings
        self._cummulative_ratings_sum_award_date = cummulative_ratings_sum_award_date
        self._drop_table = drop_table
        self._instance = None
        self._results = {}

    def open_connection(self, environ):
        self._instance = self._open_connection(environ)
        return self._instance

    def set_up(self, movies_data, ratings_data):
        self._results["load"] = self._set_up(self._instance, movies_data, ratings_data)
        return self

    def num_of_ratings(self):
        self._results["num_of_ratings"] = self._num_of_ratings(self._instance)
        return self

    def user_most_ratings(self):
        self._results["user_most_ratings"] = self._user_most_ratings(self._instance)
        return self

    def title_most_ratings(self):
        self._results["title_most_ratings"] = self._title_most_ratings(self._instance)
        return self

    def cummulative_ratings_sum_award_date(self):
        self._results[
            "cummulative_ratings_sum_award_date"
        ] = self._cummulative_ratings_sum_award_date(self._instance)
        return self

    def drop_table(self):
        self._results["drop_table"] = self._drop_table(self._instance)
        return self


@is_timed
def psycopg_create_insert(conn, movies_data, ratings_data):
    pass


@is_timed
def psycopg_num_of_ratings(conn):
    pass


@is_timed
def psycopg_user_most_ratings(conn):
    pass


@is_timed
def psycopg_title_most_ratings(conn):
    pass


@is_timed
def psycopg_cummulative_ratings_sum_award_date(conn):
    pass


@is_timed
def psycopg_drop_collection(conn):
    pass


def get_psycopg_test():
    return ResultsMonad(
        lambda environ: postgres_connect(environ["POSTGRES_CONN_STR"]),
        psycopg_create_insert,
        psycopg_num_of_ratings,
        psycopg_user_most_ratings,
        psycopg_title_most_ratings,
        psycopg_cummulative_ratings_sum_award_date,
        psycopg_drop_collection,
    )


@is_timed
def pymongo_create_insert(conn, movies_data, ratings_data):
    pass


@is_timed
def pymongo_num_of_ratings(conn):
    pass


@is_timed
def pymongo_user_most_ratings(conn):
    pass


@is_timed
def pymongo_title_most_ratings(conn):
    pass


@is_timed
def pymongo_cummulative_ratings_sum_award_date(conn):
    pass


@is_timed
def pymongo_drop_collection(conn):
    pass


def get_pymongo_test():
    return ResultsMonad(
        lambda environ: MongoClient(environ["MONGO_CONN_STR"]),
        pymongo_create_insert,
        pymongo_num_of_ratings,
        pymongo_user_most_ratings,
        pymongo_title_most_ratings,
        pymongo_cummulative_ratings_sum_award_date,
        pymongo_drop_collection,
    )


@is_timed
def neo4j_create_insert(conn, movies_data, ratings_data):
    pass


@is_timed
def neo4j_num_of_ratings(conn):
    pass


@is_timed
def neo4j_user_most_ratings(conn):
    pass


@is_timed
def neo4j_title_most_ratings(conn):
    pass


@is_timed
def neo4j_cummulative_ratings_sum_award_date(conn):
    pass


@is_timed
def neo4j_drop_collection(conn):
    pass


def get_neo4j_test():
    return ResultsMonad(
        lambda environ: GraphDatabase.driver(
            environ["NEO4J_CONN_STR"],
            auth=(environ["NEO4J_USERNAME"], environ["NEO4J_PASSWORD"]),
        ),
        neo4j_create_insert,
        neo4j_num_of_ratings,
        neo4j_user_most_ratings,
        neo4j_title_most_ratings,
        neo4j_cummulative_ratings_sum_award_date,
        neo4j_drop_collection,
    )


@is_timed
def redis_create_insert(conn, movies_data, ratings_data):
    pass


@is_timed
def redis_num_of_ratings(conn):
    pass


@is_timed
def redis_user_most_ratings(conn):
    pass


@is_timed
def redis_title_most_ratings(conn):
    pass


@is_timed
def redis_cummulative_ratings_sum_award_date(conn):
    pass


@is_timed
def redis_drop_collection(conn):
    pass


def get_redis_test():
    return ResultsMonad(
        lambda environ: Redis(
            host=environ["REDIS_HOST"],
            db=0,
            username=environ["REDIS_USERNAME"],
            password=environ["REDIS_PASSWORD"],
        ),
        redis_create_insert,
        redis_num_of_ratings,
        redis_user_most_ratings,
        redis_title_most_ratings,
        redis_cummulative_ratings_sum_award_date,
        redis_drop_collection,
    )


def main():
    environ = load_dotenv()

    if (
        (not "MONGO_CONN_STR" in environ)
        or (not "POSTGRES_CONN_STR" in environ)
        or (not "NEO4J_CONN_STR" in environ)
        or (not "NEO4J_USERNAME" in environ)
        or (not "NEO4J_PASSWORD" in environ)
        or (not "REDIS_HOST" in environ)
        or (not "REDIS_USERNAME" in environ)
        or (not "REDIS_PASSWORD" in environ)
    ):
        raise RuntimeError(".env file does not have required values")

    # define different databases here
    domains = [
        get_psycopg_test(),
        get_pymongo_test(),
        get_neo4j_test(),
        get_redis_test(),
    ]
    results = []

    movies_data = open_movie_file()
    ratings_data = open_1_file()

    for domain in domains:
        with domain.open_connection(environ):
            # Load the data
            result_monad = (
                domain.set_up(movies_data, ratings_data)
                .num_of_ratings()
                .user_most_ratings()
                .title_most_ratings()
                .cummulative_ratings_sum_award_date()
                .drop_table()
            )

        results.append(result_monad.get_result())


if __name__ == "__main__":
    main()
