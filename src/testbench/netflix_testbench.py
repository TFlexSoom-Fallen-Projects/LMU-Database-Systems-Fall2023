"""
Name: Tristan Hilbert
Date: 11/22/2023
Filename: netflix_testbench.py
Desc: A python script to time different operations most efficiently per database.
"""

from psycopg import connect as postgres_connect
from psycopg import Connection
from pymongo import MongoClient
from neo4j import GraphDatabase, Driver
from redis import Redis
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.reducers import count
from json import dumps

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
    # Open a cursor to perform database operations
    with conn.cursor() as cur:
        # Execute a command: this creates a new table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS netflix_movie (
                id SERIAL PRIMARY KEY,
                release_year date,
                title TEXT NOT NULL
            );
                    
            CREATE TABLE IF NOT EXISTS netflix_user (
                id SERIAL PRIMARY KEY
            );
        """
        )

        # Make the changes to the database persistent
        conn.commit()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS netflix_rating (
                user_id SERIAL NOT NULL,
                movie_id SERIAL NOT NULL,
                rating INTEGER NOT NULL,
                award_date DATE NOT NULL,
                PRIMARY KEY(user_id, movie_id),
                FOREIGN KEY(user_id) REFERENCES netflix_user(id),
                FOREIGN KEY(movie_id) REFERENCES netflix_movie(id)
            );
            """
        )

        conn.commit()

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_award_date ON netflix_rating ( award_date );
            """
        )

        conn.commit()

    with conn.cursor() as cur:
        movies_str = "INSERT INTO netflix_movie (id, release_year, title) VALUES "
        for movie_data in movies_data:
            year_released = (
                movie_data[1] if movie_data[1] == "NULL" else f"'{movie_data[1]}-01-01'"
            )
            title = movie_data[2].replace("'", "\\'")
            movies_str += f"( {movie_data[0]}, {year_released}, E'{title}' ),"

        movies_str = movies_str[:-1] + ";"

        cur.execute(movies_str)

        # Make the changes to the database persistent
        conn.commit()

        users_set = set()
        user_str_base = "INSERT INTO netflix_user (id) VALUES "
        rating_str_base = (
            "INSERT INTO netflix_rating (user_id, movie_id, rating, award_date) VALUES "
        )

        user_str_queries = []
        rating_str_queries = []

        print(f"PREPARING {len(ratings_data)} queries")

        for movie_id, ratings in ratings_data.items():
            user_str_query = user_str_base[:]
            rating_str_query = rating_str_base[:]

            for rating in ratings:
                if rating["user_id"] not in users_set:
                    users_set.add(rating["user_id"])
                    user_str_query += f"( {rating['user_id']} ),"

                rating_str_query += f"( {rating['user_id']}, {movie_id}, {rating['rating']}, '{rating['date']}' ),"

            if len(user_str_query) > len(user_str_base):
                user_str_queries.append(user_str_query[:-1] + ";")

            rating_str_queries.append(rating_str_query[:-1] + ";")
            print(f"PREPARED QUERY {len(rating_str_queries)}")

        print(f"COMMITTING {len(rating_str_queries)} QUERIES")
        i = 0
        for user_str_query in user_str_queries:
            cur.execute(user_str_query)

            # Make the changes to the database persistent
            conn.commit()
            i += 1
            print(f"COMMITTED {i} queries")

        print(f"COMMITTING {len(rating_str_queries)} QUERIES")
        i = 0
        for rating_str_query in rating_str_queries:
            cur.execute(rating_str_query)

            # Make the changes to the database persistent
            conn.commit()
            i += 1
            print(f"COMMITTED {i} queries")


@is_timed
def psycopg_num_of_ratings(conn: Connection):
    with conn.cursor() as cur:
        count = cur.execute(
            """
            SELECT COUNT(*) FROM netflix_rating;
            """
        ).fetchone()

        return str(count)


@is_timed
def psycopg_user_most_ratings(conn):
    with conn.cursor() as cur:
        user_id, cnt = cur.execute(
            """
            SELECT user_id, COUNT(*) AS cnt 
            FROM netflix_rating
            GROUP BY user_id
            ORDER BY cnt DESC
            LIMIT 1;
            """
        ).fetchone()

        return f"{user_id}:{cnt}"


@is_timed
def psycopg_title_most_ratings(conn):
    with conn.cursor() as cur:
        movie_title, cnt = cur.execute(
            """
            SELECT nt.title, COUNT(*) AS cnt 
            FROM netflix_rating nr
            JOIN netflix_title nt ON nr.movie_id = nt.id
            GROUP BY nr.movie_id
            ORDER BY cnt DESC
            LIMIT 1;
            """
        ).fetchone()

        return f"{movie_title}:{cnt}"


@is_timed
def psycopg_cummulative_ratings_sum_award_date(conn):
    with conn.cursor() as cur:
        sums = cur.execute(
            """
            SELECT SUM(rating) OVER (ORDER BY award_date)
            FROM netflix_rating
            LIMIT 20;
            """
        ).fetchall()

        return dumps(sums)


@is_timed
def psycopg_drop_collection(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            DROP TABLE IF EXISTS netflix_rating;
            """
        )
        conn.commit()

        cur.execute(
            """
            DROP TABLE IF EXISTS netflix_movie;
                    
            DROP TABLE IF EXISTS netflix_user;
            """
        )

        conn.commit()


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
    movie_list = []  # (id, release_year, title)
    user_set = set()  # (id)
    rating_list = []  # (user_id, movie_id, rating, award_date)

    for movie_data in movies_data:
        title = movie_data[2].replace("'", "\\'")
        movie_list.append(
            {
                "id": movie_data[0],
                "release_year": movie_data[1],
                "title": title,
            }
        )

    for movie_id, ratings in ratings_data.items():
        for rating in ratings:
            user_set.add(rating["user_id"])

            rating_list.append(
                {
                    "user_id": rating["user_id"],
                    "movie_id": movie_id,
                    "rating": rating["rating"],
                    "award_date": rating["date"],
                }
            )

    user_list = [{"id": user_id} for user_id in user_set]

    db = conn.netflix_db
    print("SUBMITTING MOVIES!")
    db.netflix_movie.insert_many(movie_list, ordered=False)
    print("FINISHED SUBMITTING MOVIES!")

    print("SUBMITTING USERS!")
    db.netflix_user.insert_many(user_list, ordered=False)
    print("FINISHED SUBMITTING USERS!")

    print("SUBMITTING RATINGS!")
    db.netflix_rating.insert_many(rating_list, ordered=False)
    print("FINISHED SUBMITTING RATINGS!")


@is_timed
def pymongo_num_of_ratings(conn):
    db = conn.netflix_db

    return db.netflix_rating.count_documents({})


@is_timed
def pymongo_user_most_ratings(conn):
    db = conn.netflix_db

    agg_result = db.netflix_rating.aggregate(
        {"$group": {"user_id": "$user_id", "num_ratings": {"$sum": 1}}},
        {"$sort": {"num_ratings": -1}},
        {"$limit": 1},
    )

    return agg_result["user_id"]


@is_timed
def pymongo_title_most_ratings(conn):
    db = conn.netflix_db

    agg_result = db.netflix_rating.aggregate(
        {"$group": {"movie_id": "$movie_id", "num_ratings": {"$sum": 1}}},
        {"$sort": {"num_ratings": -1}},
        {"$limit": 1},
        {
            "$lookup": {
                "from": "netflix_movie",
                "localField": "movie_id",
                "foreignField": "id",
                "as": "title",
            }
        },
    )

    print(agg_result)
    return agg_result["title"]


@is_timed
def pymongo_cummulative_ratings_sum_award_date(conn):
    db = conn.netflix_db

    agg_result = db.netflix_rating.aggregate(
        {
            "$setWindowFields": {
                "sortBy": {
                    "$award_date": 1,
                },
                "output": {
                    "rating_sum": {
                        "$sum": "$rating",
                        # "window": {
                        #     "documents": [ <lower boundary>, <upper boundary> ],
                        #     "range": [ <lower boundary>, <upper boundary> ],
                        #     "unit": <time unit>
                        # }
                    },
                },
            }
        }
    )

    print(agg_result)
    return agg_result["rating_sum"]


@is_timed
def pymongo_drop_collection(conn):
    db = conn.netflix_db
    db.netflix_rating.drop()
    db.netflix_user.drop()
    db.netflix_movie.drop()


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
def neo4j_create_insert(conn: Driver, movies_data, ratings_data):
    conn.execute_query(
        "CREATE CONSTRAINT IF NOT EXISTS FOR (m:NetflixMovie) REQUIRE (m.uuid) IS UNIQUE;"
    )
    conn.execute_query("CREATE INDEX IF NOT EXISTS FOR (m:NetflixMovie) ON (m.title);")
    conn.execute_query(
        "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE (u.id) IS UNIQUE;"
    )

    movie_query = """
            CALL apoc.cypher.runMany(
            'CREATE (:NetflixMovie {
                uuid: $uuid,
                release_year: $release_year,
                title: $title
            });'
        """

    user_query = """
            CALL apoc.cypher.runMany(
            'CREATE (:User { id: $id });'
        """

    rating_query = """
            CALL apoc.cypher.runMany(
            'MATCH (m:NetflixMovie { uuid: $mid })
            MATCH (u:User { id: $id })
            CREATE (u)-[r:RATED { rating: $rating, award_date: $award_date}]->(m);'
        """

    for movie_data in movies_data:
        title = movie_data[2].replace("'", "\\'")

        item = {
            "uuid": movie_data[0],
            "release_year": movie_data[1],
            "title": title,
        }

        movie_query += f",\n{item}"

    users_set = set()

    for movie_id, ratings in ratings_data.items():
        for rating in ratings:
            if rating["user_id"] not in users_set:
                users_set.add(rating["user_id"])
                item = {"id": rating["user_id"]}
                user_query += f",\n{item}"

            item = {
                "id": rating["user_id"],
                "mid": str(movie_id),
                "rating": rating["rating"],
                "award_date": rating["date"],
            }

            rating_query += f",\n{item}"

    movie_query += ");"
    user_query += ");"
    rating_query += ");"

    conn.execute_query(movie_query)
    conn.execute_query(user_query)
    conn.execute_query(rating_query)


@is_timed
def neo4j_num_of_ratings(conn):
    return conn.execute_query("COUNT(MATCH [RATED]);")


@is_timed
def neo4j_user_most_ratings(conn):
    return conn.execute_query(
        """
        MATCH (u:USER)-[r:RATED]
        RETURN u.id, max(count(*))
        """
    )


@is_timed
def neo4j_title_most_ratings(conn):
    return conn.execute_query(
        """
        MATCH [r:RATED]-(m:MOVIE)
        RETURN m.title, max(count(*))
        """
    )


@is_timed
def neo4j_cummulative_ratings_sum_award_date(conn):
    # TODO
    pass


@is_timed
def neo4j_drop_collection(conn):
    conn.execute_query(
        """
        MATCH [r:RATED]
        DETACH DELETE r;
        """
    )
    conn.execute_query(
        """
        MATCH (m:MOVIE)
        DETACH DELETE m;
        """
    )
    conn.execute_query(
        """
        MATCH (u:USER)
        DETACH DELETE u;
        """
    )


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
def redis_create_insert(conn: Redis, movies_data, ratings_data):
    movie_list = []  # (id, release_year, title)
    user_set = set()  # (id)
    rating_list = []  # (user_id, movie_id, rating, award_date)

    for movie_data in movies_data:
        title = movie_data[2].replace("'", "\\'")
        movie_list.append(
            {
                "name": f"movie:{movie_data[0]}",
                "items": [
                    f"release_year {movie_data[1]}",
                    f"title {title}",
                ],
            }
        )

    for movie_id, ratings in ratings_data.items():
        for rating in ratings:
            user_set.add(rating["user_id"])

            rating_list.append(
                {
                    "name": f"rating:{rating['user_id']}:{movie_id}",
                    "items": [
                        f"user_id {rating['user_id']}",
                        f"movie_id {movie_id}",
                        f"rating {rating['rating']}",
                        f"date {rating['date']}",
                    ],
                }
            )

    movie_pipeline = conn.pipeline(transaction=False)
    conditional_batchify = 0
    on_count = 1000
    for movie_item in movie_list:
        movie_pipeline.hset(name=movie_item["name"], items=movie_item["items"])
        conditional_batchify += 1

        if conditional_batchify == on_count:
            movie_pipeline.execute()
            movie_pipeline = conn.pipeline(transaction=False)
            conditional_batchify = 0

    if conditional_batchify != 0:
        conditional_batchify = 0
        movie_pipeline.execute()

    conn.sadd("user", ",".join([str(user) for user in user_set]))

    conn.ft("rating:userid").create_index(
        (
            (NumericField("$.user_id", as_name="user_id")),
            (NumericField("$.movie_id", as_name="movie_id")),
            (NumericField("$.rating", as_name="rating")),
            (TextField("$.date", as_name="date")),
        ),
        definition=IndexDefinition("rating:", index_type=IndexType.HASH),
    )

    rating_pipeline = conn.pipeline(transaction=False)
    for rating in rating_list:
        conn.hset(name=rating["name"], items=rating["items"])

        if conditional_batchify == on_count:
            rating_pipeline.execute()
            rating_pipeline = conn.pipeline(transaction=False)
            conditional_batchify = 0

    if conditional_batchify != 0:
        conditional_batchify = 0
        rating_pipeline.execute()


@is_timed
def redis_num_of_ratings(conn: Redis):
    count = conn.keys("rating:")

    return count


@is_timed
def redis_user_most_ratings(conn: Redis):
    res = conn.ft("rating:userid").aggregate(
        AggregateRequest("*")
        .group_by("@user_id", count().alias("count"))
        .sort_by("@count")
        .limit(0, 1)
    )

    print(res)
    return res[0]["user_id"]


@is_timed
def redis_title_most_ratings(conn: Redis):
    res = conn.ft("rating:userid").aggregate(
        AggregateRequest("*")
        .group_by("@movie_id", count().alias("count"))
        .sort_by("@count")
        .limit(0, 1)
    )

    print(res)

    title = conn.hget(f"movie:{res['movie_id']}", "title")
    return title


@is_timed
def redis_cummulative_ratings_sum_award_date(conn: Redis):
    # TODO
    pass


@is_timed
def redis_drop_collection(conn: Redis):
    conn.delete("user")

    for hashset in conn.hscan_iter("delscanmovie", match="movie:"):
        print(hashset)
        conn.delete(hashset["key"])

    for hashset in conn.hscan_iter("delscan", match="rating:"):
        print(hashset)
        conn.delete(hashset["key"])


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
                # .drop_table()
            )

        results.append(result_monad.get_result())


if __name__ == "__main__":
    main()
