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
from redis.commands.search.reducers import count as redis_red_count
from redis.commands.search.reducers import sum as redis_red_sum
from redis.commands.search.aggregation import Asc, Desc
from json import dump, dumps, JSONEncoder

from datetime import datetime
from time import time
from dataclasses import dataclass

from data.ingress import open_1_file, open_movie_file
from util import read_dotenv_file

batch_size = 10_000
limit_size = 30


@dataclass
class Result:
    time_seconds: int
    result: str


class ResultEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


def is_timed(callable):
    def on_call(*args, **kwargs):
        start = time()
        result = callable(*args, **kwargs)
        end = time()
        timing = end - start
        print(f"TIMING {timing}")
        return Result(time_seconds=(end - start), result=result)

    return on_call


class ResultsMonad:
    def __init__(
        self,
        open_connection,
        name,
        set_up,
        num_of_ratings,
        user_most_ratings,
        title_most_ratings,
        cummulative_ratings_sum_award_date,
        drop_table,
    ):
        self._name = name
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

    def get_result(self):
        return {self._name: self._results}


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
        movie_inserts = []
        for movie_data in movies_data:
            year_released = (
                movie_data[1] if movie_data[1] == "NULL" else f"'{movie_data[1]}-01-01'"
            )
            title = movie_data[2].replace("'", "\\'")
            movie_inserts.append(f"( {movie_data[0]}, {year_released}, E'{title}' )")

        users_set = set()
        user_inserts = []
        rating_inserts = []

        print("ONTO RATINGS")

        for movie_id, ratings in ratings_data.items():
            for rating in ratings:
                if rating["user_id"] not in users_set:
                    users_set.add(rating["user_id"])
                    user_inserts.append(f"( {rating['user_id']} )")

                rating_inserts.append(
                    f"( {rating['user_id']}, {movie_id}, {rating['rating']}, '{rating['date']}' )"
                )

        print("EXECUTING")

        movie_query = f"INSERT INTO netflix_movie (id, release_year, title) VALUES {','.join(movie_inserts)};"
        user_query = f"INSERT INTO netflix_user (id) VALUES {','.join(user_inserts)};"
        cur.execute(movie_query)
        conn.commit()
        cur.execute(user_query)
        conn.commit()

        total_queries = len(rating_inserts)
        num_batches = total_queries // batch_size
        print(f"PSQL COMMITTING:{num_batches}")
        for i in range(num_batches):
            rating_query = f"INSERT INTO netflix_rating (user_id, movie_id, rating, award_date) VALUES {','.join(rating_inserts[i*batch_size:(i+1)*batch_size])};"
            cur.execute(rating_query)

        if (total_queries % batch_size) != 0:
            rating_query = f"INSERT INTO netflix_rating (user_id, movie_id, rating, award_date) VALUES {','.join(rating_inserts[num_batches*batch_size:])};"
            cur.execute(rating_query)

        conn.commit()


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
            JOIN netflix_movie nt ON nr.movie_id = nt.id
            GROUP BY nr.movie_id, nt.title
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
            SELECT rsum FROM (
              SELECT
 	            award_date,
 	            SUM(rating) OVER (ORDER BY award_date) AS rsum
              FROM netflix_rating
            ) AS inner_q
            GROUP BY award_date, rsum
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
        "POSTGRESQL",
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
                "id": int(movie_data[0]),
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
                    "award_date": datetime.strptime(rating["date"], "%Y-%m-%d"),
                }
            )

    user_list = [{"id": user_id} for user_id in user_set]

    db = conn.netflix_db
    db.netflix_movie.insert_many(movie_list, ordered=False)
    db.netflix_user.insert_many(user_list, ordered=False)
    db.netflix_rating.insert_many(rating_list, ordered=False)


@is_timed
def pymongo_num_of_ratings(conn):
    db = conn.netflix_db
    return db.netflix_rating.count_documents({})


@is_timed
def pymongo_user_most_ratings(conn: MongoClient):
    db = conn.netflix_db

    agg_result = db.netflix_rating.aggregate(
        [
            {
                "$group": {
                    "_id": "$user_id",
                    "num_ratings": {"$sum": 1},
                }
            },
            {"$sort": {"num_ratings": -1}},
            {"$limit": 1},
        ]
    )

    user_result = list(agg_result)[0]
    return f"{user_result['user_id']}:{user_result['num_ratings']}"


@is_timed
def pymongo_title_most_ratings(conn):
    db = conn.netflix_db

    agg_result = db.netflix_rating.aggregate(
        [
            {
                "$group": {
                    "_id": "$movie_id",
                    "num_ratings": {"$sum": 1},
                }
            },
            {"$sort": {"num_ratings": -1}},
            {
                "$lookup": {
                    "from": "netflix_movie",
                    "localField": "_id",
                    "foreignField": "id",
                    "as": "movie",
                }
            },
            {"$limit": 1},
        ]
    )

    movie_result = list(agg_result)[0]
    movie_title = "NA"

    if "movie" in movie_result and len(movie_result["movie"]) > 0:
        movie_title = movie_result["movie"][0]["title"]

    return f"{movie_title}:{movie_result['num_ratings']}"


@is_timed
def pymongo_cummulative_ratings_sum_award_date(conn):
    db = conn.netflix_db

    agg_result = db.netflix_rating.aggregate(
        [
            {
                "$group": {
                    "_id": "$award_date",
                    "rating": {"$sum": "$rating"},
                }
            },
            {
                "$setWindowFields": {
                    "sortBy": {
                        "_id": 1,
                    },
                    "output": {
                        "rating_sum": {
                            "$sum": "$rating",
                            "window": {
                                "documents": ["unbounded", "current"],
                            },
                        },
                    },
                }
            },
            {"$limit": 20},
        ]
    )

    list_result = [
        agg_result_elem["rating_sum"] for agg_result_elem in list(agg_result)
    ]

    return dumps(list_result)


@is_timed
def pymongo_drop_collection(conn):
    db = conn.netflix_db
    db.netflix_rating.drop()
    db.netflix_user.drop()
    db.netflix_movie.drop()


def get_pymongo_test():
    return ResultsMonad(
        lambda environ: MongoClient(environ["MONGO_CONN_STR"]),
        "DOCUMENT MONGODB",
        pymongo_create_insert,
        pymongo_num_of_ratings,
        pymongo_user_most_ratings,
        pymongo_title_most_ratings,
        pymongo_cummulative_ratings_sum_award_date,
        pymongo_drop_collection,
    )


@is_timed
def neo4j_create_insert(conn: Driver, movies_data, ratings_data):
    conn.execute_query("CREATE INDEX FOR (m:MOVIE) ON (m.title)")
    conn.execute_query("CREATE CONSTRAINT FOR (m:MOVIE) REQUIRE m.uuid IS UNIQUE")
    conn.execute_query("CREATE CONSTRAINT FOR (u:USER) REQUIRE u.id IS UNIQUE")

    movie_batches = []
    for movie_data in movies_data:
        title = "'" + movie_data[2].replace("'", "\\'") + "'"
        movie_batches.append(
            f"{'{'}uuid:{movie_data[0]},release_year:{movie_data[1]},title:{title}{'}'}"
        )

    users_set = set()
    user_batches = []
    rating_batches = []
    for movie_id, ratings in list(ratings_data.items()):
        for rating in ratings:
            if rating["user_id"] not in users_set:
                users_set.add(rating["user_id"])
                user_batches.append(f"{'{'}id:{rating['user_id']}{'}'}")

            rating_batches.append(
                f"{'{'}id:{rating['user_id']},mid:{movie_id},rating:{rating['rating']},award_date:'{rating['date']}'{'}'}"
            )

    movie_query = f"WITH [{','.join(movie_batches)}] AS batch UNWIND batch as row CREATE (n:MOVIE{'{'}uuid:row.uuid,release_year:row.release_year,title:row.title{'}'});"

    conn.execute_query(movie_query)

    total_queries = len(user_batches)
    num_batches = total_queries // batch_size
    for i in range(num_batches):
        user_query = f"WITH [{','.join(user_batches[i*batch_size:(i+1)*batch_size])}] AS batch UNWIND batch as row CREATE (:USER{'{'}id:row.id{'}'});"
        conn.execute_query(user_query)

    if (total_queries % batch_size) != 0:
        user_query = f"WITH [{','.join(user_batches[num_batches*batch_size:])}] AS batch UNWIND batch as row CREATE (:USER{'{'}id:row.id{'}'});"
        conn.execute_query(user_query)

    rating_query_postfix = """
        UNWIND batch as row
        MATCH (m:MOVIE{uuid:row.mid})
        MATCH (u:USER{id:row.id})
        CREATE (u)-[:RATED {rating:row.rating,award_date:row.award_date}]->(m);
        """

    total_queries = len(rating_batches)
    num_batches = total_queries // batch_size
    print(f"Neo4J COMMITTING:{num_batches}")
    for i in range(num_batches):
        rating_query = f"WITH [{','.join(rating_batches[i*batch_size:(i+1)*batch_size])}] AS batch {rating_query_postfix}"
        conn.execute_query(rating_query)

    if (total_queries % batch_size) != 0:
        rating_query = f"WITH [{','.join(rating_batches[num_batches*batch_size:])}] AS batch {rating_query_postfix}"
        conn.execute_query(rating_query)


@is_timed
def neo4j_num_of_ratings(conn):
    return conn.execute_query(
        """
        MATCH (:USER)-[r:RATED]->(:MOVIE)
        WITH count(*) AS counts
        RETURN counts;
        """
    )


@is_timed
def neo4j_user_most_ratings(conn):
    results = conn.execute_query(
        """
        MATCH (:USER)-[r:RATED]->(:MOVIE)
        WITH count(*) AS counts
        RETURN u.id
        ORDER BY counts DESC LIMIT 1;
        """
    )

    return results[0]


@is_timed
def neo4j_title_most_ratings(conn):
    results = conn.execute_query(
        """
        MATCH (:USER)-[r:RATED]->(m:MOVIE)
        WITH count(*) AS counts
        RETURN m.title
        ORDER BY counts DESC LIMIT 1;
        """
    )

    return results[0]


@is_timed
def neo4j_cummulative_ratings_sum_award_date(conn):
    results = conn.execute_query(
        """
        MATCH (:USER)-[r:RATED]->(m:MOVIE)
        WITH sum(r.rating), r.award_date AS ratingsAwards
        UNWIND ratingsRunning AS ratingsUnwound
        RETURN ratingsRunning
        ORDER BY r.award_date ASC LIMIT 20;
        """
    )

    return dumps(results)


@is_timed
def neo4j_drop_collection(conn):
    conn.execute_query(
        """
        MATCH (u)-[r:RATED]->(m)
        DETACH DELETE u, r, m;
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
        "GRAPH NEO4J",
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
                    "release_year",
                    str(movie_data[1]),
                    "title",
                    str(title),
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
                        "user_id",
                        int(rating["user_id"]),
                        "movie_id",
                        int(movie_id),
                        "rating",
                        str(rating["rating"]),
                        "date",
                        str(rating["date"]),
                    ],
                }
            )

    movie_pipeline = conn.pipeline(transaction=False)
    conditional_batchify = 0
    on_count = 10_000
    for movie_item in movie_list:
        movie_pipeline.hset(name=movie_item["name"], items=movie_item["items"])
        conditional_batchify += 1

        if conditional_batchify == on_count:
            movie_pipeline.execute()
            conditional_batchify = 0

    if conditional_batchify != 0:
        conditional_batchify = 0
        movie_pipeline.execute()

    conn.sadd("user", ",".join([str(user) for user in user_set]))

    conn.ft("idx:rating").create_index(
        (
            (NumericField("user_id", sortable=True)),
            (NumericField("movie_id", sortable=True)),
            (TextField("date", sortable=True)),
        ),
        definition=IndexDefinition(["rating:"], index_type=IndexType.HASH),
    )

    rating_pipeline = conn.pipeline(transaction=False)
    i = 0
    for rating in rating_list:
        conn.hset(name=rating["name"], items=rating["items"])
        conditional_batchify += 1

        if conditional_batchify == on_count:
            i += 1
            rating_pipeline.execute()
            conditional_batchify = 0

    if conditional_batchify != 0:
        conditional_batchify = 0
        rating_pipeline.execute()


@is_timed
def redis_num_of_ratings(conn: Redis):
    count = conn.keys("rating:*")

    return str(len(count))


@is_timed
def redis_user_most_ratings(conn: Redis):
    res = conn.ft("idx:rating").aggregate(
        AggregateRequest("*")
        .load("*")
        .group_by("@user_id", redis_red_count().alias("c"))
        .sort_by(Desc("@c"), max=1)
        .apply(identity="@user_id")
    )

    return res.rows[0]["user_id"]


@is_timed
def redis_title_most_ratings(conn: Redis):
    res = conn.ft("idx:rating").aggregate(
        AggregateRequest("*")
        .load("*")
        .group_by("@movie_id", redis_red_count().alias("c"))
        .sort_by(Desc("@c"), max=1)
    )

    title = conn.hget(f"movie:{res.rows[0]['movie_id']}", "title")
    return title


@is_timed
def redis_cummulative_ratings_sum_award_date(conn: Redis):
    res = conn.ft("idx:rating").aggregate(
        AggregateRequest("*")
        .load("rating")
        .group_by("@date", redis_red_sum("@rating"))
        .sort_by(Asc("@date"))
    )

    running = 0
    windowed_results = []
    for result in res.rows[:20]:
        running += result["rating"]
        windowed_results.append(running)

    return dumps(windowed_results)


@is_timed
def redis_drop_collection(conn: Redis):
    conn.delete("user")

    for hashset in conn.hscan_iter("delscanmovie", match="movie:*"):
        conn.delete(hashset["key"])

    for hashset in conn.hscan_iter("delscan", match="rating:*"):
        conn.delete(hashset["key"])


def get_redis_test():
    return ResultsMonad(
        lambda environ: Redis(
            host=environ["REDIS_HOST"],
            db=0,
            username=environ["REDIS_USERNAME"],
            password=environ["REDIS_PASSWORD"],
        ),
        "KEY-VALUE REDIS",
        redis_create_insert,
        redis_num_of_ratings,
        redis_user_most_ratings,
        redis_title_most_ratings,
        redis_cummulative_ratings_sum_award_date,
        redis_drop_collection,
    )


def main():
    environ = read_dotenv_file()

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
        # get_psycopg_test(),
        get_pymongo_test(),
        # get_neo4j_test(),
        # get_redis_test(),
    ]
    results = []

    movies_data = open_movie_file()
    ratings_data = open_1_file()
    ratings_data = dict(list(ratings_data.items())[:limit_size])

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

    with open("./output.json", "w+") as fd:
        dump(results, fd, cls=ResultEncoder)


if __name__ == "__main__":
    main()
