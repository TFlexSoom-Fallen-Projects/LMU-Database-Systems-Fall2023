"""
Name: Tristan Hilbert
Date: 11/4/2023
Filename: insert_collection.py
Desc: Create 3 collections in MongoDB
"""

from redis import Redis
from data.ingress import open_1_file, open_movie_file
from utils import read_dotenv_file


def redis_insert_keyvalue(host, username, password, movies_data, ratings_data):
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
                        f"rating {rating['rating']}",
                        f"date {rating['date']}",
                    ],
                }
            )

    rds = Redis(host=host, username=username, password=password)

    print("Starting Movies")
    for movie_item in movie_list:
        rds.hset(name=movie_item["name"], items=movie_item["items"])
    print("Finished Movies")

    print("Starting Users")
    rds.sadd("user", ",".join([str(user) for user in user_set]))
    print("Finished Users")

    print("Starting Ratings")
    for rating in rating_list:
        rds.hset(name=rating["name"], items=rating["items"])
    print("Finished Ratings")


def main():
    environ = read_dotenv_file()

    if (
        (not "REDIS_HOST" in environ)
        or (not "REDIS_USERNAME" in environ)
        or (not "REDIS_PASSWORD" in environ)
    ):
        raise RuntimeError(".env does not have proper values")

    movies_data = open_movie_file()
    ratings_data = open_1_file()
    redis_insert_keyvalue(
        environ["REDIS_HOST"],
        environ["REDIS_USERNAME"],
        environ["REDIS_PASSWORD"],
        movies_data,
        ratings_data,
    )


if __name__ == "__main__":
    main()
