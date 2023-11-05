"""
Name: Tristan Hilbert
Date: 11/4/2023
Filename: create_collection.py
Desc: Create an Aggregation in MongoDB
"""

from pymongo import MongoClient
from data.ingress import open_1_file, open_movie_file
from utils import read_dotenv_file
from time import sleep


def pymongo_insert_collection(connection_string, movies_data, ratings_data):
    movie_list = []  # (id, release_year, title)
    user_set = set()  # (id)
    rating_list = []  # (user_id, movie_id, rating, award_date)

    for movie_data in movies_data:
        year_released = (
            movie_data[1] if movie_data[1] == "NULL" else f"'{movie_data[1]}-01-01'"
        )
        title = movie_data[2].replace("'", "\\'")
        movie_list.append(
            {
                "id": movie_data[0],
                "release_year": year_released,
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

    # Connect to an existing databas
    with MongoClient(connection_string) as client:
        db = client.netflix_db
        print("SUBMITTING MOVIES!")
        db.netflix_movie.insert_many(movie_list)
        print("FINISHED SUBMITTING MOVIES!")

        print("SUBMITTING USERS!")
        db.netflix_user.insert_many(user_list)
        print("FINISHED SUBMITTING USERS!")

        print("SUBMITTING RATINGS!")
        db.netflix_rating.insert_many(rating_list)
        print("FINISHED SUBMITTING RATINGS!")


def main():
    environ = read_dotenv_file()

    if not "MONGO_CONN_STR" in environ:
        raise RuntimeError("Please Find .env file in slack!")

    movies_data = open_movie_file()
    ratings_data = open_1_file()
    pymongo_insert_collection(environ["MONGO_CONN_STR"], movies_data, ratings_data)


if __name__ == "__main__":
    main()
