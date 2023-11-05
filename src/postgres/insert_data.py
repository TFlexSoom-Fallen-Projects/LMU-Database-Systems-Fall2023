"""
Name: Tristan Hilbert
Date: 10/10/2023
Filename: create_table.py
Desc: A python script to create the tables for the postgres database
"""

import psycopg
from data.ingress import open_1_file, open_movie_file
from utils import read_dotenv_file


def psycopg_insert_tables(connection_string, movies_data, ratings_data):
    # Connect to an existing database
    with psycopg.connect(connection_string) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            print(f"Postgres Connection: {conn}")

            movies_str = "INSERT INTO netflix_movie (id, release_year, title) VALUES "
            for movie_data in movies_data:
                year_released = (
                    movie_data[1]
                    if movie_data[1] == "NULL"
                    else f"'{movie_data[1]}-01-01'"
                )
                title = movie_data[2].replace("'", "\\'")
                movies_str += f"( {movie_data[0]}, {year_released}, E'{title}' ),"

            movies_str = movies_str[:-1] + ";"

            cur.execute(movies_str)

            # Make the changes to the database persistent
            conn.commit()

            users_set = set()
            user_str_base = "INSERT INTO netflix_user (id) VALUES "
            rating_str_base = "INSERT INTO netflix_rating (user_id, movie_id, rating, award_date) VALUES "

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


def main():
    environ = read_dotenv_file()

    if not "POSTGRES_CONN_STR" in environ:
        raise RuntimeError("Please Find .env file in slack!")

    movies_data = open_movie_file()
    ratings_data = open_1_file()
    psycopg_insert_tables(environ["POSTGRES_CONN_STR"], movies_data, ratings_data)


if __name__ == "__main__":
    main()
