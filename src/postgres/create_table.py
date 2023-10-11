"""
Name: Tristan Hilbert
Date: 10/10/2023
Filename: create_table.py
Desc: A python script to create the tables for the postgres database
"""

import psycopg
from utils import read_dotenv_file

def psycopg_create_tables(connection_string):
    # Connect to an existing database
    with psycopg.connect(connection_string) as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            print(f"Postgres Connection: {conn}")

            # Execute a command: this creates a new table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS netflix_movie (
                    id SERIAL PRIMARY KEY,
                    release_year date,
                    title TEXT NOT NULL
                );
                        
                CREATE TABLE IF NOT EXISTS netflix_user (
                    id SERIAL PRIMARY KEY
                );
            """)

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

    

def main():
    environ = read_dotenv_file()

    if not "POSTGRES_CONN_STR" in environ:
        raise RuntimeError("Please Find .env file in slack!")

    psycopg_create_tables(environ["POSTGRES_CONN_STR"])


if __name__ == "__main__":
    main()
