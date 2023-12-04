"""
Name: Tristan Hilbert
Date: 10/10/2023
Filename: ingress.py
Desc: File to read combined_data.txt
"""

from typing import TextIO, List
from csv import reader

prefix = "./resources/dataset/"
file_names = [
    "combined_data_1.txt",
    "combined_data_2.txt",
    "combined_data_3.txt",
    "combined_data_4.txt",
]


def read_comb_data(txt_file: TextIO) -> dict:
    rows = {}
    movie_id = None
    for line in txt_file:
        if ":" in line:
            movie_id = int(line[:-2])
            rows[movie_id] = rows.get(movie_id, [])
        else:
            segments = line.split(",")
            if segments[2][-1] == "\n":
                segments[2] = segments[2][:-1]

            rows[movie_id].append(
                {
                    "user_id": int(segments[0]),
                    "rating": int(segments[1]),
                    "date": segments[2],
                }
            )

    return rows


def open_file(file_name: str) -> dict:
    with open(prefix + file_name, "r") as file:
        return read_comb_data(file)


def open_1_file() -> dict:
    return open_file(file_names[0])


def open_all_files() -> dict:
    for file_name in file_names:
        data = dict(data, open_file(file_name))

    return data


def open_movie_file() -> List[tuple]:
    result = []
    with open("./resources/dataset/movie_titles.csv", "r") as file:
        for line in reader(file):
            result.append(line)

    return result
