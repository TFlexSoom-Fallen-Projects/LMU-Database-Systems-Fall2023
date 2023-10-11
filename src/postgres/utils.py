"""
Name: Tristan Hilbert
Date: 10/10/2023
Filename: utils.py
Desc: A python script to create the tables for the postgres database
"""

import os
from dotenv import load_dotenv

def read_dotenv_file():
    load_dotenv()
    return os.environ