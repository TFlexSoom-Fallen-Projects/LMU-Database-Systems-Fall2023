# LMU-Database-Systems-Fall2023
Project for a few unlikely undergrad and graduate students

# Project Setup
1. Install [python](https://www.python.org/downloads/)
2. Install [pip](https://pip.pypa.io/en/stable/installation/)
3. Open a command window or terminal in the "root" of the repository (folder)
4. `python -m venv venv` to create a virtual environment in folder `./venv`
5. `source venv/bin/activate` or `venv\Scripts\activate` for windows
6. `pip install -r requirements.txt`
7. Create a `.env` file with the required connection strings (see below)
8. Now you can run any of the different scripts in `src` like `python ./src/test_connection.py`

## .env
In order to keep usernames and passwords a secret we use an ignored `.env` file.
In the Loyola Marymount Group we have a group channel which the `.env` is stored.
Feel free to message me TFlexSoom (aka Tristan Hilbert) for access.