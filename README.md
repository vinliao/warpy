# Warpy (Beta)

Warpy is a Python environment for doing data science with Farcaster data.

To get started:

1. Make a Python virtual environment (ex: `python3 -m venv venv`)
2. Install dependencies `pip -r requirements.txt`
3. Go to example.ipynb, run all cells
4. Voila, a notebook with Farcater data!

It includes indexers (`users.py`, `casts.py` still TODO) that queries Farcaster data from various sources, and stores it in a SQLite database. To see what the tables and columns, check out `models.py`.

(Note: more data will be added in the future!)