# Warpy (Beta)

Warpy is a Python environment for doing data science with Farcaster data.

To get started:

1. `pip -r requirements.txt`
2. Go to example.ipynb, run all cells
3. Start querying the SQLite database

It includes indexers (`users.py`, `casts.py` still TODO) that queries Farcaster data from various sources, and stores it in a SQLite database. To see what the tables and columns, check out `models.py`.
