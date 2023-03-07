# Warpy (Beta)

Warpy is a Python environment for doing data science with Farcaster data.

To get started:

1. `pip -r requirements.txt`
2. Download `data.db` from the link below, put in root directory of Warpy
3. Go to example.ipynb, run all cells
4. Start querying the SQLite database

It includes indexers (`users.py`, `casts.py` still TODO) that queries Farcaster data from various sources, and stores it in a SQLite database. To see what the tables and columns, check out `models.py`.

Download link to the SQLite DB: [here](https://www.dropbox.com/s/7xt62rc2cgbx65e/data.db?dl=0) (sha256sum: `62b5b48aaf7551be18a5a50bcf7440047699a1fa9047addc130f7645cb9bce48`).

(Note: more data will be added in the future!)