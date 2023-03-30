# Warpy (Beta)

[Farcaster](https://github.com/farcasterxyz/protocol) is an Ethereum-based programmable social network. Warpy provides open-source Farcaster datasets.

Steps to use:

1. `python -m venv venv`
2. `pip install -r requirements.txt`
3. `python main.py --download`
4. Go to `example.ipynb` for examples
5. To enable English-to-SQL (with `query.py`), you need to insert your OpenAI API key in `.env`

To see what you can do with the datasets, see [example.ipynb](example.ipynb). To see the data schema, see [models.py](models.py).

Dataset latest cast timestamp: 1680169689000; dataset highest fid: 11423; dataset highest block number: 16938534; tar.gz shasum: `bc351cb1792612acc6847aecee299e0535f94c4f8e62a1a93b5a0b2f9dabeee8`.
