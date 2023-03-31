# Warpy (Beta)

[Farcaster](https://github.com/farcasterxyz/protocol) is an Ethereum-based programmable social network. Warpy provides open-source Farcaster datasets.

One-command install: run `make install`. To see what you can do with the datasets, see [example.ipynb](example.ipynb). To see the data schema, see [models.py](models.py).

All of `main.py` commands:

```
Usage: main.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help    Show this message and exit.

Commands:
  download  Download datasets.
  indexer
  init      Initialize the environment with OpenAI and Warpcast Hub keys.
  package   Package and zip datasets.
  query
```

Some example commands:

```bash
# Note: environment variables must be set before running any commands

# Initialize the environment with OpenAI and Warpcast Hub keys
python main.py init

# Download the latest dataset
python main.py download

# Index the latest casts
python main.py indexer cast

# Query the database with English
python main.py query "get latest cast by dwr"
```

Dataset latest cast timestamp: 1680169689000; dataset highest fid: 11423; dataset highest block number: 16938534; tar.gz shasum: `bc351cb1792612acc6847aecee299e0535f94c4f8e62a1a93b5a0b2f9dabeee8`.
