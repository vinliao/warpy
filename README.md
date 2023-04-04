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

Dataset latest cast timestamp: 1680611427000; dataset highest fid: 11652; dataset highest block number: 16974689; tar.gz shasum: `957d812e726019ddb85af4b9f90ede7b55cdae44d2b78a530b9f0a70eb25d0e7`.
