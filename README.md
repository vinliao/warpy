# Warpy (Beta)

[Farcaster](https://github.com/farcasterxyz/protocol) is an Ethereum-based programmable social network. Warpy provides open-source Farcaster datasets.

To see what you can do with the datasets, see [example.ipynb](example.ipynb). To see the data schema, see [models.py](models.py).

One-command install on MacOS and Linux machines: run `make install`. Windows machines: it's recommended to use [WSL](https://docs.microsoft.com/en-us/windows/wsl/install-win10) and run `make install` there. If you don't want to use `make`, you can run `pip install -r requirements.txt` manually inside a virtual environment.

All of `main.py` commands:

```
Usage: main.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help    Show this message and exit.

Commands:
  download  Download datasets.
  env
  indexer
  package   Package and zip datasets.
  query
  upload    Upload datasets.
```

Some example commands:

```bash
# Download the latest dataset
python main.py download

# Index the latest casts
python main.py indexer cast

# Set OpenAI environment variables
python main.py env openai

# Query the database with English
python main.py query "get latest cast by dwr"
```

Dataset latest cast timestamp: 1680611427000; dataset highest fid: 11652; dataset highest block number: 16974689; tar.gz shasum: `957d812e726019ddb85af4b9f90ede7b55cdae44d2b78a530b9f0a70eb25d0e7`.
