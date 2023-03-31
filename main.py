import os
import asyncio
from dotenv import load_dotenv
import typer

from indexer.users import main as user_indexer_main
from indexer.casts import main as cast_indexer_main
from indexer.eth import main as eth_indexer_main
from packager.package import main as packager_main
from packager.download import main as downloader_main

load_dotenv()
warpcast_hub_key = os.getenv('WARPCAST_HUB_KEY')

app = typer.Typer()

indexer_app = typer.Typer()
app.add_typer(indexer_app, name="indexer")


@indexer_app.command("all")
def refresh_all_data():
    """Refresh all data in the DB."""
    asyncio.run(user_indexer_main())
    cast_indexer_main()
    asyncio.run(eth_indexer_main())


@indexer_app.command("user")
def refresh_user_data():
    """Refresh user data."""
    asyncio.run(user_indexer_main())


@indexer_app.command("cast")
def refresh_cast_data():
    """Refresh cast data."""
    cast_indexer_main()


@indexer_app.command("eth")
def refresh_eth_data():
    """Refresh onchain Ethereum data."""
    asyncio.run(eth_indexer_main())


@app.command()
def download():
    """Download datasets."""
    downloader_main()


@app.command()
def package():
    """Package and zip datasets."""
    packager_main()


if __name__ == "__main__":
    app()
