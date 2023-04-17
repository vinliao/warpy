import asyncio
import os
import time

import typer
from dotenv import load_dotenv, set_key
from sqlalchemy import create_engine

from indexer.casts import main as cast_indexer_main
from indexer.ensdata import main as ensdata_indexer_main
from indexer.eth import main as eth_indexer_main
from indexer.reactions import main as reaction_indexer_main
from indexer.user_eth_association import main as user_eth_association_main
from indexer.users import main as user_indexer_main
from packager.download import main as downloader_main
from packager.package import main as packager_main
from packager.upload import main as uploader_main
from utils.models import Base
from utils.query import execute_natural_language_query, execute_raw_sql

db_path = "datasets/datasets.db"
engine = create_engine(f"sqlite:///{db_path}")

if not os.path.exists(os.path.dirname(db_path)):
    os.makedirs(os.path.dirname(db_path))

if not os.path.exists(db_path):
    Base.metadata.create_all(engine)


load_dotenv()
warpcast_hub_key = os.getenv("WARPCAST_HUB_KEY")
openai_api_key = os.getenv("OPENAI_API_KEY")
alchemy_api_key = os.getenv("ALCHEMY_API_KEY")

app = typer.Typer()

indexer_app = typer.Typer()
env_app = typer.Typer()
app.add_typer(indexer_app, name="indexer")
app.add_typer(env_app, name="env")


def update_warpcast_key():
    dotenv_path = ".env"
    global warpcast_hub_key
    print(f"Current Warpcast Hub key: {warpcast_hub_key}")
    new_warpcast_hub_key = typer.prompt("Enter your new Warpcast Hub key")
    set_key(dotenv_path, "WARPCAST_HUB_KEY", new_warpcast_hub_key)
    warpcast_hub_key = new_warpcast_hub_key


def update_openai_key():
    dotenv_path = ".env"
    global openai_api_key
    print(f"Current OpenAI API key: {openai_api_key}")
    new_openai_api_key = typer.prompt("Enter your new OpenAI API key")
    set_key(dotenv_path, "OPENAI_API_KEY", new_openai_api_key)
    openai_api_key = new_openai_api_key


def update_alchemy_key():
    dotenv_path = ".env"
    global alchemy_api_key
    print(f"Current Alchemy key: {alchemy_api_key}")
    new_alchemy_api_key = typer.prompt("Enter your new Alchemy API key")
    set_key(dotenv_path, "ALCHEMY_API_KEY", new_alchemy_api_key)
    alchemy_api_key = new_alchemy_api_key


def update_env_variables():
    update_warpcast_key()
    update_openai_key()
    update_alchemy_key()


@env_app.command("all")
def init_all():
    """Initialize the environment variables."""
    if warpcast_hub_key or openai_api_key or alchemy_api_key:
        typer.echo("Environment variables detected:")
        typer.echo(f"WARPCAST_HUB_KEY: {warpcast_hub_key}")
        typer.echo(f"OPENAI_API_KEY: {openai_api_key}")
        typer.echo(f"ALCHEMY_API_KEY: {alchemy_api_key}")
        if typer.confirm("Do you want to overwrite the existing values?"):
            update_env_variables()
    else:
        update_env_variables()


@env_app.command("warpcast")
def init_warpcast():
    """Initialize the Warpcast Hub key."""
    update_warpcast_key()


@env_app.command("openai")
def init_openai():
    """Initialize the OpenAI API key."""
    update_openai_key()


@env_app.command("alchemy")
def init_alchemy():
    """Initialize the Alchemy API key."""
    update_alchemy_key()


@indexer_app.command("all")
def refresh_all_data():
    """Refresh all data in the DB."""
    if not warpcast_hub_key or not alchemy_api_key:
        print(
            "Error: you need to set the environment variables WARPCAST_HUB_KEY and ALCHEMY_API_KEY. Run `python main.py env all` to do so."
        )
        return
    asyncio.run(user_indexer_main())
    cast_indexer_main()
    asyncio.run(eth_indexer_main())


@indexer_app.command("user")
def refresh_user_data():
    """Refresh user data."""
    if not warpcast_hub_key:
        print(
            "Error: you need to set the environment variable WARPCAST_HUB_KEY. Run `python main.py env warpcast` to do so."
        )
        return

    asyncio.run(user_indexer_main(engine))


@indexer_app.command("cast")
def refresh_cast_data():
    """Refresh cast data."""
    if not warpcast_hub_key:
        print(
            "Error: you need to set the environment variable WARPCAST_HUB_KEY. Run `python main.py env warpcast` to do so."
        )
        return

    cast_indexer_main(engine)


@indexer_app.command("reaction")
def refresh_reaction_data():
    """Refresh reactions data."""
    if not warpcast_hub_key:
        print(
            "Error: you need to set the environment variable WARPCAST_HUB_KEY. Run `python main.py env warpcast` to do so."
        )
        return

    asyncio.run(reaction_indexer_main(engine))


@indexer_app.command("eth")
def refresh_eth_data():
    """Refresh onchain Ethereum data."""
    if not alchemy_api_key:
        print(
            "Error: you need to set the environment variable ALCHEMY_API_KEY. Run `python main.py env alchemy` to do so."
        )
        return

    asyncio.run(eth_indexer_main(engine))


@indexer_app.command("ens")
def refresh_ens_data():
    """Refresh ENS data."""

    asyncio.run(ensdata_indexer_main(engine))


@indexer_app.command("usereth")
def make_user_eth_association():
    """Make user-eth association table."""

    user_eth_association_main(engine)


@app.command()
def download():
    """Download datasets."""
    downloader_main()


@app.command()
def upload():
    """Upload datasets."""
    uploader_main()


@app.command()
def package():
    """Package and zip datasets."""
    packager_main()


@app.command()
def query(
    query: str = typer.Argument(
        None, help="Query Farcaster data with natural language."
    ),
    raw: str = typer.Option(None, help="Query Farcaster data with SQL."),
    advanced: str = typer.Option(None, help="For testing purposes."),
    csv: bool = typer.Option(
        False, help="Save the result to a CSV file. Format: {unix_timestamp}.csv"
    ),
):
    if not openai_api_key and raw is None:
        print(
            "Error: you need to set the environment variable OPENAI_API_KEY. Run `python main.py env openai` to do so."
        )
        return

    if raw:
        if os.path.exists(raw):
            try:
                with open(raw, "r") as f:
                    raw_sql = f.read()
                    df = execute_raw_sql(engine, raw_sql)
            except Exception as e:
                typer.echo(f"Error: Could not execute SQL from file. {str(e)}")
        else:
            df = execute_raw_sql(engine, raw)
    elif query:
        df = execute_natural_language_query(engine, query)
    # elif advanced:
    #     execute_advanced_query(advanced)
    else:
        typer.echo("Please provide either --raw, --query, or --advanced option.")
        return

    if df is not None:
        print(df)

    if csv is True and df is not None:
        df.write_csv(f"{int(time.time())}.csv")


if __name__ == "__main__":
    app()
