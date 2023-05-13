import asyncio
import gc
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from sqlalchemy import create_engine
from uvicorn import Config, Server

from src.indexer.casts_warpcast import main as casts_warpcast_main
from src.indexer.reactions_warpcast import main as reactions_warpcast_main
from src.indexer.users_searchcaster import main as users_searchcaster_main
from src.indexer.users_warpcast import main as users_warpcast_main

load_dotenv()

PLANETSCALE_URL = os.getenv("PLANETSCALE_MYSQL_URL")
if not PLANETSCALE_URL:
    raise Exception("PLANETSCALE_MYSQL_URL is not set in .env")

engine = create_engine(PLANETSCALE_URL)


def run_casts_warpcast():
    casts_warpcast_main(engine)


async def run_index_users():
    users_warpcast_main(engine)
    await users_searchcaster_main(engine)
    engine.dispose()
    gc.collect()


async def run_index_casts():
    casts_warpcast_main(engine)
    engine.dispose()
    gc.collect()


async def run_index_reactions():
    await reactions_warpcast_main(engine)
    engine.dispose()
    gc.collect()


scheduler = AsyncIOScheduler()

# TODO: find optimal interval schedule
scheduler.add_job(run_index_users, "interval", hours=17)
scheduler.add_job(run_index_casts, "interval", hours=19)
scheduler.add_job(run_index_reactions, "interval", hours=23)


async def start_scheduler():
    scheduler.start()
    await asyncio.sleep(float("inf"))  # Sleep indefinitely


async def run_api_server():
    config = Config(
        "src.app.api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        log_level="info",
    )
    server = Server(config)
    await server.serve()


async def main():
    indexer_task = asyncio.create_task(start_scheduler())
    api_task = asyncio.create_task(run_api_server())
    await asyncio.gather(indexer_task, api_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
