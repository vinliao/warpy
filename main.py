import asyncio
import os
import sys

import src.indexer as indexer

# ======================================================================================
# refresher
# ======================================================================================


async def refresh_user() -> None:
    quwf = "queue/user_warpcast.ndjson"
    qusf = "queue/user_searchcaster.ndjson"
    uf = "data/users.parquet"

    # NOTE: to refresh from scratch, delete the queue and the parquet
    refresh_everything = False
    if refresh_everything:
        os.remove(quwf)
        os.remove(uf)

    print("Fetching from Warpcast")
    await indexer.QueueConsumer.user_warpcast(1000)
    print("Fetching from Searchcaster")
    await indexer.QueueConsumer.user_searchcaster(125)
    print("Fetching from Ensdata")
    await indexer.QueueConsumer.user_ensdata(50)

    df = indexer.Merger.user(quwf, qusf, uf)
    df.to_parquet(uf, index=False)


async def refresh_cast() -> None:
    cf = "data/casts.parquet"
    qf = "queue/cast_warpcast.ndjson"

    # edit this cursor to continue from a certain point
    # None means refresh latest until the tail of casts in database
    cursor = None

    await indexer.QueueConsumer.cast_warpcast(cursor)

    df = indexer.Merger.cast(qf, cf)
    df.to_parquet(cf, index=False)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python main.py --refresh-user, --refresh-cast, or --query")
        sys.exit(1)

    option = sys.argv[1]
    if option == "--refresh-user":
        asyncio.run(refresh_user())
    elif option == "--refresh-cast":
        asyncio.run(refresh_cast())
    elif option == "--query":
        filename = sys.argv[2] if len(sys.argv) > 2 else "query.sql"
        with open(filename, "r") as file:
            query = file.read()
        print(indexer.execute_query_df(query))
    else:
        print("Invalid option. Use --refresh-user, --refresh-cast, or --query")
        sys.exit(1)


if __name__ == "__main__":
    main()
