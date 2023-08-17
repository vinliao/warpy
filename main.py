import asyncio
import os
import sys

import src.indexer as indexer
import src.pipes as pipes

# ======================================================================================
# refresher
# ======================================================================================


async def refresh_user() -> None:
    quwf = "queue/user_warpcast.ndjson"
    qusf = "queue/user_searchcaster.ndjson"
    quef = "queue/user_ensdata.ndjson"
    uf = "data/users.parquet"

    # NOTE: to refresh from scratch, delete the queue and the parquet
    refresh_everything = False
    if refresh_everything:
        os.remove(quwf)
        os.remove(uf)
        os.remove(qusf)  # uncomment to renew searchcaster data

    # TODO: caller UX is still bad, so much timeout!
    fids = indexer.QueueProducer.user_warpcast(quwf, uf)
    await indexer.BatchFetcher.user_warpcast(fids=fids, n=100, out=quwf)
    fids = indexer.QueueProducer.user_searchcaster(quwf, qusf)
    await indexer.BatchFetcher.user_searchcaster(fids=fids, n=125, out=qusf)
    addrs = indexer.QueueProducer.user_ensdata(qusf, quef)
    await indexer.BatchFetcher.user_ensdata(addrs, n=50, out=quef)
    df = indexer.Merger.user(quwf, qusf, uf)
    df.to_parquet(uf, index=False)


async def refresh_cast() -> None:
    cf = "data/casts.parquet"
    qf = "queue/cast_warpcast.ndjson"

    # edit this cursor to continue from a certain point
    # None means refresh latest until the tail of casts in database
    cursor = None
    await indexer.BatchFetcher.cast_warpcast(cursor)
    df = indexer.Merger.cast(qf, cf)
    df.to_parquet(cf, index=False)


async def refresh_reactions() -> None:
    cf = "data/casts.parquet"
    t1 = indexer.TimeConverter.ago_to_unixms(factor="days", units=60)
    t2 = indexer.TimeConverter.ms_now()
    hashes = indexer.QueueProducer.reaction_warpcast(t1, t2, cf)
    await indexer.BatchFetcher.reaction_warpcast(hashes)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python main.py --refresh-user, --refresh-cast, or --query")
        sys.exit(1)

    option = sys.argv[1]
    if option == "--refresh-user":
        asyncio.run(refresh_user())
    elif option == "--refresh-cast":
        asyncio.run(refresh_cast())
    elif option == "--refresh-reaction":
        asyncio.run(refresh_reactions())
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
