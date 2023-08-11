import asyncio
import os
import sys

import src.indexer as indexer

# TODO: probs needs cli options to run these things

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
        if len(sys.argv) < 3:
            print("Please provide a query string after --query")
            sys.exit(1)
        query = sys.argv[2]
        indexer.execute_query(query)
    else:
        print("Invalid option. Use --refresh-user, --refresh-cast, or --query")
        sys.exit(1)


if __name__ == "__main__":
    main()

# ======================================================================================
# pulse pipes
# ======================================================================================


# NOTE: how to query with "where array"
# query = """
#     SELECT images
#     FROM read_json_auto('queue/cast_warpcast.ndjson')
#     WHERE array_length(images) = 0;
# """


# def _active_ens_usernames() -> dict:
#     """
#     Pipe that returns:
#     - total active users
#     - how much of those actives have ens
#     - how much of those actives have ens and use ens as their fc username
#     """

#     def _stringify_addresses(addresses: List[str]) -> str:
#         return "', '".join(list(filter(None, addresses)))

#     q1 = "SELECT address FROM read_parquet('data/users.parquet') "
#     q1 += "WHERE is_active = true"
#     r1: List[str] = indexer.execute_query(q1)

#     q2 = "SELECT DISTINCT ens FROM read_json_auto('queue/user_ensdata.ndjson') "
#     q2 += "WHERE address IN ('" + _stringify_addresses(r1) + "')"
#     r2: List[str] = indexer.execute_query(q2)

#     q3 = "SELECT username FROM read_parquet('data/users.parquet') "
#     q3 += "WHERE username IN ('" + "', '".join(r2) + "')"
#     r3: List[str] = indexer.execute_query(q3)

#     return {
#         "active_users": len(r1),
#         "active_users_with_ens": len(r2),
#         "active_users_with_ens_username": len(r3),
#     }


# def _active_users(n=20):
#     def to_dict(xs):
#         return [
#             {
#                 "fid": fid,
#                 "count": count,
#                 "username": utils.get_username_by_fid(fid),
#             }
#             for fid, count in xs
#         ]

#     # TODO: figure out a better way than hardcoding date
#     t1 = utils.ymd_to_unixms(2023, 7, 1)
#     t2 = utils.ymd_to_unixms(2023, 8, 1)

#     query = f"""
#     WITH relevant_reactions AS (
#         SELECT target_hash
#         FROM read_parquet('data/reactions.parquet')
#         WHERE timestamp >= {t1} AND timestamp < {t2}
#     ),
#     relevant_casts AS (
#         SELECT hash, author_fid
#         FROM read_parquet('data/casts.parquet')
#         WHERE hash IN (SELECT target_hash FROM relevant_reactions)
#     )
#     SELECT relevant_casts.author_fid, COUNT(*) AS count
#     FROM relevant_casts
#     INNER JOIN relevant_reactions ON relevant_casts.hash =relevant_reactions.target_hash
#     GROUP BY relevant_casts.author_fid
#     ORDER BY count DESC
#     LIMIT {n}
#     """

#     df = indexer.execute_query_df(query)
#     return to_dict(df.to_records(index=False))


# def _cast_reaction_bucket():
#     # TODO: figure out a better way than hardcoding date
#     t1 = utils.ymd_to_unixms(2023, 7, 1)
#     t2 = utils.ymd_to_unixms(2023, 8, 1)

#     q1 = "SELECT timestamp FROM read_parquet('data/casts.parquet') "
#     q1 += f"WHERE timestamp >= {t1} AND timestamp < {t2}"
#     ts1 = utils._execute_query(q1)

#     q2 = "SELECT timestamp FROM read_parquet('data/reactions.parquet') "
#     q2 += f"WHERE timestamp >= {t1} AND timestamp < {t2}"
#     ts2 = utils._execute_query(q2)

#     def daily_bucket(xs):
#         date_list = [datetime.datetime.utcfromtimestamp(x / 1000.0).date() for x in xs]
#         date_counts = Counter(date_list)
#         result = sorted([(date, count) for date, count in date_counts.items()])

#         return result

#     return {"casts": daily_bucket(ts1), "reactions": daily_bucket(ts2)}
