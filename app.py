import datetime
from collections import Counter
from typing import List

import indexer
import utils

# ======================================================================================
# pulse pipes
# ======================================================================================


# NOTE: how to query with "where array"
# query = """
#     SELECT images
#     FROM read_json_auto('queue/cast_warpcast.ndjson')
#     WHERE array_length(images) = 0;
# """


def _active_ens_usernames() -> dict:
    """
    Pipe that returns:
    - total active users
    - how much of those actives have ens
    - how much of those actives have ens and use ens as their fc username
    """

    def _stringify_addresses(addresses: List[str]) -> str:
        return "', '".join(list(filter(None, addresses)))

    q1 = "SELECT address FROM read_parquet('data/users.parquet') "
    q1 += "WHERE is_active = true"
    r1: List[str] = indexer.execute_query(q1)

    q2 = "SELECT DISTINCT ens FROM read_json_auto('queue/user_ensdata.ndjson') "
    q2 += "WHERE address IN ('" + _stringify_addresses(r1) + "')"
    r2: List[str] = indexer.execute_query(q2)

    q3 = "SELECT username FROM read_parquet('data/users.parquet') "
    q3 += "WHERE username IN ('" + "', '".join(r2) + "')"
    r3: List[str] = indexer.execute_query(q3)

    return {
        "active_users": len(r1),
        "active_users_with_ens": len(r2),
        "active_users_with_ens_username": len(r3),
    }


def _active_users(n=20):
    def to_dict(xs):
        return [
            {
                "fid": fid,
                "count": count,
                "username": utils.get_username_by_fid(fid),
            }
            for fid, count in xs
        ]

    # TODO: figure out a better way than hardcoding date
    t1 = utils.ymd_to_unixms(2023, 7, 1)
    t2 = utils.ymd_to_unixms(2023, 8, 1)

    query = f"""
    WITH relevant_reactions AS (
        SELECT target_hash
        FROM read_parquet('data/reactions.parquet')
        WHERE timestamp >= {t1} AND timestamp < {t2}
    ),
    relevant_casts AS (
        SELECT hash, author_fid
        FROM read_parquet('data/casts.parquet')
        WHERE hash IN (SELECT target_hash FROM relevant_reactions)
    )
    SELECT relevant_casts.author_fid, COUNT(*) AS count
    FROM relevant_casts
    INNER JOIN relevant_reactions ON relevant_casts.hash =relevant_reactions.target_hash
    GROUP BY relevant_casts.author_fid
    ORDER BY count DESC
    LIMIT {n}
    """

    df = indexer.execute_query_df(query)
    return to_dict(df.to_records(index=False))


def _cast_reaction_bucket():
    # TODO: figure out a better way than hardcoding date
    t1 = utils.ymd_to_unixms(2023, 7, 1)
    t2 = utils.ymd_to_unixms(2023, 8, 1)

    q1 = "SELECT timestamp FROM read_parquet('data/casts.parquet') "
    q1 += f"WHERE timestamp >= {t1} AND timestamp < {t2}"
    ts1 = utils._execute_query(q1)

    q2 = "SELECT timestamp FROM read_parquet('data/reactions.parquet') "
    q2 += f"WHERE timestamp >= {t1} AND timestamp < {t2}"
    ts2 = utils._execute_query(q2)

    def daily_bucket(xs):
        date_list = [datetime.datetime.utcfromtimestamp(x / 1000.0).date() for x in xs]
        date_counts = Counter(date_list)
        result = sorted([(date, count) for date, count in date_counts.items()])

        return result

    return {"casts": daily_bucket(ts1), "reactions": daily_bucket(ts2)}
