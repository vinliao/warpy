import datetime
from collections import Counter
from typing import Any, List

import duckdb

import utils

con = duckdb.connect(database=":memory:")


def _active_new_users(n=20):
    owa = utils.weeks_ago_to_unixms(1)
    twa = utils.weeks_ago_to_unixms(2)

    query = f"""
        SELECT author_fid, COUNT(*) AS count
        FROM read_parquet('data/casts.parquet')
        WHERE author_fid IN (
            SELECT fid
            FROM read_parquet('data/users.parquet')
            WHERE registered_at < {owa} AND registered_at > {twa}
        )
        GROUP BY author_fid
        ORDER BY count DESC
        LIMIT {n}
    """
    result = con.execute(query).fetchall()
    return [(utils.get_username_by_fid(fid), count) for fid, count in result]


# def _active_users(n=20):
#     def to_username(xs):
#         return list(map(lambda x: (utils.get_username_by_fid(x[0]), x[1]), xs))

#     # t = utils.weeks_ago_to_unixms(4)
#     # where_t = f"WHERE timestamp > {t}"

#     t1 = utils.ymd_to_unixms(2023, 7, 1)
#     t2 = utils.ymd_to_unixms(2023, 8, 1)

#     queries = {
#         "casts_sent": f"""
#             SELECT author_fid, COUNT(*) AS count
#             FROM read_parquet('data/casts.parquet')
#             WHERE timestamp >= {t1} AND timestamp < {t2}
#             AND parent_hash IS NULL
#             GROUP BY author_fid
#             ORDER BY count DESC
#             LIMIT {n}
#         """,
#         "reply_sent": f"""
#             SELECT author_fid, COUNT(*) AS count
#             FROM read_parquet('data/casts.parquet')
#             WHERE timestamp >= {t1} AND timestamp < {t2}
#             AND parent_hash IS NOT NULL
#             GROUP BY author_fid
#             ORDER BY count DESC
#             LIMIT {n}
#         """,
#         "reactions_sent": f"""
#             SELECT reactor_fid AS author_fid, COUNT(*) AS count
#             FROM read_parquet('data/reactions.parquet')
#             WHERE timestamp >= {t1} AND timestamp < {t2}
#             GROUP BY reactor_fid
#             ORDER BY count DESC
#             LIMIT {n}
#         """,
#         "reactions_received": f"""
#         SELECT author_fid, COUNT(*) AS count
#         FROM read_parquet('data/casts.parquet')
#         WHERE hash IN (
#             SELECT target_hash
#             FROM read_parquet('data/reactions.parquet')
#             WHERE timestamp >= {t1} AND timestamp < {t2}
#         )
#         GROUP BY author_fid
#         ORDER BY count DESC
#         LIMIT {n}
#         """,
#     }


#     dfs = {key: con.execute(sql).fetchdf() for key, sql in queries.items()}
#     return {
#         key: to_username(df.itertuples(index=False, name=None))
#         for key, df in dfs.items()
#     }
def _active_users(n=20):
    def to_username(xs):
        return list(map(lambda x: (utils.get_username_by_fid(x[0]), x[1]), xs))

    t1 = utils.ymd_to_unixms(2023, 7, 1)
    t2 = utils.ymd_to_unixms(2023, 8, 1)

    # 1. Query for top n users by reactions received
    reaction_query = f"""
        SELECT author_fid, COUNT(*) AS count
        FROM read_parquet('data/casts.parquet')
        WHERE hash IN (
            SELECT target_hash
            FROM read_parquet('data/reactions.parquet')
            WHERE timestamp >= {t1} AND timestamp < {t2}
        )
        GROUP BY author_fid
        ORDER BY count DESC
        LIMIT {n}
    """
    reaction_df = con.execute(reaction_query).fetchdf()
    top_reacted_fids = reaction_df["author_fid"].tolist()

    # Convert list of fids to a string for SQL query
    fids_str = ", ".join(str(fid) for fid in top_reacted_fids)

    # 2. Queries for casts sent, reply sent, and reactions sent, filtered by the users selected above
    other_queries = {
        "casts_sent": f"""
            SELECT author_fid, COUNT(*) AS count
            FROM read_parquet('data/casts.parquet')
            WHERE timestamp >= {t1} AND timestamp < {t2}
            AND author_fid IN ({fids_str})
            GROUP BY author_fid
            ORDER BY count DESC
        """,
    }

    other_dfs = {key: con.execute(sql).fetchdf() for key, sql in other_queries.items()}
    return {
        "reactions_received": to_username(
            reaction_df.itertuples(index=False, name=None)
        ),
        **{
            key: to_username(df.itertuples(index=False, name=None))
            for key, df in other_dfs.items()
        },
    }


def _cast_reaction_count(weeks=4):
    # week 0 = last week until now; week 1 = two weeks ago until last week, etc.
    # visualize result of this fn with bar chart, two bars per week
    def _count_reaction_cast(week):
        t_from = utils.weeks_ago_to_unixms(week + 1)
        t_until = utils.weeks_ago_to_unixms(week)
        query = f"""
            SELECT
                (SELECT COUNT(*) FROM read_parquet('data/casts.parquet')
                 WHERE timestamp BETWEEN {t_from} AND {t_until}) AS cast,
                (SELECT COUNT(*) FROM read_parquet('data/reactions.parquet')
                 WHERE timestamp BETWEEN {t_from} AND {t_until}) AS reaction
        """
        return week, con.execute(query).fetchone()

    return {
        week: (cast, reaction)
        for week, (cast, reaction) in map(_count_reaction_cast, range(weeks))
    }


def execute_query(query: str) -> List[Any]:
    con = duckdb.connect(database=":memory:")
    return list(filter(None, [x[0] for x in con.execute(query).fetchall()]))


def _active_ens_usernames() -> dict:
    # TODO: can be simplified it seems
    def _stringify_addresses(addresses: List[str]) -> str:
        return "', '".join(list(filter(None, addresses)))

    q1 = "SELECT address FROM read_parquet('data/users.parquet') WHERE is_active = true"
    r1: List[str] = execute_query(q1)
    q2 = (
        "SELECT ens FROM read_json_auto('queue/user_ensdata.ndjson') WHERE address"
        " IN ('"
        + _stringify_addresses(r1)
        + "')"
    )
    r2: List[str] = execute_query(q2)
    q3 = (
        "SELECT username FROM read_parquet('data/users.parquet') WHERE username LIKE"
        " '%.eth' AND is_active = true"
    )
    r3 = execute_query(q3)

    return {
        "active_users": len(r1),
        "active_users_with_ens": len(r2),
        "active_users_with_ens_username": len(r3),
    }


def _cast_reaction_bucket():
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


print(_active_users(10))

pipes = {
    "weekly": [_active_new_users, _active_users, _cast_reaction_count],
    "biweekly": [_cast_reaction_count],
}
