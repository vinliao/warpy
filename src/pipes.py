import datetime
from collections import Counter
from typing import Any, Hashable

import src.indexer as indexer

# NOTE: how to query with "where array"
# query = """
#     SELECT images
#     FROM read_json_auto('queue/cast_warpcast.ndjson')
#     WHERE array_length(images) = 0;
# """

# F9766E
# 00BFC4


def channel_volume(
    t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
    t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
    limit: int = 5,
) -> dict[str, Any]:
    query_volume = f"""
    SELECT channel_id, COUNT(*) as count
    FROM read_parquet('data/casts.parquet')
    WHERE timestamp >= {t1} AND timestamp < {t2} AND channel_id IS NOT NULL
    GROUP BY channel_id
    ORDER BY count DESC
    LIMIT {limit};
    """

    query_reaction_avg = f"""
    SELECT c.channel_id,
           COUNT(r.hash) / CAST(COUNT(DISTINCT c.hash) AS FLOAT) as reaction_per_cast
    FROM read_parquet('data/casts.parquet') AS c
    LEFT JOIN read_parquet('data/reactions.parquet') AS r ON c.hash = r.target_hash
    WHERE c.timestamp >= {t1} AND c.timestamp < {t2} AND c.channel_id IS NOT NULL
    GROUP BY c.channel_id
    HAVING COUNT(DISTINCT c.hash) > 100
    ORDER BY reaction_per_cast DESC
    LIMIT {limit};
    """

    df_volume = indexer.execute_query_df(query_volume)
    df_reaction_avg = indexer.execute_query_df(query_reaction_avg)

    return {
        "volume": df_volume.to_dict(orient="records"),
        "reaction_average": df_reaction_avg.to_dict(orient="records"),
    }


def cast_reaction_volume(
    t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
    t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
) -> dict[str, Any]:
    def daily_bucket(table: str) -> list[tuple[datetime.date, int]]:
        def make_date(timestamp: int) -> datetime.date:
            return datetime.datetime.utcfromtimestamp(timestamp / 1000.0).date()

        query = f"SELECT timestamp FROM read_parquet('data/{table}.parquet') "
        query += f"WHERE timestamp >= {t1} AND timestamp < {t2}"
        timestamps = indexer.execute_query(query)
        dates = [make_date(ts) for ts in timestamps]
        return sorted(Counter(dates).items())

    return {"casts": daily_bucket("casts"), "reactions": daily_bucket("reactions")}


def popular_users(
    t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
    t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
    limit: int = 20,
) -> list[dict[Hashable, Any]]:
    query = f"""
    SELECT c.author_fid AS fid, COUNT(*) AS count
    FROM read_parquet('data/reactions.parquet') r
    INNER JOIN read_parquet('data/casts.parquet') c
    ON c.hash = r.target_hash AND r.timestamp >= {t1} AND r.timestamp < {t2}
    GROUP BY c.author_fid
    ORDER BY count DESC
    LIMIT {limit}
    """
    df = indexer.execute_query_df(query)
    df["username"] = df["fid"].apply(indexer.get_username_by_fid)
    return df.to_dict(orient="records")


def activation_table(
    t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
    t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
) -> list[dict[Hashable, Any]]:
    counts = [0, 1, 5, 10, 25, 50, 100, 250]
    query_counts = ", ".join(
        [
            f"CAST(SUM(CASE WHEN cast_count >= {c} THEN 1 ELSE 0 END) AS INT)"
            f" AS cast_{c}_plus"
            for c in counts
        ]
    )

    query = f"""
    SELECT 
        DATE_TRUNC('week', TIMESTAMP 'epoch' + registered_at * INTERVAL '1 millisecond')
            AS week,
        COUNT(fid) AS total_registered,
        {query_counts}
    FROM (
        SELECT 
            u.fid, 
            u.registered_at,
            COUNT(c.timestamp) AS cast_count
        FROM read_parquet('data/users.parquet') u
        LEFT JOIN read_parquet('data/casts.parquet') c ON u.fid = c.author_fid
        WHERE u.registered_at >= {t1} AND u.registered_at < {t2}
        GROUP BY u.fid, u.registered_at
    ) subquery
    GROUP BY week
    """

    df = indexer.execute_query_df(query)
    df["week"] = df["week"].dt.strftime("%Y-%m-%d")
    return df.to_dict(orient="records")
