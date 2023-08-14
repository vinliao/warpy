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


def channel_volume(
    t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
    t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
    limit: int = 5,
) -> dict[str, Any]:
    q_volume = f"""
    SELECT channel_id, COUNT(*) as count
    FROM read_parquet('data/casts.parquet')
    WHERE timestamp >= {t1} AND timestamp < {t2} AND channel_id IS NOT NULL
    GROUP BY channel_id
    ORDER BY count DESC
    LIMIT {limit};
    """

    q_reaction_avg = f"""
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

    df_volume = indexer.execute_query_df(q_volume)
    df_reaction_avg = indexer.execute_query_df(q_reaction_avg)

    return {
        "volume": df_volume.to_dict(orient="records"),
        "reaction_average": df_reaction_avg.to_dict(orient="records"),
    }


def cast_reaction_volume(
    t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
    t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
) -> dict[str, Any]:
    q1 = "SELECT timestamp FROM read_parquet('data/casts.parquet') "
    q1 += f"WHERE timestamp >= {t1} AND timestamp < {t2}"
    ts1 = indexer.execute_query(q1)

    q2 = "SELECT timestamp FROM read_parquet('data/reactions.parquet') "
    q2 += f"WHERE timestamp >= {t1} AND timestamp < {t2}"
    ts2 = indexer.execute_query(q2)

    def daily_bucket(xs: list[int]) -> list[tuple[datetime.date, int]]:
        date_list = [datetime.datetime.utcfromtimestamp(x / 1000.0).date() for x in xs]
        date_counts = Counter(date_list)
        result = sorted([(date, count) for date, count in date_counts.items()])
        return result

    return {"casts": daily_bucket(ts1), "reactions": daily_bucket(ts2)}


def popular_users(
    t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
    t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
    limit: int = 20,
) -> list[dict[Hashable, Any]]:
    q = f"""
    SELECT c.author_fid AS fid, COUNT(*) AS count
    FROM read_parquet('data/reactions.parquet') r
    INNER JOIN read_parquet('data/casts.parquet') c
    ON c.hash = r.target_hash AND r.timestamp >= {t1} AND r.timestamp < {t2}
    GROUP BY c.author_fid
    ORDER BY count DESC
    LIMIT {limit}
    """
    df = indexer.execute_query_df(q)
    df["username"] = df["fid"].apply(indexer.get_username_by_fid)

    return df.to_dict(orient="records")


def activation_table(
    t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
    t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
) -> list[dict[Hashable, Any]]:
    q = f"""
    SELECT 
        DATE_TRUNC('week', TIMESTAMP 'epoch' + registered_at * INTERVAL '1 millisecond') AS week,
        COUNT(fid) AS total_registered,
        CAST(SUM(CASE WHEN cast_count = 0 THEN 1 ELSE 0 END) AS INT) AS cast_0,
        CAST(SUM(CASE WHEN cast_count >= 1 THEN 1 ELSE 0 END) AS INT) AS cast_1_plus,
        CAST(SUM(CASE WHEN cast_count >= 5 THEN 1 ELSE 0 END) AS INT) AS cast_5_plus,
        CAST(SUM(CASE WHEN cast_count >= 10 THEN 1 ELSE 0 END) AS INT) AS cast_10_plus,
        CAST(SUM(CASE WHEN cast_count >= 25 THEN 1 ELSE 0 END) AS INT) AS cast_25_plus,
        CAST(SUM(CASE WHEN cast_count >= 50 THEN 1 ELSE 0 END) AS INT) AS cast_50_plus,
        CAST(SUM(CASE WHEN cast_count >= 100 THEN 1 ELSE 0 END) AS INT) AS cast_100_plus,
        CAST(SUM(CASE WHEN cast_count >= 250 THEN 1 ELSE 0 END) AS INT) AS cast_250_plus
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

    df = indexer.execute_query_df(q)
    df["week"] = df["week"].dt.strftime("%Y-%m-%d")
    return df.to_dict(orient="records")
