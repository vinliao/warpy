import os
from typing import Dict, Generator, List, Tuple

import duckdb
import pandas as pd
import requests

import utils


def download_fip2_ndjson(filename: str = "data/fip2.ndjson") -> None:
    url = "https://pub-3916d8c82abb435eb70175747fdc2119.r2.dev/fip2.ndjson"
    if os.path.exists(filename):
        print(f"{filename} already exists.")
        return

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as file:
            file.write(response.content)


def get_url_by_id(channel_id: str) -> str:
    f = "data/fip2.ndjson"
    download_fip2_ndjson(f)
    query = f"select parent_url from read_json_auto('{f}') "
    query += f"where channel_id = '{channel_id}'"
    con = duckdb.connect()
    r = con.execute(query).fetchone()
    assert isinstance(r, tuple)
    assert isinstance(r[0], str)
    return r[0]


def get_id_by_url(url: str) -> str:
    f = "data/fip2.ndjson"
    download_fip2_ndjson(f)
    query = f"select channel_id from read_json_auto('{f}') "
    query += f"where parent_url = '{url}'"
    con = duckdb.connect()
    r = con.execute(query).fetchone()
    assert isinstance(r, tuple)
    assert isinstance(r[0], str)
    return r[0]


# figure out how to cache the db so i don't have to keep running on unimportant queries
def execute_query_df(
    query: str, pg_url: str = "postgresql://app:password@localhost:6541/hub"
) -> pd.DataFrame:
    # NOTE: must have replicator running, maybe have a shell script or something
    return pd.read_sql(query, pg_url, dtype_backend="pyarrow")


def channel_volume(start: int, end: int, limit: int = 10) -> pd.DataFrame:
    t1 = f"to_timestamp({start / 1000})"
    t2 = f"to_timestamp({end / 1000})"

    query = f"""
    SELECT parent_url, COUNT(*) as count
    FROM casts
    WHERE timestamp >= {t1} AND timestamp < {t2} AND parent_url IS NOT NULL
    GROUP BY parent_url
    ORDER BY count DESC
    LIMIT {limit};
    """
    return execute_query_df(query)


def channel_volume_table(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 6, 1)
) -> pd.DataFrame:
    # NOTE: 2023/06/01 is first-ever channel cast
    end = utils.TimeConverter.ms_now()

    url_id_map = pd.read_json("data/fip2.ndjson", lines=True)
    url_id_map = url_id_map.set_index("parent_url").to_dict()["channel_id"]
    get_channel_id = lambda url: url_id_map[url]

    def generate_intervals(
        start_timestamp: int, end_timestamp: int
    ) -> Generator[Tuple[int, int], None, None]:
        one_week_ms = utils.TimeConverter.to_ms("weeks", 1)
        while start_timestamp < end_timestamp:
            next_timestamp = start_timestamp + one_week_ms
            yield start_timestamp, next_timestamp
            start_timestamp = next_timestamp

    def process_interval(interval: Tuple[int, int]) -> List[str]:
        start, end = interval
        df = channel_volume(start=start, end=end, limit=10)
        df = df[df["parent_url"].isin(list(url_id_map.keys()))]

        count_str = " (" + df["count"].astype(str) + " casts)"
        df["result"] = "f/" + df["parent_url"].apply(get_channel_id) + count_str
        return [utils.TimeConverter.unixms_to_ymd(start)] + list(df["result"])

    all_records = list(map(process_interval, generate_intervals(start, end)))
    columns = ["Date"] + [f"Rank {i}" for i in range(1, 11)]
    return pd.DataFrame(all_records[::-1], columns=columns)


def popular_users(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 7, 1),
    end: int = utils.TimeConverter.ymd_to_unixms(2023, 8, 1),
    limit: int = 20,
) -> pd.DataFrame:
    def get_username_map() -> Dict[int, str]:
        query = """
        SELECT fid, value AS username
        FROM user_data
        WHERE type = 6
        """

        df = execute_query_df(query)
        return dict(zip(df["fid"], df["username"]))

    t1 = f"to_timestamp({start / 1000})"
    t2 = f"to_timestamp({end / 1000})"
    query = f"""
        SELECT c.fid AS fid, COUNT(*) AS count
        FROM reactions r
        INNER JOIN casts c
        ON c.hash = r.target_hash AND r.timestamp >= {t1} AND r.timestamp < {t2}
        GROUP BY c.fid
        ORDER BY count DESC
        LIMIT {limit}
    """

    df = execute_query_df(query)
    username_map = get_username_map()
    df["username"] = df["fid"].apply(lambda fid: username_map.get(fid, "unknown"))
    return df


def cast_reaction_volume(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 7, 1),
    end: int = utils.TimeConverter.ymd_to_unixms(2023, 8, 1),
) -> pd.DataFrame:
    def daily_bucket(table: str) -> pd.DataFrame:
        t1 = f"to_timestamp({start / 1000})"
        t2 = f"to_timestamp({end / 1000})"
        query = f"""
            SELECT date_trunc('day', timestamp) AS date, COUNT(*) AS count
            FROM {table}
            WHERE timestamp >= {t1} AND timestamp < {t2}
            GROUP BY date
            ORDER BY date
        """
        return execute_query_df(query)

    c_df = daily_bucket("casts")
    r_df = daily_bucket("reactions")

    return pd.merge(c_df, r_df, on="date", suffixes=("_casts", "_reactions"))



# TODO;
# def activation_table(
#     start: int = utils.TimeConverter.ymd_to_unixms(2023, 7, 1),
#     end: int = utils.TimeConverter.ymd_to_unixms(2023, 8, 1),
# ) -> pd.DataFrame:
#     counts = [0, 1, 5, 10, 25, 50, 100, 250]
#     query_counts = ", ".join(
#         [
#             f"CAST(SUM(CASE WHEN cast_count >= {c} THEN 1 ELSE 0 END) AS INT)"
#             f" AS cast_{c}_plus"
#             for c in counts
#         ]
#     )

#     t1 = f"to_timestamp({start / 1000})"
#     t2 = f"to_timestamp({end / 1000})"
#     query = f"""
#     SELECT
#         DATE_TRUNC('week', TIMESTAMP 'epoch' + registered_at * INTERVAL '1 second')
#             AS week,
#         COUNT(fid) AS total_registered,
#         {query_counts}
#     FROM (
#         SELECT
#             u.fid,
#             u.created_at AS registered_at,
#             COUNT(c.timestamp) AS cast_count
#         FROM user_data u
#         LEFT JOIN casts c ON u.fid = c.fid
#         WHERE u.created_at >= {t1} AND u.created_at < {t2} AND u.type = 6
#         GROUP BY u.fid, u.registered_at
#     ) subquery
#     GROUP BY week
#     """

#     df = execute_query_df(query)
#     df["week"] = df["week"].dt.strftime("%Y-%m-%d")
#     return df