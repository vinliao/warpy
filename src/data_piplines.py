import ast
import os
from typing import Any, Dict, Generator, List, Optional, Tuple

import duckdb
import pandas as pd
import requests

import utils

# ======================================================================================
# utils
# ======================================================================================


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
def execute_query(
    query: str, pg_url: str = "postgresql://app:password@localhost:6541/hub"
) -> pd.DataFrame:
    # NOTE: must have replicator running, maybe have a shell script or something
    return pd.read_sql(query, pg_url, dtype_backend="pyarrow")


def to_hex(column: str, name: Optional[str] = None) -> str:
    return f"'0x' || encode({column}, 'hex') AS {name if name else column}"


def to_bytea(hex_column: str) -> str:
    # Removing the '0x' prefix if present
    if hex_column.startswith("0x"):
        hex_column = hex_column[2:]
    return f"decode('{hex_column}', 'hex')"


# ======================================================================================
# pipelines
# ======================================================================================


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
    return execute_query(query)


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

        df = execute_query(query)
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

    df = execute_query(query)
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
        return execute_query(query)

    c_df = daily_bucket("casts")
    r_df = daily_bucket("reactions")

    return pd.merge(c_df, r_df, on="date", suffixes=("_casts", "_reactions"))


def embed_count(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 7, 1),
    end: int = utils.TimeConverter.ymd_to_unixms(2023, 8, 1),
) -> pd.DataFrame:
    t1 = f"to_timestamp({start / 1000})"
    t2 = f"to_timestamp({end / 1000})"

    def _categorize(embeds: list[dict[str, Any]]) -> str:
        if not embeds:
            return "no_embed"

        exts = [".jpg", ".jpeg", ".png", ".gif"]
        has_image = any(ext in e["url"] for e in embeds for ext in exts)
        has_link = any(all(ext not in e["url"] for ext in exts) for e in embeds)

        if has_image and has_link:
            return "image_and_link"
        elif has_image:
            return "image_only"
        else:
            return "link_only"

    query = "SELECT embeds, timestamp FROM casts WHERE "
    query += f"timestamp >= {t1} AND timestamp < {t2}"
    df = execute_query(query)
    df["embeds"] = df["embeds"].apply(ast.literal_eval)
    df["category"] = df["embeds"].apply(_categorize)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = df.pivot_table(
        index=pd.Grouper(key="timestamp", freq="D"),
        columns="category",
        values="embeds",
        aggfunc="count",
        fill_value=0,
    )
    df.reset_index(inplace=True)
    cols = ["image_and_link", "image_only", "link_only", "no_embed"]
    df["total"] = df[cols].sum(axis=1)
    df = df.iloc[::-1]
    return df


def top_casts_embed_count(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 7, 1),
    end: int = utils.TimeConverter.ymd_to_unixms(2023, 8, 1),
) -> pd.DataFrame:
    t1 = f"to_timestamp({start / 1000})"
    t2 = f"to_timestamp({end / 1000})"

    def _categorize(embeds: list[dict[str, Any]]) -> str:
        if not embeds:
            return "no_embed"

        exts = [".jpg", ".jpeg", ".png", ".gif"]
        has_image = any(ext in e["url"] for e in embeds for ext in exts)
        has_link = any(all(ext not in e["url"] for ext in exts) for e in embeds)

        if has_image:
            return "image_only"
        if has_link:
            return "link_only"
        return "other"

    query = f"""
        WITH ranked_casts AS (
            SELECT 
                c.id,
                c.embeds,
                c.timestamp,
                c.hash,
                SUM(CASE r.reaction_type WHEN 1 THEN 1 WHEN 2 THEN 3 ELSE 0 END) 
                    AS reactions_count,
                ROW_NUMBER() OVER(PARTITION BY DATE(c.timestamp) ORDER BY 
                    SUM(CASE r.reaction_type WHEN 1 THEN 1 WHEN 2 THEN 3 ELSE 0 END) 
                    DESC) AS rank
            FROM casts c
            LEFT JOIN reactions r ON c.hash = r.target_hash
            WHERE c.timestamp >= {t1} AND c.timestamp < {t2}
            GROUP BY c.id, c.timestamp, c.hash
        )
        SELECT
            {to_hex('rc.hash', 'encoded_hash')},
            rc.reactions_count,
            DATE(rc.timestamp) AS date,
            rc.embeds
        FROM ranked_casts rc
        WHERE rc.rank <= 50
    """

    df = execute_query(query)
    df["embeds"] = df["embeds"].apply(ast.literal_eval)
    df["category"] = df["embeds"].apply(_categorize)
    df = (
        df.groupby(["date", "category"])
        .apply(lambda x: x.nlargest(10, "reactions_count"))
        .reset_index(drop=True)
    )
    df = (
        df.groupby(["date", "category"])["reactions_count"]
        .mean()
        .reset_index(name="avg_reactions")
    )
    df = df.pivot(index="date", columns="category", values="avg_reactions").fillna(0)
    df.reset_index(inplace=True)
    df = df.iloc[::-1]
    return df


# end = utils.TimeConverter.ms_now()
# start = end - utils.TimeConverter.to_ms("months", 3)
# df = top_casts_embed_count(start, end)
# df.to_csv("data/dummy.csv", index=False)


# df = top_casts_embed_count()
# print(df)
# df.to_csv("data/dummy.csv", index=False)

# df = top_casts_embed_count()
# df.to_csv("data/dummy.csv", index=False)

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

#     df = execute_query(query)
#     df["week"] = df["week"].dt.strftime("%Y-%m-%d")
#     return df
