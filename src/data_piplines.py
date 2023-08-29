import ast
import json
import os
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

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
# lookup tables
# ======================================================================================


def reverse_dict(d: Dict[Any, Any]) -> Dict[Any, Any]:
    return {v: k for k, v in d.items()}


def channel_lookup(type: Literal["channel_id", "parent_url"]) -> Dict[str, str]:
    def make_parent_url_lookup() -> Dict[str, str]:
        df = pd.read_json("data/fip2.ndjson", lines=True)
        return dict(zip(df["parent_url"], df["channel_id"]))

    if type == "channel_id":
        return make_parent_url_lookup()
    elif type == "parent_url":
        return reverse_dict(make_parent_url_lookup())


def fid_lookup(type: Literal["fid", "username"]) -> Dict[Any, Any]:
    def make_username_lookup() -> Dict[int, str]:
        # value of user_data type 6 is username
        query = """
        SELECT fid, value AS username
        FROM user_data
        WHERE type = 6
        """

        df = execute_query(query)
        return dict(zip(df["fid"], df["username"]))

    if type == "username":
        return make_username_lookup()
    elif type == "fid":
        return reverse_dict(make_username_lookup())


def hash_lookup(type: Literal["parent_hash"]) -> Dict[Any, Any]:
    def make_parent_hash_lookup() -> Dict[str, str]:
        query = f"""
        SELECT {to_hex('hash')}, {to_hex('parent_hash')}
        FROM casts
        WHERE parent_hash IS NOT NULL
        """

        df = execute_query(query)
        return dict(zip(df["hash"], df["parent_hash"]))

    if type == "parent_hash":
        return make_parent_hash_lookup()


# ======================================================================================
# pipelines
# ======================================================================================


def popular_users(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 7, 1),
    end: int = utils.TimeConverter.ymd_to_unixms(2023, 8, 1),
    limit: int = 20,
) -> pd.DataFrame:
    t1 = f"to_timestamp({start / 1000})"
    t2 = f"to_timestamp({end / 1000})"

    query_reactions = f"""
        SELECT
            c.fid AS fid,
            COUNT(*) AS reactions_received
        FROM
            reactions r
            INNER JOIN casts c ON c.hash = r.target_hash
            AND r.timestamp >= {t1}
            AND r.timestamp < {t2}
        GROUP BY
            c.fid
        ORDER BY
            reactions_received DESC
        LIMIT
            {limit}
    """

    df_reactions = execute_query(query_reactions)
    fids_str = ",".join(str(fid) for fid in df_reactions["fid"])
    query_casts = f"""
        SELECT
            fid,
            COUNT(*) AS total_casts
        FROM
            casts
        WHERE
            timestamp >= {t1}
            AND timestamp < {t2}
            AND fid IN ({fids_str})
        GROUP BY
            fid
    """

    df_casts = execute_query(query_casts)
    df = pd.merge(df_reactions, df_casts, on="fid", how="left")
    df["total_casts"] = df["total_casts"].fillna(0)

    username_lookup = fid_lookup("username")
    df["username"] = df["fid"].apply(lambda fid: username_lookup.get(fid, "unknown"))
    return df


def cast_reaction_volume(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 7, 1),
    end: int = utils.TimeConverter.ymd_to_unixms(2023, 8, 1),
) -> pd.DataFrame:
    def _get_data(col: str) -> pd.DataFrame:
        t1 = f"to_timestamp({start / 1000})"
        t2 = f"to_timestamp({end / 1000})"

        query = f"""
            SELECT
                date_trunc('day', timestamp) AS date,
                COUNT(*) AS count,
                COUNT(DISTINCT fid) AS unique_fids
            FROM
                {col}
            WHERE
                timestamp >= {t1}
                AND timestamp < {t2}
            GROUP BY
                date
            ORDER BY
                date
        """
        return execute_query(query)

    c_df = _get_data("casts")
    r_df = _get_data("reactions")

    df = pd.merge(c_df, r_df, on="date", suffixes=("_casts", "_reactions"))
    df = df.iloc[::-1]
    return df


def cast_channel_volume(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 7, 1),
    end: int = utils.TimeConverter.ymd_to_unixms(2023, 8, 1),
) -> pd.DataFrame:
    t1 = f"to_timestamp({start / 1000})"
    t2 = f"to_timestamp({end / 1000})"

    channel_id_lookup = channel_lookup("channel_id")
    parent_urls = list(channel_id_lookup.keys())
    parent_urls_str = ",".join(f"'{url}'" for url in parent_urls)

    query = f"""
        SELECT
            date_trunc('day', timestamp) AS date,
            COUNT(*) FILTER (
                WHERE
                    parent_url IS NOT NULL
                    AND parent_url IN ({parent_urls_str})
            ) AS channel_count,
            COUNT(*) FILTER (
                WHERE
                    parent_url IS NULL
            ) AS non_channel_count
        FROM
            casts
        WHERE
            timestamp >= {t1}
            AND timestamp < {t2}
        GROUP BY
            date
        ORDER BY
            date
    """

    df = execute_query(query)
    df = df.iloc[::-1]
    return df


# TODO: too complex!
def channel_volume_table(
    start: int = utils.TimeConverter.ymd_to_unixms(2023, 6, 1)
) -> pd.DataFrame:
    def _channel_volume(start: int, end: int, limit: int = 10) -> pd.DataFrame:
        t1 = f"to_timestamp({start / 1000})"
        t2 = f"to_timestamp({end / 1000})"

        query = f"""
            WITH cast_data AS (
                SELECT parent_url, COUNT(*) as cast_count
                FROM casts
                WHERE timestamp >= {t1} AND timestamp < {t2} AND parent_url IS NOT NULL
                GROUP BY parent_url
            ),
            reaction_data AS (
                SELECT c.parent_url, COUNT(r.id) as reaction_count
                FROM casts c
                LEFT JOIN reactions r ON r.target_hash = c.hash
                WHERE c.timestamp >= {t1} AND c.timestamp < {t2} 
                    AND c.parent_url IS NOT NULL
                GROUP BY c.parent_url
            )
            SELECT cd.parent_url
            FROM cast_data cd
            LEFT JOIN reaction_data rd ON cd.parent_url = rd.parent_url
            ORDER BY (cd.cast_count + COALESCE(rd.reaction_count, 0)) DESC
            LIMIT {limit};
        """
        return execute_query(query)

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
        df = _channel_volume(start=start, end=end, limit=10)
        df = df[df["parent_url"].isin(list(url_id_map.keys()))]

        df["result"] = "f/" + df["parent_url"].apply(get_channel_id)
        return [utils.TimeConverter.unixms_to_ymd(start)] + list(df["result"])

    all_records = list(map(process_interval, generate_intervals(start, end)))
    columns = ["Date"] + [f"Rank {i}" for i in range(1, 11)]
    return pd.DataFrame(all_records[::-1], columns=columns)


def frequency_heatmap(start: int, end: int) -> pd.DataFrame:
    def execute_hourly_query(table: str) -> pd.DataFrame:
        t1 = f"to_timestamp({start / 1000})"
        t2 = f"to_timestamp({end / 1000})"
        query = f"""
            WITH hourly_data AS (
                SELECT 
                    EXTRACT(HOUR FROM timestamp) AS hour,
                    EXTRACT(DOW FROM timestamp) AS day_of_week,
                    COUNT(*) AS count
                FROM {table}
                WHERE timestamp >= {t1} AND timestamp < {t2}
                GROUP BY hour, day_of_week
            )
            SELECT 
                hour,
                AVG(CASE WHEN day_of_week = 0 THEN count END) AS "Sun",
                AVG(CASE WHEN day_of_week = 1 THEN count END) AS "Mon",
                AVG(CASE WHEN day_of_week = 2 THEN count END) AS "Tue",
                AVG(CASE WHEN day_of_week = 3 THEN count END) AS "Wed",
                AVG(CASE WHEN day_of_week = 4 THEN count END) AS "Thu",
                AVG(CASE WHEN day_of_week = 5 THEN count END) AS "Fri",
                AVG(CASE WHEN day_of_week = 6 THEN count END) AS "Sat"
            FROM hourly_data
            GROUP BY hour
            ORDER BY hour;
        """
        return execute_query(query)

    casts_df = execute_hourly_query("casts")
    reactions_df = execute_hourly_query("reactions")

    df = pd.merge(casts_df, reactions_df, on="hour", suffixes=("_casts", "_reactions"))
    return df


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


# TODO: clean up two functions below
def earliest_actions() -> pd.DataFrame:
    end = utils.TimeConverter.ms_now()
    start = end - utils.TimeConverter.to_ms("weeks", 1)
    df = popular_users(start, end, 150)
    fids = list(df["fid"])

    # filter out fids that are above 10k, because casts_old max(fid) = ~12k
    # 2k as a buffer for activities to accumulate
    fids = [fid for fid in fids if fid < 10000]

    def get_user_data(df_c: pd.DataFrame, df_r: pd.DataFrame, fid: int) -> pd.DataFrame:
        three_days_ms = utils.TimeConverter.to_ms("days", 3)
        one_week_ms = utils.TimeConverter.to_ms("weeks", 1)
        one_month_ms = utils.TimeConverter.to_ms("months", 1)
        one_quarter_ms = utils.TimeConverter.to_ms("months", 3)

        t = df_c[df_c["author_fid"] == fid]["timestamp"].min()
        df_c = df_c[(df_c["author_fid"] == fid)]
        time_intervals = [
            ("three_days", t + three_days_ms),
            ("one_week", t + one_week_ms),
            ("one_month", t + one_month_ms),
            ("one_quarter", t + one_quarter_ms),
        ]

        result = {"fid": fid}

        for label, interval_ms in time_intervals:
            interval_hashes = list(df_c[df_c["timestamp"] <= interval_ms]["hash"])
            interval_reactions = df_r[df_r["target_hash"].isin(interval_hashes)]
            result[f"casts_{label}"] = len(interval_hashes)
            result[f"reactions_{label}"] = interval_reactions.shape[0]

        return result

    df_c = pd.read_parquet("casts_old.parquet")
    df_r = pd.read_parquet("reactions_old.parquet")
    df_count = pd.DataFrame([get_user_data(df_c, df_r, fid) for fid in fids])
    df = df.merge(df_count, on="fid")
    return df
    # df.to_csv("data/dummy.csv", index=False)


def invited_by_and_purple() -> pd.DataFrame:
    def purple_lookup() -> Dict[str, bool]:
        with open("pprl.json", "r") as f:
            data = json.load(f)
        owners = [node["token"]["owner"] for node in data["data"]["mints"]["nodes"]]

        query = """
        SELECT fid, claim->>'address' as address
        FROM verifications
        """
        df = execute_query(query)
        df["is_purple"] = df["address"].isin(owners)
        df = df[df["is_purple"] == True]
        df = df[["fid", "is_purple"]]
        return df.set_index("fid").to_dict()["is_purple"]

    purple_lookup_dict = purple_lookup()
    purple_lookup_fn = lambda fid: purple_lookup_dict.get(fid, False)

    username_lookup = fid_lookup("username")
    username_lookup_fn = lambda fid: username_lookup.get(fid, None)
    df = pd.read_json("queue/user_warpcast.ndjson", lines=True, dtype_backend="pyarrow")
    df = df[["fid", "username", "inviter_fid"]]
    df["inviter_username"] = df["inviter_fid"].apply(username_lookup_fn)
    df = df[df["inviter_fid"].notnull() & df["inviter_username"].notnull()]
    # filter out rows where inviter_fid is higher than fid
    df = df[df["inviter_fid"] < df["fid"]]
    df["is_purple"] = df["fid"].apply(purple_lookup_fn)
    # print(df)
    # df.to_csv("data/dummy.csv", index=False)

    df = (
        df.groupby("inviter_fid")
        .agg(
            total_invites=pd.NamedAgg(column="fid", aggfunc="count"),
            purple_invites=pd.NamedAgg(column="is_purple", aggfunc="sum"),
        )
        .reset_index()
    )
    df["inviter_username"] = df["inviter_fid"].apply(username_lookup_fn)
    print(df)
    # df.to_csv("data/dummy.csv", index=False)
    return df


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
