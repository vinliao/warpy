import datetime
import os

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
    return con.execute(query).fetchone()[0]


def get_id_by_url(url: str) -> str:
    f = "data/fip2.ndjson"
    download_fip2_ndjson(f)
    query = f"select channel_id from read_json_auto('{f}') "
    query += f"where parent_url = '{url}'"
    con = duckdb.connect()
    return con.execute(query).fetchone()[0]


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


def channel_volume_table() -> pd.DataFrame:
    genesis = utils.TimeConverter.ymd_to_unixms(2023, 6, 1)
    start = utils.TimeConverter.unixms_to_datetime(genesis)
    end = utils.TimeConverter.unixms_to_datetime(utils.TimeConverter.ms_now())

    url_id_map = pd.read_json("data/fip2.ndjson", lines=True)
    url_id_map = url_id_map.set_index("parent_url").to_dict()["channel_id"]
    get_channel_id = lambda url: url_id_map[url]

    def generate_intervals(start_date, end_date):
        while start_date < end_date:
            next_date = start_date + datetime.timedelta(weeks=1)
            yield start_date, next_date
            start_date = next_date

    def process_interval(interval):
        start, end = interval
        t1 = utils.TimeConverter.datetime_to_unixms(start)
        t2 = utils.TimeConverter.datetime_to_unixms(end)
        df = channel_volume(start=t1, end=t2, limit=10)
        df = df[df["parent_url"].isin(list(url_id_map.keys()))]

        count_str = " (" + df["count"].astype(str) + " casts)"
        df["result"] = "f/" + df["parent_url"].apply(get_channel_id) + count_str
        return [start.strftime("%b %d %Y")] + list(df["result"])

    all_records = list(map(process_interval, generate_intervals(start, end)))
    columns = ["Date"] + [f"Rank {i}" for i in range(1, 11)]
    return pd.DataFrame(all_records[::-1], columns=columns)


print(channel_volume_table())


# # Write to CSV
# with open("channel_volumes.csv", "w", newline="") as file:
#     writer = csv.writer(file)
#     writer.writerow(["Date"] + [f"Rank {i}" for i in range(1, 11)])  # Header
#     writer.writerows(all_records)  # Write all the records


# # # TODO: very rough, needs refinement
# def channel_volume_table() -> None:
#     timestamp_start = 1685658318000
#     all_records = []
#     now = datetime.datetime.now()
#     for i in range(16):
#         t2 = now - datetime.timedelta(weeks=i)
#         t1 = now - datetime.timedelta(weeks=i + 1)
#         t1_unix = indexer.TimeConverter.datetime_to_unixms(t1)
#         t2_unix = indexer.TimeConverter.datetime_to_unixms(t2)
#         result = channel_volume(t1=t1_unix, t2=t2_unix, limit=10)
#         week_result = [t2.strftime("%b %d %Y")]
#         week_result += [record["channel_id"] for record in result["result"]]
#         all_records.append(week_result)

#     # Write to CSV
#     with open("channel_volumes.csv", "w", newline="") as file:
#         writer = csv.writer(file)
#         writer.writerow(["Date"] + [f"Rank {i}" for i in range(1, 11)])  # Header
# # writer.writerows(all_records)


# def channel_volume(
#     t1: int = indexer.TimeConverter.ymd_to_unixms(2023, 7, 1),
#     t2: int = indexer.TimeConverter.ymd_to_unixms(2023, 8, 1),
#     limit: int = 5,
# ) -> dict[str, Any]:
#     query = f"""
#     SELECT channel_id, COUNT(*) as count
#     FROM read_parquet('data/casts.parquet')
#     WHERE timestamp >= {t1} AND timestamp < {t2} AND channel_id IS NOT NULL
#     GROUP BY channel_id
#     ORDER BY count DESC
#     LIMIT {limit};
#     """
#     df = indexer.execute_query_df(query)
#     df["channel_id"] = df["channel_id"].apply(lambda x: f"f/{x}")
#     return {"result": df.to_dict(orient="records")}


# # TODO: very rough, needs refinement
# def channel_volume_table() -> None:
#     all_records = []
#     now = datetime.datetime.now()
#     for i in range(16):
#         t2 = now - datetime.timedelta(weeks=i)
#         t1 = now - datetime.timedelta(weeks=i + 1)
#         t1_unix = indexer.TimeConverter.datetime_to_unixms(t1)
#         t2_unix = indexer.TimeConverter.datetime_to_unixms(t2)
#         result = channel_volume(t1=t1_unix, t2=t2_unix, limit=10)
#         week_result = [t2.strftime("%b %d %Y")]
#         week_result += [record["channel_id"] for record in result["result"]]
#         all_records.append(week_result)

#     # Write to CSV
#     with open("channel_volumes.csv", "w", newline="") as file:
#         writer = csv.writer(file)
#         writer.writerow(["Date"] + [f"Rank {i}" for i in range(1, 11)])  # Header
# writer.writerows(all_records)


#     query_reaction_avg = f"""
#     SELECT c.channel_id,
#            COUNT(r.hash) / CAST(COUNT(DISTINCT c.hash) AS FLOAT) as reaction_per_cast
#     FROM read_parquet('data/casts.parquet') AS c
#     LEFT JOIN read_parquet('data/reactions.parquet') AS r ON c.hash = r.target_hash
#     WHERE c.timestamp >= {t1} AND c.timestamp < {t2} AND c.channel_id IS NOT NULL
#     GROUP BY c.channel_id
#     HAVING COUNT(DISTINCT c.hash) > 100
#     ORDER BY reaction_per_cast DESC
#     LIMIT {limit};
#     """

#     df_volume = indexer.execute_query_df(query_volume)
#     df_reaction_avg = indexer.execute_query_df(query_reaction_avg)

#     return {
#         "volume": df_volume.to_dict(orient="records"),
#         "reaction_average": df_reaction_avg.to_dict(orient="records"),
#     }


# def cast_reaction_volume(
#     t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
#     t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
# ) -> dict[str, Any]:
#     def daily_bucket(table: str) -> list[tuple[datetime.date, int]]:
#         def make_date(timestamp: int) -> datetime.date:
#             return datetime.datetime.utcfromtimestamp(timestamp / 1000.0).date()

#         query = f"SELECT timestamp FROM read_parquet('data/{table}.parquet') "
#         query += f"WHERE timestamp >= {t1} AND timestamp < {t2}"
#         timestamps = indexer.execute_query(query)
#         dates = [make_date(ts) for ts in timestamps]
#         return sorted(Counter(dates).items())

#     return {"casts": daily_bucket("casts"), "reactions": daily_bucket("reactions")}


# def popular_users(
#     t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
#     t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
#     limit: int = 20,
# ) -> list[dict[Hashable, Any]]:
#     query = f"""
#     SELECT c.author_fid AS fid, COUNT(*) AS count
#     FROM read_parquet('data/reactions.parquet') r
#     INNER JOIN read_parquet('data/casts.parquet') c
#     ON c.hash = r.target_hash AND r.timestamp >= {t1} AND r.timestamp < {t2}
#     GROUP BY c.author_fid
#     ORDER BY count DESC
#     LIMIT {limit}
#     """
#     df = indexer.execute_query_df(query)
#     df["username"] = df["fid"].apply(indexer.get_username_by_fid)
#     return df.to_dict(orient="records")


# def activation_table(
#     t1: int = indexer.TimeUtils.ymd_to_unixms(2021, 7, 1),
#     t2: int = indexer.TimeUtils.ymd_to_unixms(2021, 8, 1),
# ) -> list[dict[Hashable, Any]]:
#     counts = [0, 1, 5, 10, 25, 50, 100, 250]
#     query_counts = ", ".join(
#         [
#             f"CAST(SUM(CASE WHEN cast_count >= {c} THEN 1 ELSE 0 END) AS INT)"
#             f" AS cast_{c}_plus"
#             for c in counts
#         ]
#     )

#     query = f"""
#     SELECT
#         DATE_TRUNC('week', TIMESTAMP 'epoch' + registered_at * INTERVAL '1 millisecond')
#             AS week,
#         COUNT(fid) AS total_registered,
#         {query_counts}
#     FROM (
#         SELECT
#             u.fid,
#             u.registered_at,
#             COUNT(c.timestamp) AS cast_count
#         FROM read_parquet('data/users.parquet') u
#         LEFT JOIN read_parquet('data/casts.parquet') c ON u.fid = c.author_fid
#         WHERE u.registered_at >= {t1} AND u.registered_at < {t2}
#         GROUP BY u.fid, u.registered_at
#     ) subquery
#     GROUP BY week
#     """

#     df = indexer.execute_query_df(query)
#     df["week"] = df["week"].dt.strftime("%Y-%m-%d")
#     return df.to_dict(orient="records")
