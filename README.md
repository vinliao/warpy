# Warpy (Beta): Farcaster datasets, one API call away

[Farcaster](https://farcaster.xyz) is an Ethereum-based programmable social network. Warpy provides open-source Farcaster datasets.

To see what you can do with the datasets and API, see [example.ipynb](example.ipynb). To open the example notebook in Google Colab, use this [link](https://githubtocolab.com/vinliao/warpy/blob/master/example.ipynb). To see the data schema, see [models.py](models.py).

## TLDR: the API endpoints

**`/query`**: Execute SQL and natural language queries

The `/query` endpoint allows you to execute SELECT, SHOW, DESCRIBE, and EXPLAIN queries in either raw SQL format or natural language (with the help of GPT-4).

### Example

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"type": "english",
          "query": "get latest cast in the farcaster network"}' \
     https://api.warpy.dev/query
```

Parameters

- `type`: The type of query input. Valid values are "raw" for raw SQL queries, "english" for english-to-SQL with GPT-3.5, and "english-advanced" for english-to-SQL with GPT-4 (with more a complex prompt).
- `query`: The query string in either raw SQL format or natural language.
- `export`: (Optional) Set the value to "csv" to export the query result as a CSV file. The response will contain an S3 URL to download the file.

Example response:

```json
{
  "result": [
    {
      "hash": "0x249214e1fe963bdc88a90b351a35e47817e43ea5",
      "thread_hash": "0xd490a3be27d8ffcc39c7d7343855585e8bf412e9",
      "text": "and I might have to do something fun with this integration as well 👀🗂️\n\nhttps://twitter.com/ourzora/status/1652040075398586368?s=46&t=8l54n7dYtHePgMrhGh-cUw",
      "timestamp": 1682714308000,
      "author_fid": 616,
      "parent_hash": "0xd490a3be27d8ffcc39c7d7343855585e8bf412e9"
    }
  ],
  "sql": "SELECT * FROM casts ORDER BY timestamp DESC LIMIT 1;",
  "thoughts": null,
  "schema": "09e5d80a53a4"
}
```

Again, see [example.ipynb](example.ipynb) for more examples. Ping me on Farcaster ([@pixel](https://warpcast.com/pixel)) if you have any questions!
