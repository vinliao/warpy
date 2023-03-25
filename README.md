# Warpy (Beta)

[Farcaster](https://github.com/farcasterxyz/protocol) is an Ethereum-based programmable social network. Warpy provides open-source Farcaster datasets.

To download the datasets, run `python download.py`. If you prefer, `curl https://pub-3916d8c82abb435eb70175747fdc2119.r2.dev/datasets.tar.gz` and `tar -xvf datasets.tar.gz`.

Here are the schemas for the datasets:

**UserExtraDataClass**

| Field             | Type          | Description                                  |
| :---------------- | :------------ | :------------------------------------------- |
| fid               | int           | Farcaster ID                                 |
| following_count   | int           | Number of following                          |
| follower_count    | int           | Number of followers                          |
| location_id       | Optional[str] | Location ID                                  |
| verified          | bool          | Verification status                          |
| farcaster_address | Optional[str] | Farcaster address                            |
| external_address  | Optional[str] | External address (the ones with NFT and ENS) |
| registered_at     | int           | FID registration timestamp                   |

**LocationDataClass**

| Field       | Type | Description      |
| :---------- | :--- | :--------------- |
| id          | str  | Location ID      |
| description | str  | Location details |

**UserDataClass**

| Field        | Type | Description                |
| :----------- | :--- | :------------------------- |
| fid          | int  | Farcaster ID               |
| username     | str  | Farcaster username (fname) |
| display_name | str  | User's display name        |
| pfp_url      | str  | Profile picture URL        |
| bio_text     | str  | User's bio information     |

**CastDataClass**

| Field       | Type          | Description           |
| :---------- | :------------ | :-------------------- |
| hash        | str           | Cast hash             |
| thread_hash | str           | Thread hash           |
| text        | str           | Cast text             |
| timestamp   | int           | Cast timestamp        |
| author_fid  | int           | Author's Farcaster ID |
| parent_hash | Optional[str] | Parent cast hash      |

Latest cast timestamp: 1679718792000; highest fid: 11237.
