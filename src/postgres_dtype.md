## Examples of SQL queries

Note: for reference only, see https://raw.githubusercontent.com/farcasterxyz/hub-monorepo/main/packages/hub-nodejs/examples/replicate-data-postgres/README.md

Once some data is populated, you can start to query it using SQL. Here are some examples: 

Get the 10 most recent casts for a user:
```sql
select timestamp, text, mentions, mentions_positions, embeds from casts where fid = 2 order by timestamp desc limit 10;
```

Get the number of likes for a user's last 20 casts:
```sql
select timestamp, (select count(*) from reactions where reaction_type = 1 and target_hash = casts.hash and target_fid = casts.fid) from casts where fid = 3 order by timestamp desc limit 20;
```

Get the top-20 most recasted casts:
```sql
select c.hash, count(*) as recast_count from casts as c join reactions as r on r.target_hash = c.hash and r.target_fid = c.fid where r.reaction_type = 2 group by c.hash order by recast_count desc limit 20;
```

See the list of tables below for the schema.

## Database Schema

The example initializes the following tables in Postgres DB where data from the Hubs are stored:

### `messages`

All Farcaster messages retrieved from the hub are stored in this table. Messages are never deleted, only soft-deleted (i.e. marked as deleted but not actually removed from the DB).

Column Name | Data Type | Description
-- | -- | --
id | `bigint` | Generic identifier specific to this DB (a.k.a. [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key))
created_at | `timestamp without time zone` | When the row was first created in this DB (not the same as the message timestamp!)
updated_at | `timestamp without time zone` | When the row was last updated.
deleted_at | `timestamp without time zone` | When the message was deleted by the hub (e.g. in response to a `CastRemove` message, etc.)
pruned_at | `timestamp without time zone` | When the message was pruned by the hub.
revoked_at | `timestamp without time zone` | When the message was revoked by the hub due to revocation of the signer that signed the message.
timestamp | `timestamp without time zone` | Message timestamp in UTC.
fid | `bigint` | FID of the user that signed the message.
message_type | `smallint` | Message type.
hash | `bytea` | Message hash.
hash_scheme | `smallint` | Message hash scheme.
signature | `bytea` | Message signature.
signature_scheme | `smallint` | Message hash scheme.
signer | `bytea` | Signer used to sign this message.
raw | `bytea` | Raw bytes representing the serialized message [protobuf](https://protobuf.dev/).

### `casts`

Represents a cast authored by a user.

Column Name | Data Type | Description
-- | -- | --
id | `bigint` | Generic identifier specific to this DB (a.k.a. [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key))
created_at | `timestamp without time zone` | When the row was first created in this DB (not the same as the message timestamp!)
updated_at | `timestamp without time zone` | When the row was last updated.
deleted_at | `timestamp without time zone` | When the cast was considered deleted by the hub (e.g. in response to a `CastRemove` message, etc.)
timestamp | `timestamp without time zone` | Message timestamp in UTC.
fid | `bigint` | FID of the user that signed the message.
hash | `bytea` | Message hash.
parent_hash | `bytea` | If this cast was a reply, the hash of the parent cast. `null` otherwise.
parent_fid | `bigint` | If this cast was a reply, the FID of the author of the parent cast. `null` otherwise.
parent_url | `text` | If this cast was a reply to a URL (e.g. an NFT, a web URL, etc.), the URL. `null` otherwise.
text | `text` | The raw text of the cast with mentions removed.
embeds | `text[]` | Array of URLs that were embedded with this cast.
mentions | `bigint[]` | Array of FIDs mentioned in the cast.
mentions_positions | `smallint[]` | UTF8 byte offsets of the mentioned FIDs in the cast.

### `reactions`

Represents a user reacting (liking or recasting) content.

Column Name | Data Type | Description
-- | -- | --
id | `bigint` | Generic identifier specific to this DB (a.k.a. [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key))
created_at | `timestamp without time zone` | When the row was first created in this DB (not the same as the message timestamp!)
updated_at | `timestamp without time zone` | When the row was last updated.
deleted_at | `timestamp without time zone` | When the reaction was considered deleted by the hub (e.g. in response to a `ReactionRemove` message, etc.)
timestamp | `timestamp without time zone` | Message timestamp in UTC.
fid | `bigint` | FID of the user that signed the message.
reaction_type | `smallint` | Type of reaction.
hash | `bytea` | Message hash.
target_hash | `bytea` | If target was a cast, the hash of the cast. `null` otherwise.
target_fid | `bigint` | If target was a cast, the FID of the author of the cast. `null` otherwise.
target_url | `text` | If target was a URL (e.g. NFT, a web URL, etc.), the URL. `null` otherwise.

### `verifications`

Represents a user verifying something on the network. Currently, the only verification is proving ownership of an Ethereum wallet address.

Column Name | Data Type | Description
-- | -- | --
id | `bigint` | Generic identifier specific to this DB (a.k.a. [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key))
created_at | `timestamp without time zone` | When the row was first created in this DB (not the same as the message timestamp!)
updated_at | `timestamp without time zone` | When the row was last updated.
deleted_at | `timestamp without time zone` | When the verification was considered deleted by the hub (e.g. in response to a `VerificationRemove` message, etc.)
timestamp | `timestamp without time zone` | Message timestamp in UTC.
fid | `bigint` | FID of the user that signed the message.
hash | `bytea` | Message hash.
claim | `jsonb` | JSON object in the form `{"address": "0x...", "blockHash": "0x...", "ethSignature": "0x..."}`. See [specification](https://github.com/farcasterxyz/protocol/blob/main/docs/SPECIFICATION.md#15-verifications) for details.

### `signers`

Represents signers that users have registered as authorized to sign Farcaster messages on the user's behalf.

Column Name | Data Type | Description
-- | -- | --
id | `bigint` | Generic identifier specific to this DB (a.k.a. [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key))
created_at | `timestamp without time zone` | When the row was first created in this DB (not the same as the message timestamp!)
updated_at | `timestamp without time zone` | When the row was last updated.
deleted_at | `timestamp without time zone` | When the signer was considered deleted by the hub (e.g. in response to a `SignerRemove` message, etc.)
timestamp | `timestamp without time zone` | Message timestamp in UTC.
fid | `bigint` | FID of the user that signed the message.
hash | `bytea` | Message hash.
custody_address | `bytea` | The address of the FID that signed the `SignerAdd` message.
signer | `bytea` | The public key of the signer that was added.
name | `text` | User-specified human-readable name for the signer (e.g. the application it is used for).

### `user_data`

Represents data associated with a user (e.g. profile photo, bio, username, etc.)

Column Name | Data Type | Description
-- | -- | --
id | `bigint` | Generic identifier specific to this DB (a.k.a. [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key))
created_at | `timestamp without time zone` | When the row was first created in this DB (not the same as the message timestamp!)
updated_at | `timestamp without time zone` | When the row was last updated.
deleted_at | `timestamp without time zone` | When the data was considered deleted by the hub
timestamp | `timestamp without time zone` | Message timestamp in UTC.
fid | `bigint` | FID of the user that signed the message.
hash | `bytea` | Message hash.
type | `smallint` | The type of user data (PFP, bio, username, etc.)
value | `text` | The string value of the field.

### `fids`

Stores the custody address that owns a given FID (i.e. a Farcaster user).

Column Name | Data Type | Description
-- | -- | --
fid | `bigint` | Farcaster ID (the user ID)
created_at | `timestamp without time zone` | When the row was first created in this DB (not the same as when the user was created!)
updated_at | `timestamp without time zone` | When the row was last updated.
custody_address | `bytea` | ETH address of the wallet that owns the FID.

### `links`

Represents a link between two FIDs (e.g. a follow, subscription, etc.)

Column Name | Data Type | Description
-- | -- | --
id | `string` | Generic identifier specific to this DB (a.k.a. [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key))
fid | `bigint` | Farcaster ID (the user ID)
target_fid | `bigint` | Farcaster ID of the target user
type | `string` | Type of connection between users like `follow`
timestamp | `timestamp without time zone` | Message timestamp in UTC.
created_at | `timestamp without time zone` | When the row was first created in this DB
updated_at | `timestamp without time zone` | When the row was last updated
display_timestamp | `timestamp without time zone` | When the row was last updated
deleted_at | `timestamp without time zone` | When the link was considered deleted by the hub (e.g. in response to a `LinkRemoveMessage` message, etc.)