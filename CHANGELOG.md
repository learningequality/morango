# Release Notes

List of the most important changes for each release.

## 0.8.0
 - Drops support for Python 2.7
 - Upgrades cryptography library to 40.0.2, the latest version that supports Python 3.6
 - Upgrades M2Crypto to 0.41.0
 - Upgrades Django to version 3
 - Upgrades django-mptt to a version greater than 0.10 for Django 3 compatibility
 - Upgrades djangorestframework to a version greater than 3.10 for Django 3 compatibility
 - Upgrades django-ipware to version 4.0.2, the latest version that supports Python 3.6
 - Drops usage of "value_from_object_json_compatible" on fields and instead inspects booleans for "morango_serialize_to_string" attribute to determine if the field value_to_string method should be used for serialization and to_python used for deserialization
 - Removes support for MorangoMPTT models

## 0.7.1
 - Supersedes 0.7.0
 - Drops support for Python 3.4 and 3.5
 - Upgrades cryptography library to 3.3.2, the latest version that supports Python 2.7

## 0.6.19
- The `cleanupsyncs` management command now only cleans up sync sessions if also inactive for `expiration` amount of time
- Fixes issue accessing index on queryset in `cleanupsyncs` management command

## 0.6.18
- Prevent creation of Deleted and HardDeleted models during deserialization to allow propagation of syncable objects that are recreated after a previous deletion without causing a merge conflict.

## 0.6.17
- Added `client-instance-id`, `server-instance-id`, `sync-filter`, `push` and `pull` arguments to `cleanupsyncs` management command
- Added option for resuming a sync to ignore the `SyncSession`'s existing `process_id`
- Added custom user agent to sync HTTP requests
- Fixed documentation build issues
- Makefile no longer defines shell with explicit path

## 0.6.16
- Added dedicated `client_instance_id` and `server_instance_id` fields to `SyncSession`
- Renamed `client_instance` and `server_instance` fields on `SyncSession` to `client_instance_json` and `server_instance_json` respectively

## 0.6.15
- Fixes issue causing overflow error during lengthy syncs

## 0.6.14
- Fixes issue that caused discrepancies between the client's and server's sync state
- Fixes issue with transaction isolation persisting longer than intended

## 0.6.13
- Capture and retry transaction isolation errors that occur when conflicting concurrent updates are made during the transaction

## 0.6.12
- Replace serializable transaction isolation using a separate DB connection for Postgres with advisory locking.

## 0.6.11
- Added deferred processing of foreign keys to allow bulk processing and to improve performance.
- Eliminated extraneous SQL queries for the transfer session when querying for buffers.
- Added database index to Store's partition field to improve querying performance.

## 0.6.10
- Fixes Django migration issue introduced in 0.6.7 allowing nullable fields with PostgreSQL backends

## 0.6.9
- Fixes un-ordered selection of buffers during sync which can allow duplicates to be synced with PostgreSQL backends
- Moves updating of database counters to occur in the same DB transaction as updates to the Store
- Adds setting to disable reduction of FSICs when calculating them for a set of partitions

## 0.6.8
- Fixes subset syncing issues by introducing new FSIC v2 format

## 0.6.7
- Updates transfer status fields to be nullable and corrects prior migration

## 0.6.6
- Adds an asymmetry to FSIC calculation to ensure all matching data is synced.
- Adds support for defining custom instance info returned from info API and during sync session creation
- Updates `cleanupsyncs` management command to only close sync sessions if there are no other related transfer sessions active
- Fixes issue syncing with Morangos pre-0.6.0 causing pushed records to not be dequeued

## 0.6.5
- Sets queuing limit of 100k combined FSICs between client and server
- Fixes SQL expression tree error when there are many FSICs, up to 100k limit
- Adds additional `ids` argument to `cleanupsyncs` management command

## 0.6.4
- Fixes issue with `assert` statement removal during python optimization

## 0.6.3

- Fixes issue handling database counters which caused repeat syncing of unchanged data

## 0.6.2

- Fixes slow performance due to excessive use of `sleep`

## 0.6.1

- Fix to set counters on `TransferSession` *after* serialization
- Fix capabilities request header as it should be prefixed with `HTTP`
- Fix issues with flow of transfer operations
- Logging and error handling improvements


## 0.6.0

- Track the `TransferSession` that last modified a `Store` record
- Add state attributes to `TransferSession` for persisting its stage and status during a sync
- Update the timestamp of the last activity for a `SyncSession` and `TransferSession` during a sync
- Add support for resuming a sync
- Add support for configuring custom handling of transfer operations
- Add support for handling transfer operations asynchronously

## 0.5.6

- Add management command for garbage collection of buffer data
- Speed up instance ID calculation by skipping hostname check

## 0.5.5

- Allow MAC address to be overridden by envvar for instance_id calculation

## 0.5.4

- Don't die on session deletion when transfersession.records_total is None

## 0.5.3

- Cache the instance ID on app load, to avoid database lockup issues

## 0.5.2

- Split up `SyncClient` and fix bandwidth tracking (https://github.com/learningequality/morango/pull/85)

## 0.5.1

- Deserialization improvements (https://github.com/learningequality/morango/pull/84)

## 0.5.0

- Increase the stability of the Instance ID so it doesn't change as frequently (https://github.com/learningequality/morango/pull/83)

## 0.4.11

- Add serialized isolation level decorator for postgres transactions (https://github.com/learningequality/morango/pull/77)

## 0.4.10

- Bug fixes and performance improvements
- Enforce serializable isolation connection for postgres

## 0.4.9

- Fix for not sending correct cert chain

## 0.4.8

- Retry logic to better handle flaky network connections
- Introduce ALLOW_CERTIFICATE_PUSHING to support Cloud Kolibri
- Overall project refactor

## 0.4.7

- Small fixes

## 0.4.6

- Switch from file-based caching to Django's lru_cache for sqlite max vars

## 0.4.5

- fixes issue where GET requests with body payload fails

## 0.4.4

- adds gzipping capability on buffer post requests
- parametrizes chunk size to allow it to be set when initiating sync sessions

## 0.4.3

- remove unused files in dist

## 0.4.2

- Added fix for writing CACHE file on windows

## 0.4.1

- Added fix for writing CACHE file to user directory

## 0.4.0

- Added inverse CSR endpoints for pushing data to a server
- various performance improvements
- allow for hard deletion which purges data and is able to propagate to other devices

## 0.3.3

- Add transactions around queuing into buffer and dequeuing into store

## 0.3.2

- Mute signals before deserialization/saving store models

## 0.3.1

- removed logic of loading scope definition fixtures (delegated to main application)

## 0.3.0

- added support for postgres database backend

## 0.2.4

## 0.2.3

## 0.2.2

## 0.2.1

## 0.2.0

## 0.1.1

## 0.1.0

- First working version for morango

## 0.0.2: content-curation compatibility!

- make requirements more flexible

## 0.0.1: the initial release!

- Add in model name to uuid calc.
