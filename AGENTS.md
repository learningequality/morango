# AGENTS.md

Morango is the Django library that syncs data between Kolibri devices. This file lists the conventions and pitfalls that agents need when they change it.

## Downstream users

Kolibri is the main user of Morango. It also imports private names, for example `morango.deferrable_foreign_keys._get_table_sql`.

- Before you remove or rename a name, search Kolibri for it: `gh search code "<name>" --repo learningequality/kolibri`.
- Kolibri pins an exact Morango version, so a removal breaks Kolibri only when it upgrades.
- If you remove or rename a name, add it to `CHANGELOG.md` with its replacement and any renamed arguments.

## Commands

Run these from the repository root. Set `PYTHONPATH=.:tests/testapp` first, as `tox.ini` does.

| Task | Command |
|---|---|
| Install | `uv sync --group dev` |
| Tests on SQLite | `make test`, which runs `python -O -m pytest tests/testapp/tests/` |
| Tests on PostgreSQL | `make test-with-postgres`. This target starts and stops the database with Docker Compose. |
| Tests on a running PostgreSQL | Set `DJANGO_SETTINGS_MODULE=testapp.postgres_settings`, then run pytest. |
| Lint and format | `prek run --all-files`. CI runs `uv run prek run --all-files --show-diff-on-failure`. |
| Migrations | `python tests/testapp/manage.py makemigrations --check` |

## Tests

- Run one test process at a time. The second SQLite test database is the file `tests/testapp/testapp2.db`, so parallel runs corrupt each other. The integration tests fail with errors that look real.
- The suite runs with `python -O`, which removes `assert` statements. For a check that must run, use `morango.utils._assert`.
- FK constraints are `DEFERRABLE INITIALLY DEFERRED` on PostgreSQL, and on SQLite after migration `0004_deferrable_foreign_keys`. A save that breaks an FK succeeds in its savepoint, and the transaction fails at commit.
- To test that a write commits, use `TransactionTestCase`. A `TestCase` only shows a deferred FK violation as an error when the test tears down.

## Database writes

- Do not use `bulk_update` for Morango writes. We measured it as slower than a loop of `save(update_fields=...)` calls, because Django spends the time on building the query.
- If all rows get the same values, use one `QuerySet.update()`. `bulk_create(ignore_conflicts=True)` is permitted.
- To delete app rows without signals, build the `Collector` inside `mute_signals(signals.post_delete)`. Django checks for signal receivers when it decides to fast-delete. Fast-deleted rows are in `collector.fast_deletes` as querysets, not in `collector.data`.

## Streaming pipelines

Serialization and deserialization run as `Source > Transform > Buffer > Sink` pipelines. The modules are in `morango/sync/stream/`:

- `core.py`: the pipeline types (`Source`, `Transform`, `Buffer`, `Unbuffer`, `Sink`).
- `serialize.py`: app models into the Store.
- `deserialize.py`: Store records into app models.

These rules apply to the pipeline modules:

- Transforms only read. All writes are in the sink and in the helper functions next to it.
- `Buffer` yields a chunk only when the next item arrives, or when a partition (if configured) or the entire stream ends. As a result, the sink writes a record up to one chunk, plus one item, after a transform validated it.
- Because of this lag, a sink must make sure that a record's FK targets still exist before it saves the record. A target can fail to save, or a cascade can delete it, after the record passed validation.
- Do not import `morango.sync.operations` from `morango/sync/stream/`. The stream modules import `morango.models.core` at module level, so `morango.models` must not import `morango.sync.stream` at module level.

## Git and pull requests

- Put any broad reformatting changes (across the repo) in its own commit, and add its full SHA to `.git-blame-ignore-revs`.
- The pull request template requires an AI usage section.

## Changelog

`CHANGELOG.md` has one `## <version>` heading for each release, with no `v` prefix. Add notes to the section of the version that is not released yet. If that section does not exist, add it at the top. Each entry is one change, in one line, starting with a verb.
