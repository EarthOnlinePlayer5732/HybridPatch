# Security policy

## Scope and trust boundary

HybridPatch is local research and evaluation code, not a network service or a
multi-user application. Command-line paths are supplied by the trusted local
operator and should only point to project-owned experiment directories.

The database-schema evaluator may execute task or model-produced SQL against a
new SQLite `:memory:` database and inspect its schema with SQLite PRAGMAs. It
does not connect to an external database or reuse a persistent database. Treat
all model output as untrusted outside these isolated evaluators; do not execute
generated scripts or commands directly on a host.

Frozen `HP_V3`–`HP_V7` snapshots and `_attic/` preserve historical bytes for
replay and provenance. Security fixes that change runnable semantics belong in
a newly versioned active snapshot rather than being backported silently.

## Credentials and experiment artifacts

- Keep API keys only in the ignored top-level `.env` or `.env.frkeys` files.
- Never commit authorization headers, cookies, account tokens, or private keys.
- Raw `exp_*`, `api_raw/`, request payloads, and private datasets remain outside
  the ordinary Git repository. Follow `docs/EXPERIMENT_RECORDS.md` when
  sanitizing and archiving them.
- Test code may use synthetic secret-shaped fixture strings to verify redaction;
  these values are not credentials.

Before publishing changes, inspect the staged file set, compare it against local
secret values without printing those values, and run the records-only validator.

## Reporting a vulnerability

Do not place credentials or private experiment payloads in a public issue.
Report a vulnerability through a private GitHub Security Advisory for this
repository and include the affected version, reproduction conditions, and the
smallest safe evidence needed to investigate it.
