# Changelog

## Unreleased

### Fixes

- A shared `EESession` no longer fails when it is used from more than one event
  loop. Every asyncio object a session owned — the locks, the concurrency
  semaphore, the assets cache and its in-flight tasks, and the `httpx` connection
  pool — binds to the first loop that touches it and raises on any other. They now
  live in a `LoopResources` bundle resolved per running loop, so one session works
  across a host loop that closes and restarts, and across two live loops at once.

  Previously the first shape produced `RuntimeError: Event loop is closed` (or, on
  uvloop, `unable to perform operation on <TCPTransport closed=True ...>`) once per
  dead pooled connection — up to `max_keepalive_connections` consecutive failures,
  not the single retryable one it looked like. The second produced
  `RuntimeError: <asyncio.locks.Event object ...> is bound to a different event loop`, and a cache entry orphaned by a closed loop could block rather than raise.
  (#14, #37)

- `aclose()` no longer leaks the sockets of a loop that stops mid-teardown. It
  scheduled the remote close and discarded the future, so a loop that stopped just
  after the scheduling succeeded never ran it. Teardown now waits for that close and
  releases the descriptors directly when the loop will not run again.

- Credentials are refreshed once across every loop driving a session, instead of
  once per loop.

### Behavior changes

Not breaking, but worth knowing before you upgrade.

- A session now keeps **one connection pool per event loop** that drives it. Code
  using a single loop is unaffected. Code driving one session from two loops — a
  blocking API on a private loop plus an `async` API on the caller's loop — will
  hold two pools, so socket usage can double. This is what makes that pattern work
  at all; it previously raised.

- The concurrency cap (30 in-flight) is now **per loop**. The rate limit (60 QPS)
  stays **process-wide**: it guards a per-user Earth Engine quota, so it is
  deliberately loop-free and is not multiplied by the number of loops.

- `aclose()` is now callable from any loop that has driven the session and closes
  every loop's transport, rather than only the loop that created the client.

### Internal

- New module `eeclient/loopstate.py` holds the per-loop scoping, the loop-free
  rate limiter, and the cross-loop single-flight.
- `SimpleRateLimiter` moved there from `eeclient.client` (still importable from
  `eeclient.client`) and is now backed by a `threading.Lock` and `time.monotonic()`.
  It no longer holds a lock across `asyncio.sleep()`, which removed a serialization
  point under bursts.
- The private attributes `_client`, `_client_lock`, `_inflight` and
  `_auth_refresh_lock` are gone from `EESession`; `_assets_cache` is now a property
  resolving to the running loop's cache.

## 3.1.0

### Behavior changes

Not breaking, but worth reading before you upgrade. Both entries below change
_when_ and _which_ exception fires for a credentials file that never worked: an
Earth Engine credentials file without a usable refresh token has never resolved
to a refreshable credential, in this library or in `earthengine-api` itself
(`ee.data.get_persistent_credentials()` gates on `refresh_token` and otherwise
falls through to ADC). No working configuration is affected, and both new errors
subclass `CredentialsResolutionError`, which these paths did not raise before —
so `except CredentialsResolutionError` now catches strictly more than it did.

One operational note: the failure moves from first token refresh to session
construction. A long-running service that builds its session at import and
currently starts up before failing on the first request will now fail at
startup instead.

- A service-account-shaped file at the Earth Engine credentials path
  (`~/.config/earthengine/credentials`) is now **refused** by
  `resolve_default_provider()` / `EESession.from_default()`, raising
  `ServiceAccountFileRefusedError`. That path is a machine-wide identity;
  resolving it implicitly hands every caller on the host the same Earth Engine
  account. Pass `allow_service_account_file=True` to opt in, or use
  `EESession.from_service_account()` directly.

  Previously such a file produced an OAuth credential with `refresh_token=None`:
  `from_default()` returned, and the failure surfaced later as a
  `google.auth.exceptions.RefreshError` on `initialize()` / the first
  `get_headers()`. It now fails at construction with an actionable message, and
  the opt-in makes the file usable for the first time. Nothing to migrate unless
  you caught `RefreshError` around that first call.

  In a SEPAL context (`sepal-user` home) resolution is SEPAL-file-only, so this
  guard does not apply there.

- A credentials file that is unparsable, is not a JSON object, or carries no
  `refresh_token` now raises `CredentialsFileUnrecognizedError` from
  `resolve_default_provider()` instead of falling through to the OAuth loader.
  Same shape of change: the error moves from first refresh to construction.

  A file that exists but cannot be read (permissions) raises
  `CredentialsResolutionError` chaining the original `OSError`, rather than
  being reported as a malformed credential.

### Features

- `session.auth_source` gains `ee_service_account_file` for a service-account
  key resolved from the Earth Engine credentials path via the new opt-in.
- `CredentialMixin.close()` releases the sync `_service` handle if one is
  attached. `EESession.aclose()` now calls it, so `await session.aclose()` is
  the complete teardown.
- New `CredentialsFileUnrecognizedError`, a `CredentialsResolutionError`
  subclass carrying the offending path. Every failure mode of
  `EESession.from_default()` is now catchable as `CredentialsResolutionError`;
  the SEPAL-specific `CredentialsFileUnknownError` is no longer raised from the
  provider-agnostic path.

## 3.0.0

### Breaking changes

- A bare `EESession()` with no credential source now **raises** instead of resolving
  credentials implicitly. Use `EESession.from_default()` (opt-in environment resolution)
  or one of the `from_*()` factories; `create()` with no `sepal_headers` also raises.
- `~/.config/earthengine/sepal_credentials` is no longer auto-discovered as a non-SEPAL
  credential source (it was a stale-prone, non-refreshable artifact).

### Features

- Provider-agnostic authentication for `EESession`. New factory constructors build a
  session from any standard credential source, holding a live, refreshable
  `google.auth` credential:
  - `EESession.from_service_account(info_or_path, ...)`
  - `EESession.from_earthengine_token(...)` — `EARTHENGINE_TOKEN` env, else the EE OAuth file
  - `EESession.from_google_credentials(creds, ...)`
  - `EESession.from_application_default(...)` — opt-in Application Default Credentials
  - `EESession.from_sepal_headers(headers, ...)` — names the existing SEPAL path
  - `EESession.from_default()` — resolve from the environment (opt-in)
- Resolution is **never implicit**: a bare `EESession()` requires an explicit source.
  `EESession.from_default()` resolves from the environment, walking local sources only:
  `EARTHENGINE_TOKEN` → Earth Engine OAuth file. ADC is not included; call
  `from_application_default()`.
- Live token refresh for google-auth-backed modes (removes the "file mode cannot
  self-refresh" limitation for them).
- `session.auth_source` reports the precise credential origin (`sepal_session` /
  `sepal_file` / `earthengine_token` / `ee_oauth_file` / `service_account` /
  `application_default` / `google_credentials`) — finer-grained than `auth_mode`.

### Backward compatibility

- SEPAL session and SEPAL file modes are unchanged and take precedence.
- `SepalCredentialMixin` remains importable as an alias of the neutral `CredentialMixin`.
- Inside a SEPAL context (a `sepal-user` home) the default resolver is SEPAL-only and
  fails closed — it never falls through to a machine credential.

### Dependencies

- Declare `google-auth` and `requests` explicitly (previously transitive / undeclared).
