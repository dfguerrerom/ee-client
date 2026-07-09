# Changelog

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
