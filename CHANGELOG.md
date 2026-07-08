# Changelog

## 2.7.0

### Features

- Provider-agnostic authentication for `EESession`. New factory constructors build a
  session from any standard credential source, holding a live, refreshable
  `google.auth` credential:
  - `EESession.from_service_account(info_or_path, ...)`
  - `EESession.from_earthengine_token(...)` — `EARTHENGINE_TOKEN` env, else the EE OAuth file
  - `EESession.from_google_credentials(creds, ...)`
  - `EESession.from_application_default(...)` — opt-in Application Default Credentials
  - `EESession.from_sepal_headers(headers, ...)` — names the existing SEPAL path
- A headerless `EESession()` now resolves credentials agnostically via a documented,
  all-local chain: SEPAL file → `EARTHENGINE_TOKEN` → Earth Engine OAuth file. ADC is
  never used implicitly — call `from_application_default()` to opt in.
- Live token refresh for google-auth-backed modes (removes the "file mode cannot
  self-refresh" limitation for them).

### Backward compatibility

- SEPAL session and SEPAL file modes are unchanged and take precedence.
- `SepalCredentialMixin` remains importable as an alias of the neutral `CredentialMixin`.
- Inside a SEPAL context (a `sepal-user` home) the default resolver is SEPAL-only and
  fails closed — it never falls through to a machine credential.

### Dependencies

- Declare `google-auth` and `requests` explicitly (previously transitive / undeclared).
