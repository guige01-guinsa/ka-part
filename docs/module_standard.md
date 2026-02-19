# KA-PART Module Standard (v1)

## Goal
- Keep existing apartment facility operations stable while adding new modules.
- Use one contract across DB, API, and UI to reduce regressions and latency spikes.

## 1. DB Contract
- Table: `module_contracts`
- Primary key: `module_key`
- Required fields:
  - `module_name`
  - `status` (`active` | `beta` | `disabled`)
  - `ui_path`
  - `api_prefix`
  - `default_limit`
  - `max_limit`
  - `query_timeout_ms`
  - `cache_ttl_sec`
  - `sort_order`
  - `created_at`
  - `updated_at`

Notes:
- `init_db()` seeds default contracts (`main`, `parking`, `complaints`, `inspection`, `electrical_ai`).
- Existing records are preserved; only invalid/empty values are normalized.

## 2. API Contract
- Endpoint: `GET /api/modules/contracts`
- Auth: same as `GET /api/auth/me`
- Response:
  - `allowed_modules`: module keys available to current user role
  - `contracts`: active contracts filtered by `allowed_modules`

Role behavior:
- `security_guard` users: only `parking`
- `resident` / `board_member`: only `complaints`
- others: active modules from `module_contracts` (ordered by `sort_order`)

## 3. UI Contract
- Shared base script: `static/pwa/module_base.js`
- Required load order per module page:
  1. `auth.js`
  2. `module_base.js`
  3. `<module>.js`

Recommended module bootstrap flow:
1. `KAAuth.requireAuth()`
2. `KAModuleBase.bootstrap(moduleKey, { defaultLimit, maxLimit })`
3. Build API query via `ctx.withSite(path, extraQuery)`
4. Clamp list limits via `ctx.clampLimit(value, fallback)`

## 4. Performance Guardrails
- SQLite connection best-effort tuning:
  - `journal_mode=WAL`
  - `synchronous=NORMAL`
  - `temp_store=MEMORY`
  - `busy_timeout`
- Module list APIs must clamp `limit` by contract (`default_limit`, `max_limit`).
- Keep query filters site-scoped (`site_code`) and indexed.

## 5. New Module Checklist
1. Add/enable module row in `module_contracts`.
2. Create API router file under `app/routes/`.
3. Add PWA files under `static/pwa/`:
   - `<module>.html`
   - `<module>.css`
   - `<module>.js`
4. Load `auth.js` + `module_base.js` in `<module>.html`.
5. Add module to role-aware navigation only after bootstrap API check passes.
6. Run smoke checks on login, role access, and list endpoints with high `limit` values.
