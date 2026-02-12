# Complaints API (`/api/v1`)

Integrated into `app/main.py` via `app/routes/complaints.py`.

## Auth

Use existing session token:

- `Authorization: Bearer <token>`
- or auth cookie (`ka_part_auth_token`)

## Public endpoints

- `GET /api/v1/codes/complaint-categories`
- `GET /api/v1/notices`
- `GET /api/v1/faqs`
- `POST /api/v1/complaints`
- `POST /api/v1/emergencies`
- `GET /api/v1/complaints`
- `GET /api/v1/complaints/{id}`
- `POST /api/v1/complaints/{id}/comments`

## Admin endpoints

- `GET /api/v1/admin/complaints`
- `GET /api/v1/admin/complaints/{id}`
- `PATCH /api/v1/admin/complaints/{id}/triage`
- `POST /api/v1/admin/complaints/{id}/assign`
- `PATCH /api/v1/admin/work-orders/{id}`
- `POST /api/v1/admin/visits`
- `PATCH /api/v1/admin/visits/{id}/checkout`
- `POST /api/v1/admin/notices`
- `PATCH /api/v1/admin/notices/{id}`
- `GET /api/v1/admin/stats/complaints`

## Storage

- SQLite tables are auto-created in `data/ka.db` by `app/complaints_db.py`.
- PostgreSQL DDL reference: `sql/postgres/20260212_complaints_v1.sql`.

## Resident PWA

- URL: `/pwa/complaints.html`
- Features:
  - complaint submit (common/private/emergency)
  - my complaint list + status filter
  - complaint detail timeline (status history)
  - comment thread
  - admin processing panel (visible to admin/site_admin only)

## Security notes

- Client-side CSP is applied in `complaints.html`.
- Attachment URLs are validated on both client and server:
  - only `http/https`
  - max 10 URLs
  - max 500 chars per URL
