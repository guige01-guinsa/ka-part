# civil_api_node

Express scaffold for apartment complaint handling (`/api/v1`).

## Run

```bash
cd services/civil_api_node
npm install
npm start
```

## Auth model (scaffold)

- Resident calls: `X-User-Id` header.
- Admin calls: `X-User-Id` and `X-Role: admin` (or `staff`).

Storage is in-memory for PoC. Replace `store.js` with PostgreSQL.
