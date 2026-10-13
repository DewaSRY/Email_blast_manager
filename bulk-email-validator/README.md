# Bulk Email Validator MVP

A single-VPS starter for validating **permission-based email lists**. Stack: Next.js, TypeScript, Rust/Axum/Tokio, PostgreSQL, Docker Compose, Caddy.

## Validation scope
- Email syntax
- DNS domain existence and MX lookup (with A/AAAA fallback classification)
- Disposable-domain list matching (seed list included; update it from a source you trust)
- Conservative SMTP host reachability: connects to an MX on port 25, reads the greeting, and sends QUIT. **It does not issue `VRFY` or `RCPT TO` commands or claim a mailbox exists.** Mailbox-level SMTP probing is unreliable and can violate provider policies.

## Quick start
1. Install Docker Desktop or Docker Engine + Compose.
2. Copy `.env.example` to `.env` and change every secret.
3. Run `docker compose up --build -d`.
4. Open `http://localhost:3000` (for production, put Caddy/HTTPS in front and restrict access).

The MVP intentionally has simple shared-secret auth at the API boundary; don't expose it publicly until you replace that with real user authentication, HTTPS, rate limiting, and a secret manager. Do not upload purchased/scraped lists or use this to send unsolicited email.

## CSV format
- One email per row, or a column named `email` (case-insensitive).
- Maximum 10,000 rows and 10 MB upload.
- Results include original email, normalized email, syntax, domain, MX, disposable, SMTP host reachability, and a combined status/reason.

## API
- `GET /health`
- `POST /api/jobs` multipart form field `file`
- `GET /api/jobs` list recent jobs
- `GET /api/jobs/:id` job status and summary
- `GET /api/jobs/:id/results` paginated JSON results (`?limit=100&offset=0`)
- `GET /api/jobs/:id/export.csv` CSV download

## Production notes
- Configure `APP_API_KEY`. The Next.js server proxies requests to the Rust API and keeps the key server-side. The MVP still has no user login, so restrict access with a VPN, IP allowlist, or authentication layer before exposing it publicly.
- For a public multi-user deployment, add real user authentication, per-user job ownership, rate limiting, and quotas.
- DNS results can be inconclusive due to temporary resolver failures. SMTP reachability is host-level only, not proof that a specific mailbox exists.
- Check your VPS provider's port 25 policy. The app only makes outbound port-25 connections if `SMTP_REACHABILITY_ENABLED=true`.
