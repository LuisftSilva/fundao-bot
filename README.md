# Fundão Bot

Fundão Bot is a Cloudflare Worker that bridges Telegram with the ResIOT API so that on-call teams can monitor the municipality's LoRa gateways. The worker exposes a Telegram webhook, calls the ResIOT `/api/gateways` endpoints on demand, and keeps a full uptime history in a GitHub Gist for later reporting.

## Architecture

- **Telegram webhook** – Messages arrive via `/webhook[/SECRET]`. The worker authenticates the chat against an allow‑list stored in the same GitHub Gist and responds with HTML tables that fit Telegram limits. The `/start` command installs a reply keyboard so users can tap “Status” or “Histórico” instead of typing commands.
- **ResIOT integration** – `/status`, `/status_ok`, `/status_nok`, `/fetch`, and `/history` call `RESIOT_BASE/api/gateways` (plus `/lastmessage` as fallback) using the `RESIOT_TOKEN` gRPC metadata header.
- **GitHub Gist storage** – The worker keeps:
  - `allowlist_fundao_bot.json`: Telegram chat IDs with access (admin is always whitelisted).
  - `gateways_uptime_last.json`: snapshot of the most recent known state per gateway.
  - `gateways_uptime_carry_YYYY-MM.json`: carry‑over state at the beginning of each month.
  - `gateways_uptime_transitions_YYYY-MM.ndjson`: NDJSON stream with `{ t, gw, s }` events.
- **Cron worker** – Runs every five minutes (`*/5 * * * *`) to poll ResIOT, diff states, append transitions, and refresh the snapshot/carry files. The history commands reconstruct timelines by replaying these files.

## Telegram UX

- `/start` or `/help` – installs the reply keyboard and explains the available actions.
- `/status` `/status_ok` `/status_nok` – renders the current state of the gateways with name lookups from `GATEWAY_NAMES_JSON`.
- `/history <index> [days]` – rebuilds the requested interval (default 7 days, max 90) and prints:
  - Summary (uptime %, total minutes online/offline, time range).
  - Transitions table with `Dia | Hora | Estado`.
- `/fetch <gwEUI> [days] [step]` – diagnostic command that logs granular slot data.
- The inline allow/deny buttons in admin DMs update the `allowlist_fundao_bot.json` file.

## Requirements

- Node.js 18+
- npm 9+
- Cloudflare Wrangler 4.x
- Access to the ResIOT API and a GitHub personal access token with `gist` scope.

## Configuration

Set the following environment variables (Wrangler supports `.dev.vars` for local runs):

| Variable | Description |
| --- | --- |
| `TELEGRAM_BOT_TOKEN` | Bot token from BotFather. |
| `TELEGRAM_WEBHOOK_SECRET` | Optional path segment used to secure the webhook (`/webhook/<secret>`). |
| `RESIOT_BASE` | Base URL for ResIOT (e.g., `https://api.resiot.io`). |
| `RESIOT_TOKEN` | API token sent as `Grpc-Metadata-Authorization`. |
| `GATEWAY_NAMES_JSON` | JSON object mapping ResIOT codes to human-friendly names. |
| `GITHUB_TOKEN` | Token with `gist` permission to read/write uptime and allow-list files. |
| `GITHUB_GIST_ID` | ID of the gist that stores allow-list and uptime artifacts. |

All secrets **must** be managed via Wrangler (e.g., `wrangler secret put`) when deploying to production to avoid leaking credentials in the public repository.

## Local Development

1. Install dependencies: `npm install`.
2. Create `.dev.vars` with your test credentials (or use `wrangler secret put` interactively).
3. Start the worker: `npm run dev` (alias for `wrangler dev`). Telegram webhooks can be tested with `wrangler dev --local` plus a tunneling service, or by using the `/debug/gist` endpoint to verify gist access.

## Deployment

1. Make sure `wrangler.toml` / `wrangler.jsonc` defines the target account, routes, and cron schedule.
2. Publish the worker: `npm run deploy` (`wrangler deploy`).
3. Point the Telegram bot webhook to the deployed URL, optionally including the webhook secret.

## Testing & Quality

The project currently relies on manual verification (there are no Vitest suites yet). Before shipping changes, validate:

- `/debug/gist` responds with `ok: true`.
- `/status` and `/history` render correctly for both OK and NOK gateways.
- The cron logs show `"[cron] ok. add: ..."` after polling ResIOT.

Consider adding Vitest suites around helper functions (e.g., gist utilities, date formatting, NDJSON reconstruction) as a future enhancement.

## Operational Notes

- Keep the gist files small by pruning historical months or rotating files if they approach the 5 MB limit.
- If multiple cron instances may run in parallel, add locking (KV/Durable Object) before appending to the gist to avoid overwriting.
- Monitor Telegram log output for `'resiot error'`, `'gist append'`, or `'history error'` to catch API failures early.
