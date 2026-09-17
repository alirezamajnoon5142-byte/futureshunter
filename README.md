# FuturesHunter V6.6 — Render deploy

## GitHub
Create a new repository and upload every file in this folder **except** your real `.env` file.

## Render
1. Create a new Blueprint or Web Service from the GitHub repository.
2. Render should detect `render.yaml`.
3. Add secret environment variables in Render:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - `WEBSITE_URL` (optional)
4. Deploy.
5. Open `https://YOUR-RENDER-URL/health` and confirm `"ok": true`.
6. In Telegram run `/status`, `/watch`, `/why ZEC`, and `/shadow`.

## Important free-tier limitation
Render Free web services spin down after periods without inbound traffic. Local files are also ephemeral, so scanner JSON/CSV state may reset on redeploy/restart. This package is excellent for free live testing, but not guaranteed uninterrupted 24/7 operation.
