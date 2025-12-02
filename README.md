# DamaDam Message Bot (v2.2)

Automation that logs into [damadam.pk](https://damadam.pk), scrapes target profiles, posts tailored replies, and records activity in Google Sheets.

## Features

- Headless Selenium workflow tuned for damadam.
- Google Sheets integration with retry logic and formatting helpers.
- Cookie caching to skip repeated logins.
- 🔁 GitHub Actions fn to run the bot every 30 minutes by default.

## Requirements

- Python 3.11+
- Chrome/Chromium + matching `chromedriver`
- Google service account with Sheets access
- GitHub repository with the following secrets:
  - `DD_LOGIN_EMAIL`
  - `DD_LOGIN_PASS`
  - `DD_SHEET_ID` (target spreadsheet ID)
  - `DD_CREDENTIALS_JSON` (full JSON payload of the Google service account credentials)

## Local setup

1. Create or download a Google service account JSON (read-only Sheets permissions).
2. Save a copy as `credentials.json` in the repo root (or set `CREDENTIALS_FILE` via env).
3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Set the environment variables exported in your shell:

   ```bash
   export DD_LOGIN_EMAIL=your_nick
   export DD_LOGIN_PASS=your_password
   export DD_SHEET_ID=your_sheet_id
   export COOKIE_FILE=damadam_cookies.pkl   # optional override
   ```

5. Run the bot:

   ```bash
   python Scraper.py
   ```

## GitHub Actions

The workflow at `.github/workflows/scraper.yml`:

1. Runs on `workflow_dispatch` and on a cron schedule (`*/30 * * * *`).
2. Checks out the repo, installs Python, Chrome, and dependencies.
3. Writes the service account JSON from `DD_CREDENTIALS_JSON` secret into `credentials.json`.
4. Exports the required secrets (`DD_LOGIN_EMAIL`, `DD_LOGIN_PASS`, `DD_SHEET_ID`, optional `COOKIE_FILE`).
5. Executes `python Scraper.py` inside the runner.

### Secrets management tips

- Never store the service account JSON in the repository. Use `DD_CREDENTIALS_JSON` to deliver it securely.
- If you need to override the cookie path, add a `COOKIE_FILE` secret (matching the env variable name).

## Troubleshooting

- Monitor workflow logs for Selenium/Chrome/Sheets errors.
- Ensure the service account has at least editor access to the spreadsheet.
- Manual verification statuses are logged inside the sheet to guide follow-ups.
