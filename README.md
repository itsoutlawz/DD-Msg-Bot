# DamaDam Message Bot (v2.3.0)

[![Run Scraper](https://github.com/itsoutlawz/DD-Msg-Bot/actions/workflows/scraper.yml/badge.svg)](https://github.com/itsoutlawz/DD-Msg-Bot/actions/workflows/scraper.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Version](https://img.shields.io/badge/version-2.3.0-blue)

Automation that logs into [damadam.pk](https://damadam.pk), scrapes target profiles, posts tailored replies, and records activity in Google Sheets.

**Default Social Handler:** [@net2nadeem](https://damadam.pk/users/0utLawZ/) | **Email:** [net2outlawzz@gmail.com](mailto:net2outlawzz@gmail.com)

## Features

- Headless Selenium workflow tuned for damadam.
- Google Sheets integration with retry logic and formatting helpers.
- **Cookie-based authentication** - Enforces secure login via saved cookies.
- MessageList runner with MODE support (NICK / URL).
- 3 bot modes via `DD_MODE`: `Msg`, `Inbox`, `Activity`.
- Message variables: `{name}`, `{city}`, `{nick}`, `{posts}`, `{followers}`.
- Local CSV export saved under `folderExport/` (append-only).
- GitHub Actions scheduled to run every 6 hours.
- Manual run options with configurable profile count (10/50/100) and batch size.
- Dynamic SOURCE field mapping.
- Automatic URL cleaning for posted message links.

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

4. Set the environment variables (or fill `.env` locally):

   ```bash
   export DD_LOGIN_EMAIL=your_nick
   export DD_LOGIN_PASS=your_password
   export DD_SHEET_ID=your_sheet_id
   export COOKIE_FILE=damadam_cookies.pkl   # optional override
   export SHEET_FONT=Asimovian              # optional
   ```

5. **Initial Setup - Generate Cookies:**

   First run will authenticate and save cookies for future use:

   ```bash
   python Scraper.py
   ```

## Running modes

You can choose a mode via CLI (overrides `DD_MODE`):

```bash
python Scraper.py --mode msg
python Scraper.py --mode inbox
python Scraper.py --mode activity
```

Notes:

- Inbox (Replies) runs against: `https://damadam.pk/inbox/`
- Activity runs against: `https://damadam.pk/inbox/activity/`
- Pages require login/cookies.

## GitHub Actions

The workflow at `.github/workflows/scraper.yml`:

1. **Scheduled runs:** Executes automatically every 6 hours (`0 */6 * * *`).
2. **Manual runs:** Trigger via `workflow_dispatch` with options:
   - **profiles_count:** Choose how many profiles to scrape (1=10, 2=50, 3=100)
   - **batch_size:** Set batch processing size (default: 5)
   - **comment:** Optional note for the run
3. Checks out the repo, installs Python, Chrome, and dependencies.
4. Writes the service account JSON from `DD_CREDENTIALS_JSON` secret into `credentials.json`.
5. Exports the required secrets (`DD_LOGIN_EMAIL`, `DD_LOGIN_PASS`, `DD_SHEET_ID`, optional `COOKIE_FILE`).
6. Executes `python Scraper.py` inside the runner with saved cookies.

### Secrets management tips

- Never store the service account JSON in the repository. Use `DD_CREDENTIALS_JSON` to deliver it securely.
- If you need to override the cookie path, add a `COOKIE_FILE` secret (matching the env variable name).

## Google Sheet structure

Create a tab named `MessageList` with these headers (row 1):

1. `MODE`
2. `NAME`
3. `NICK/URL`
4. `CITY`
5. `POSTS`
6. `FOLLOWRS`
7. `MESSAGE`
8. `STATUS`
9. `NOTES`
10. `RESULT URL`
11. `DATE TIME DONE`

Only rows with `STATUS` = `Pending` are processed.

### MODE behavior

- `MODE = NICK` → column `NICK/URL` is treated as nickname and converted to `https://damadam.pk/users/<nick>/`.
- `MODE = URL` → column `NICK/URL` is treated as a full URL and opened as-is.

### Message variables

In `MESSAGE` you can use:

- `{name}`
- `{city}`
- `{nick}`
- `{posts}`
- `{followers}`

## Output files

- Persistent CSV exports (append-only):
  - `folderExport/msg.csv`
  - `folderExport/inbox.csv`
  - `folderExport/activity.csv`

## Credits

- **Owner/Credit:** Nadeem
- **Assistance:** GPT-5.2

## About DamaDam

**DamaDam** is a Pakistani social networking platform for connecting with people, sharing content, and building communities.

- **Website:** [damadam.pk](https://damadam.pk)
- **Bot Handler Profile:** [0utLawZ](https://damadam.pk/users/0utLawZ/)
- **Handler Avatar:** [Profile Image](https://d3h48bfc4uelnv.cloudfront.net/avatar-imgs/922b1ef8-afc2-45b4-ae7e-46b50d202fd0.jpg)

## Troubleshooting

- Monitor workflow logs for Selenium/Chrome/Sheets errors.
- Ensure the service account has at least editor access to the spreadsheet.
- Manual verification statuses are logged inside the sheet to guide follow-ups.
- If cookies expire, the bot will re-authenticate and save new cookies automatically.
