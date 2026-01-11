# DD-Msg-Bot (DamaDam Message Bot)

Automates posting messages on damadam.pk based on rows in a Google Sheet.

Current version: `V1.1.100.2`

## What it does

- Reads `pending` rows from Google Sheet tab `MsgList`
- Chooses target based on `MODE`:
  - `url`: posts directly to a given comments URL
  - `nick`: scrapes profile, finds an open post, then posts
- Writes back results to `MsgList` (`STATUS`, `NOTES`, `RESULT URL`)
- Appends run results into a second tab: `Run History` (auto-created)

## Google Sheet tabs

### 1) MsgList (required)

| Column | Name | Description |
|---|---|---|
| A | MODE | `url` or `nick` |
| B | NAME | Used in templates and Profiles lookup |
| C | NICK/URL | Nickname (nick mode) OR comments URL (url mode) |
| D | CITY | Auto-filled when available |
| E | POSTS | Auto-filled when available |
| F | FOLLOWERS | Auto-filled when available |
| G | MESSAGE | Template message |
| H | STATUS | `pending` → `Done/Failed/Skipped` |
| I | NOTES | Notes or error summary |
| J | RESULT URL | Cleaned URL used/succeeded |

### 2) Run History (auto-created)

The bot creates this sheet if missing and **always appends** (never clears).

Columns:

- `RUN ID`, `RUN TS`, `MODE`, `TARGET`, `NAME`, `STATUS`, `RESULT URL`, `MESSAGE`, `PROCESSED`, `SUCCESS`, `FAILED`, `GSHEET API CALLS`

## Message templates

You can use:

- `{{name}}`
- `{{city}}`
- `{{posts}}`
- `{{followers}}`

Example:

```
Hello {{name}}! City: {{city}} | Posts: {{posts}} | Followers: {{followers}}
```

## Setup

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Google Sheets credentials

- Create a Google Service Account
- Download `credentials.json` into the project folder
- Share your Google Sheet(s) with the service account email

### 3) Environment variables

```bash
DD_LOGIN_EMAIL=your_username
DD_LOGIN_PASS=your_password
DD_SHEET_ID=your_google_sheet_id

# Optional
DD_PROFILES_SHEET_ID=profiles_sheet_id
DD_DEBUG=0
DD_VERBOSE_FORMS=0
DD_MAX_PROFILES=0
DD_MAX_POST_PAGES=4
DD_AUTO_PUSH=0
```

## Usage

Run normally:

```bash
python Scraper.py
```

Limit processing:

```bash
python Scraper.py --max-profiles 3
```

Deep form debugging (very noisy):

```bash
DD_DEBUG=1 DD_VERBOSE_FORMS=1 python Scraper.py --max-profiles 1
```
