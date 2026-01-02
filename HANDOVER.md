# DD-Msg-Bot — Handover / Work Summary

Date: 2026-01-02

This file explains what was implemented, how to run it, what sheets/files it uses, and what is still pending.

---

## 1) What this bot does (high level)

This project automates Damadam.pk actions using Selenium + cookies auth and writes data into Google Sheets.

It supports **3 main modes**:

- **Msg**: reads `MessageList` sheet rows with `STATUS=Pending`, opens the target profile, finds an open post, posts message, and updates the sheet.
- **Inbox**: scrapes `https://damadam.pk/inbox/` (Replies for me), parses each inbox card, and writes it to `Inbox` sheet + local CSV.
- **Activity**: scrapes `https://damadam.pk/inbox/activity/`, parses cards using same selectors, and writes it to `Activity` sheet + local CSV.

---

## 2) How to run (local)

### Install

```bash
pip install -r requirements.txt
```

### Environment / files

- `credentials.json` (Google service account) **must exist locally** but **must NOT be committed**.
- `damadam_cookies.pkl` will be created/used for login session.

### Run modes (recommended)

`--mode` overrides `DD_MODE` env.

```bash
python Scraper.py --mode msg
python Scraper.py --mode inbox
python Scraper.py --mode activity
```

---

## 3) Google Sheet tabs + schemas

### A) MessageList (Msg mode)

Processed rows:

- Only rows where **STATUS = Pending**

Headers (Row 1):

1. MODE
2. NAME
3. NICK/URL
4. CITY
5. POSTS
6. FOLLOWRS
7. MESSAGE
8. STATUS
9. NOTES
10. RESULT URL
11. DATE TIME DONE

Behavior:

- MODE=NICK: converts nick to URL `https://damadam.pk/users/<nick>/`
- MODE=URL: uses full URL directly
- Updates:
  - STATUS -> Done/Error
  - NOTES -> reason (posted, manual verify, no posts, no open posts, etc.)
  - RESULT URL -> post URL if available
  - DATE TIME DONE -> timestamp

### B) Inbox (Inbox mode)

Tab name: `Inbox`

Columns written:

- DATETIME
- SECTION
- ITEM_TYPE
- AUTHOR
- SNIPPET
- TIME
- POST_LINK
- IMAGE_LINK
- THUMBNAIL
- CAPTION
- TID
- OBID
- POID
- TUID
- ORIGIN
- REL_KEY
- HAS_REPLY_FORM
- CSRF_PRESENT
- PAGE_URL

### C) Activity (Activity mode)

Tab name: `Activity`

Same columns as Inbox.

### Relationship key (Inbox <-> Activity)

A shared column **REL_KEY** was added to both Inbox + Activity rows so you can match history across both sheets.

- If item is 1-on-1: `REL_KEY = tid:<tid>`
- Else: `REL_KEY = tuid:<tuid>|poid:<poid>|obid:<obid>|origin:<origin>`

---

## 4) Local CSV exports (append-only)

Folder: `folderExport/` (gitignored)

Files:

- `folderExport/msg.csv`
- `folderExport/inbox.csv`
- `folderExport/activity.csv`

Rules:

- Files are **append-only**.
- If file is missing/empty, header is written once.

---

## 5) Authentication rules (Damadam)

- Login + cookies required for Inbox/Activity.
- CSRF token is detected per item (`CSRF_PRESENT` column).

---

## 6) GitHub Actions

Workflow: `.github/workflows/scraper.yml`

- Runs on schedule: **every 6 hours**
- Manual workflow_dispatch supports a mode input (`DD_MODE` default Msg)

---

## 7) Important safety / secrets

Currently **tracked in git**:

- `damadam_cookies.pkl` is tracked (must be removed from git tracking before pushing).

Not tracked (good):

- `credentials.json` is gitignored.

---

## 8) Pending work (not finished)

### A) Pagination

Inbox/Activity pagination appears after 20 items.

Current code uses a temporary heuristic for Next links (searching for text "Next").

To finish reliably, you need to provide:

- Exact NEXT/PREV selector OR HTML snippet of the pagination block when it appears.

### B) Likes history

URL: `https://damadam.pk/user/likes/history/`
Not implemented as a separate mode yet.

---

## 9) How to push to GitHub safely

Before commit/push, remove cookies from git tracking:

```bash
git rm --cached damadam_cookies.pkl
```

Then stage + commit + push:

```bash
git add .
git status -sb
git commit -m "Final changes: modes, inbox/activity sheets, CSV append-only, 6h workflow"
git push
```

Make sure these do NOT appear in staged files:

- `credentials.json`
- `damadam_cookies.pkl`
- `folderExport/`

---
