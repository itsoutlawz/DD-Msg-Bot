# Pending Items

Last updated: 2026-01-02

This file tracks what is left to do.

---

## Completed

- Modes: `msg`, `inbox`, `activity`
- CLI: `python Scraper.py --mode <msg|inbox|activity>`
- Inbox parsing using provided selectors; writes to Google Sheet `Inbox` and appends `folderExport/inbox.csv`
- Activity parsing for `https://damadam.pk/inbox/activity/`; writes to Google Sheet `Activity` and appends `folderExport/activity.csv`
- Relationship key: `REL_KEY` column added to Inbox/Activity for cross-reference/history
- Sheets safety: headers update is non-destructive (does not clear existing rows)
- Google Sheets transient timeout retry/backoff
- Persistent CSV exports under `folderExport/` (append-only)
- GitHub Actions schedule every 6 hours

---

## Pending

### 1) Pagination verification (selectors implemented)

- Implemented Next-page discovery using selector: `a[href*="?page="]`
- Logs page indicator when present: `h1 span.cs`

Still needed:

- Verify on a real account with 20+ items that the pagination link works correctly and does not loop.

### 2) Likes history (optional)

- URL: `https://damadam.pk/user/likes/history/`
- Not implemented as a mode yet.

### 3) Activity detail fields / post link normalization

- Some fields may differ by section; verify selectors on Activity page.

### 4) Tests / stability

- Run full test with actual inbox items (Replies + Activity) and confirm:
  - rows correctly populated
  - REL_KEY values match expected
  - CSV appends correctly

---
