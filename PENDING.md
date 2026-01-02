# Pending Items

Last updated: 2026-01-02

This file tracks what is left to do.

---

## Completed

- Mode: `msg`
- CLI: `python Scraper.py --mode msg`
- Sheets safety: headers update is non-destructive (does not clear existing rows)
- Google Sheets transient timeout retry/backoff
- Persistent CSV exports under `folderExport/` (append-only)
- GitHub Actions schedule every 6 hours

---

## Pending

### 1) Msg-only end-to-end verification

- Verify MsgList -> Profiles lookup fills CITY/POSTS/FOLLOWRS.
- Verify NICK mode finds post pages `/profile/public/<nick>/?page=1..5` and comments.
- Verify URL mode opens the direct comments URL and comments.

---
