# Changelog

## 2.3.0 - 2026-01-02

- MessageList sheet schema (MODE/NAME/NICK-URL/…/STATUS/NOTES/RESULT URL/DATE TIME DONE).
- MODE support: NICK or URL.
- Message templating variables: {name}, {city}, {nick}, {posts}, {followers}.
- Graceful Ctrl+C handling.
- Persistent CSV exports under folderExport/ (append-only): msg.csv, inbox.csv, activity.csv.
- Added 3 modes (Msg/Inbox/Activity) via DD_MODE.
- GitHub Actions schedule changed to every 6 hours.
- Configurable Google Sheets font via SHEET_FONT.
- Added .gitignore and README updates.
