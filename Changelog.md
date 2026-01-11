# Changelog

## Version system

This project uses a four-part version:

- `V<major>.<minor>.<build>.<patch>`

Guidelines:

- **major**: breaking changes
- **minor**: new features / behavior changes without breaking
- **build**: day-to-day development increments
- **patch**: small hotfixes

## V1.1.100.2

- Added Google Sheets `Run History` tab (auto-create + append-only)
- Removed local CSV/JSON run history export to `Export/`
- Added `DD_VERBOSE_FORMS` flag to suppress noisy form-debug output by default
- Improved Profiles lookup matching (normalized nick keys) and prefill updates
- Improved URL-mode targeting and validation
- Added `--max-profiles` support
