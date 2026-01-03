# DD-Msg-Bot v2.2 - Clean Structure

DamaDam Message Bot with automated profile scraping and message sending capabilities.

## Features

- **Auto-Create Sheets**: Automatically creates MsgList sheet with proper structure
- **MODE Support**: Supports both `nick` and `url` modes
- **Template Messages**: Dynamic message templates with `{{city}}`, `{{posts}}`, `{{followers}}` placeholders
- **Thread Safety**: Built-in locks for safe concurrent operations
- **Single Sheet Design**: Uses only MsgList sheet for simplicity
- **Success URL Tracking**: Automatically logs successful post URLs

## Sheet Structure

### MsgList Sheet

| Column        | Description              | Example          |
|---------------|-------------------------|------------------|
| MODE          | `nick` or `url`        | nick             |
| NAME          | Display name            | Test User 1      |
| NICK/URL      | Nickname or direct URL  | Afshan_Qureshi   |
| CITY          | City (auto-populated)  | Karachi          |
| POSTS         | Post count (auto-populated) | 218       |
| FOLLOWERS     | Followers count (auto-populated) | 150    |
| MESSAGE       | Template message        | Hi {{name}} from {{city}}! |
| STATUS        | Status (auto-updated)  | Done/Failed      |
| NOTES         | Additional notes        | Manual check needed |
| RESULT URL    | Success URL (auto-populated) | https://... |

## Message Templates

Use placeholders in your message column:

- `{{city}}` - User's city
- `{{posts}}` - User's post count  
- `{{followers}}` - User's followers count

Example: `Hi {{name}} from {{city}}! You have {{posts}} posts.`

## Setup

1. **Google Sheets API**
   - Enable Google Sheets API
   - Create service account
   - Download `credentials.json`

2. **Environment Variables**

   ```bash
   DD_LOGIN_EMAIL=your_username
   DD_LOGIN_PASS=your_password
   DD_SHEET_ID=your_sheet_id
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

## Usage

```bash
python Scraper.py
```

The bot will:

1. Connect to Google Sheets
2. Process all "pending" targets in MsgList
3. Update status and result URLs automatically
4. Handle both nickname and URL modes

## Modes

### Nick Mode (MODE=nick)

- Scrapes user profile first
- Validates account status
- Finds open posts automatically
- Processes template with scraped data

### URL Mode (MODE=url)

- Uses direct URL without scraping
- Processes template with provided data
- Faster for direct post targeting

## Safety Features

- **Thread Locks**: Prevents concurrent sheet access issues
- **Error Handling**: Comprehensive error recovery
- **Verification**: Double-checks message posting
- **Session Management**: Automatic cookie handling

## Output

- ✅ **Success**: Message posted and verified
- ⚠️ **Manual Check**: Message sent, needs manual verification  
- ❌ **Failed**: Error occurred during processing

All results include success URLs for easy verification.

## Clean Structure

- Single `Scraper.py` file
- No unnecessary dependencies
- Clear separation of concerns
- Production-ready code

## Version

v2.2 - Clean Structure Release

- Auto-create sheet functionality
- MODE support (nick/URL)
- Template message processing
- Thread safety implementation
- Single MsgList sheet design
