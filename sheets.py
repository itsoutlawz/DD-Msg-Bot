import os
import time
import json

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

from config import CREDENTIALS_FILE, DD_SHEET_ID, SHEET_FONT
from utils import log_msg

ALIGN_MAP = {"L": "LEFT", "C": "CENTER", "R": "RIGHT"}
WRAP_MAP = {"WRAP": "WRAP", "CLIP": "CLIP", "OVERFLOW": "OVERFLOW"}

PROFILES_COLUMN_SPECS = {
    "widths": [2, 150, 80, 2, 80, 70, 140, 40, 40, 40, 70, 40, 60, 40, 2, 10, 40, 80, 150, 2, 70],
    "alignments": [
        "L",
        "L",
        "C",
        "L",
        "C",
        "C",
        "L",
        "C",
        "C",
        "C",
        "C",
        "C",
        "C",
        "C",
        "L",
        "L",
        "C",
        "L",
        "L",
        "L",
        "C",
    ],
    "wrap": ["CLIP"] * 21,
}

RUNLIST_COLUMN_SPECS = {
    "widths": [80, 120, 260, 120, 80, 80, 320, 100, 260, 260, 160],
    "alignments": ["C", "L", "L", "L", "C", "C", "L", "C", "L", "L", "C"],
    "wrap": ["CLIP"] * 11,
}

CHECKLIST_COLUMN_SPECS = {
    "widths": [200, 200, 200, 200],
    "alignments": ["L", "L", "L", "L"],
    "wrap": ["CLIP"] * 4,
}


def get_client():
    if not os.path.exists(CREDENTIALS_FILE):
        # GitHub Actions / env-secret fallback
        payload = os.environ.get("DD_CREDENTIALS_JSON", "").strip()
        if payload:
            try:
                # Validate JSON before writing
                json.loads(payload)
                with open(CREDENTIALS_FILE, "w", encoding="utf-8") as f:
                    f.write(payload)
            except Exception:
                raise FileNotFoundError(CREDENTIALS_FILE)
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(CREDENTIALS_FILE)
    if not DD_SHEET_ID:
        raise ValueError("DD_SHEET_ID is empty")

    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    return gspread.authorize(creds)


def get_sheet(sheet_name: str):
    last_exc = None
    for attempt in range(1, 4):
        try:
            client = get_client()
            workbook = client.open_by_key(DD_SHEET_ID)
            try:
                return workbook.worksheet(sheet_name)
            except WorksheetNotFound:
                log_msg(f"⚠️ Sheet '{sheet_name}' not found, using first sheet")
                return workbook.sheet1
        except Exception as exc:
            last_exc = exc
            if attempt == 3:
                raise
            log_msg(f"⚠️ Sheets connect/open failed (attempt {attempt}/3): {str(exc)[:80]}")
            time.sleep(2 ** attempt)
    if last_exc:
        raise last_exc


def get_or_create_sheet(sheet_name: str):
    last_exc = None
    for attempt in range(1, 4):
        try:
            client = get_client()
            workbook = client.open_by_key(DD_SHEET_ID)
            try:
                return workbook.worksheet(sheet_name)
            except WorksheetNotFound:
                log_msg(f"📄 Creating new sheet: {sheet_name}")
                return workbook.add_worksheet(title=sheet_name, rows=1000, cols=26)
        except Exception as exc:
            last_exc = exc
            if attempt == 3:
                raise
            log_msg(f"⚠️ Sheets connect/open failed (attempt {attempt}/3): {str(exc)[:80]}")
            time.sleep(2 ** attempt)
    if last_exc:
        raise last_exc


def index_to_column_letter(index: int) -> str:
    result = ""
    index += 1
    while index > 0:
        index -= 1
        result = chr(ord("A") + (index % 26)) + result
        index //= 26
    return result


def apply_column_styles(sheet, specs):
    max_idx = len(specs["widths"]) - 1
    last_letter = index_to_column_letter(max_idx)
    body_text = {"fontFamily": SHEET_FONT, "fontSize": 9, "bold": False}
    header_text = {"fontFamily": SHEET_FONT, "fontSize": 10, "bold": False}

    try:
        sheet.format(
            f"A1:{last_letter}1",
            {
                "textFormat": header_text,
                "horizontalAlignment": "CENTER",
                "wrapStrategy": "WRAP",
            },
        )
    except Exception as e:
        log_msg(f"⚠️ Header formatting skipped for {sheet.title}: {e}")

    for idx, width in enumerate(specs["widths"]):
        letter = index_to_column_letter(idx)
        align = (
            ALIGN_MAP.get(specs.get("alignments", [])[idx], "LEFT")
            if idx < len(specs.get("alignments", []))
            else "LEFT"
        )
        wrap_strategy = (
            WRAP_MAP.get(specs.get("wrap", [])[idx], "WRAP")
            if idx < len(specs.get("wrap", []))
            else "WRAP"
        )
        try:
            sheet.set_column_width(idx + 1, width)
        except Exception:
            pass
        try:
            sheet.format(
                f"{letter}:{letter}",
                {
                    "textFormat": body_text,
                    "horizontalAlignment": align,
                    "wrapStrategy": wrap_strategy,
                },
            )
        except Exception:
            continue

    try:
        sheet.freeze(rows=1)
    except Exception:
        pass


def apply_sheet_formatting(runlist_sheet, profiles_sheet, checklist_sheet):
    apply_column_styles(profiles_sheet, PROFILES_COLUMN_SPECS)
    apply_column_styles(runlist_sheet, RUNLIST_COLUMN_SPECS)
    if checklist_sheet:
        apply_column_styles(checklist_sheet, CHECKLIST_COLUMN_SPECS)


def retry_gspread_call(action, *args, retries=4, delay=1, **kwargs):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return action(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                raise
            log_msg(f"⚠️ GSheets write failed (attempt {attempt}/{retries}): {str(exc)[:60]}")
            time.sleep(delay)
            delay *= 2
    if last_exc:
        raise last_exc


def insert_row_with_retry(sheet, values, index, **kwargs):
    params = {"value_input_option": "USER_ENTERED"}
    params.update(kwargs)
    return retry_gspread_call(sheet.insert_row, values, index, **params)


def update_cell_with_retry(sheet, row, col, value, **kwargs):
    params = {}
    params.update(kwargs)
    return retry_gspread_call(sheet.update_cell, row, col, value, **params)


def ensure_simple_sheet_headers(sheet, headers: list[str]) -> None:
    try:
        existing = sheet.get_all_values()
    except Exception:
        existing = []
    if not existing:
        insert_row_with_retry(sheet, headers, 1)
        return
    if not existing[0]:
        insert_row_with_retry(sheet, headers, 1)
        return
    if len(existing[0]) < len(headers):
        # Update only the header row, preserve all existing data rows.
        try:
            sheet.update(f"A1", [headers], value_input_option="USER_ENTERED")
        except Exception:
            # Fallback: replace row 1 via insert/delete if update fails
            try:
                sheet.delete_rows(1)
            except Exception:
                pass
            insert_row_with_retry(sheet, headers, 1)
