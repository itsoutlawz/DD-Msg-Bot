import os
import re
import csv
from datetime import datetime, timedelta, timezone


def get_pkt_time() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)


def log_msg(m: str) -> None:
    print(f"[{get_pkt_time().strftime('%H:%M:%S')}] {m}")


def clean_text(v: str) -> str:
    if not v:
        return ""
    v = str(v).strip().replace("\xa0", " ")
    bad = {
        "No city",
        "Not set",
        "[No Posts]",
        "N/A",
        "no city",
        "not set",
        "[no posts]",
        "n/a",
        "[No Post URL]",
        "[Error]",
        "none",
        "null",
        "no age",
        "no set",
    }
    return "" if v in bad else re.sub(r"\s+", " ", v)


def convert_relative_date_to_absolute(text: str) -> str:
    if not text:
        return ""
    t = text.lower().strip()
    t = (
        t.replace("mins", "minutes")
        .replace("min", "minute")
        .replace("secs", "seconds")
        .replace("sec", "second")
        .replace("hrs", "hours")
        .replace("hr", "hour")
    )
    if any(keyword in t for keyword in {"just now", "abhi"}):
        return get_pkt_time().strftime("%d-%b-%y")
    m = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", t)
    if not m:
        return text
    amt = int(m.group(1))
    unit = m.group(2)
    s_map = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
        "week": 604800,
        "month": 2592000,
        "year": 31536000,
    }
    if unit in s_map:
        dt = get_pkt_time() - timedelta(seconds=amt * s_map[unit])
        return dt.strftime("%d-%b-%y")
    return text


def extract_nickname_from_user_url(user_url: str) -> str:
    if not user_url:
        return ""
    u = user_url.strip()
    m = re.search(r"/users/([^/]+)/?", u)
    if m:
        return m.group(1)
    return ""


def apply_message_template(
    template: str,
    *,
    name: str = "",
    nickname: str = "",
    city: str = "",
    posts: str = "",
    followers: str = "",
) -> str:
    if not template:
        return ""
    rendered = str(template)
    replacements = {
        "{name}": name or "",
        "{{name}}": name or "",
        "{nick}": nickname or "",
        "{{nick}}": nickname or "",
        "{city}": city or "",
        "{{city}}": city or "",
        "{posts}": posts or "",
        "{{posts}}": posts or "",
        "{followers}": followers or "",
        "{{followers}}": followers or "",
    }
    for k, v in replacements.items():
        rendered = rendered.replace(k, v)
        rendered = rendered.replace(k.upper(), v)
        rendered = rendered.replace(k.title(), v)
    return rendered


def append_csv_row(file_path: str, fieldnames: list[str], row: dict) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_exists = os.path.exists(file_path)
    needs_header = (not file_exists) or os.path.getsize(file_path) == 0

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if needs_header:
            w.writeheader()
        w.writerow(row)
