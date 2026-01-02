import time
import os

from config import BASE_URL
from utils import get_pkt_time, log_msg, append_csv_row
from sheets import get_or_create_sheet, ensure_simple_sheet_headers, insert_row_with_retry


def _find_next_url(driver):
    # Pagination selector wasn't provided; use a conservative heuristic.
    candidates = driver.find_elements(
        "xpath",
        "//a[contains(.,'Next') or contains(.,'NEXT') or .//button[contains(.,'Next') or contains(.,'NEXT')]]",
    )
    for a in candidates:
        try:
            href = (a.get_attribute("href") or "").strip()
            if href and href.startswith("http"):
                return href
        except Exception:
            continue
    return ""


def _run_inbox_like(driver, *, page_url: str, sheet_name: str, section: str, export_filename: str):
    driver.get(page_url)
    time.sleep(3)

    inbox_sheet = get_or_create_sheet(sheet_name)
    headers = [
        "DATETIME",
        "SECTION",
        "ITEM_TYPE",
        "AUTHOR",
        "SNIPPET",
        "TIME",
        "POST_LINK",
        "IMAGE_LINK",
        "THUMBNAIL",
        "CAPTION",
        "TID",
        "OBID",
        "POID",
        "TUID",
        "ORIGIN",
        "REL_KEY",
        "HAS_REPLY_FORM",
        "CSRF_PRESENT",
        "PAGE_URL",
    ]
    ensure_simple_sheet_headers(inbox_sheet, headers)

    def _safe_text(el, selector):
        try:
            return (el.find_element("css selector", selector).text or "").strip()
        except Exception:
            return ""

    def _safe_attr(el, selector, attr):
        try:
            v = el.find_element("css selector", selector).get_attribute(attr)
            return (v or "").strip()
        except Exception:
            return ""

    def _exists(el, selector):
        try:
            el.find_element("css selector", selector)
            return True
        except Exception:
            return False

    export_path = os.path.join("folderExport", export_filename)

    seen_pages = set()
    current_url = page_url

    while True:
        if current_url in seen_pages:
            break
        seen_pages.add(current_url)

        items = driver.find_elements("css selector", 'div.mbl.mtl[style*="border:2px solid"]')
        log_msg(f"📥 {section} items found: {len(items)}")

        now_str = get_pkt_time().strftime("%d-%b-%y %I:%M %p")

        for item in items:
            item_type = ""

            # 1) 1-ON-1
            if _exists(item, 'form[action="/1-on-1/from-single-notif/"]'):
                item_type = "1on1"
            else:
                # 2/3) Reply blocks
                if _exists(item, 'a[href^="/comments/image/"]'):
                    item_type = "image_reply"
                elif _exists(item, 'form[action="/direct-response/send/"]'):
                    item_type = "text_reply"
                else:
                    item_type = "unknown"

            author = ""
            snippet = ""
            rel_time = ""
            post_link = ""
            image_link = ""
            thumbnail = ""
            caption = ""
            tid = ""
            obid = ""
            poid = ""
            tuid = ""
            origin = ""

            csrf_present = _exists(item, 'input[name="csrfmiddlewaretoken"]')
            has_reply_form = _exists(item, 'form[action="/direct-response/send/"]')

            if item_type == "1on1":
                tid = _safe_attr(item, 'button[name="tid"]', 'value')
                caption = _safe_text(item, 'div.cm.sp')
            else:
                # text preview and author
                snippet = _safe_text(item, 'div.cl.lsp.nos span bdi bdi')
                if not snippet:
                    snippet = _safe_text(item, 'div.cl.lsp.nos span bdi')

                author = _safe_text(item, 'div.cl.lsp.nos bdi')
                rel_time = _safe_text(item, 'span.cxs.sp')

                image_link = _safe_attr(item, 'a[href^="/comments/image/"]', 'href')
                thumbnail = _safe_attr(item, 'img[src*="cloudfront"]', 'src')
                caption = _safe_text(item, 'div.cm.sp span.ct')

                # hidden metadata (inside reply form if present)
                obid = _safe_attr(item, 'form[action="/direct-response/send/"] input[name="obid"]', 'value')
                poid = _safe_attr(item, 'form[action="/direct-response/send/"] input[name="poid"]', 'value')
                tuid = _safe_attr(item, 'form[action="/direct-response/send/"] input[name="tuid"]', 'value')
                origin = _safe_attr(item, 'form[action="/direct-response/send/"] input[name="origin"]', 'value')

                # post link
                post_link = _safe_attr(item, 'a[href^="/comments/text/"]', 'href')
                if not post_link:
                    post_link = _safe_attr(item, 'a[href^="/content/"]', 'href')

            row = {
                "DATETIME": now_str,
                "SECTION": section,
                "ITEM_TYPE": item_type,
                "AUTHOR": author,
                "SNIPPET": snippet,
                "TIME": rel_time,
                "POST_LINK": post_link,
                "IMAGE_LINK": image_link,
                "THUMBNAIL": thumbnail,
                "CAPTION": caption,
                "TID": tid,
                "OBID": obid,
                "POID": poid,
                "TUID": tuid,
                "ORIGIN": origin,
                "REL_KEY": f"tid:{tid}" if tid else f"tuid:{tuid}|poid:{poid}|obid:{obid}|origin:{origin}",
                "HAS_REPLY_FORM": "Yes" if has_reply_form else "No",
                "CSRF_PRESENT": "Yes" if csrf_present else "No",
                "PAGE_URL": current_url,
            }

            insert_row_with_retry(inbox_sheet, [row.get(h, "") for h in headers], 2)
            append_csv_row(export_path, headers, row)

        next_url = _find_next_url(driver)
        if not next_url:
            break
        current_url = next_url
        driver.get(current_url)
        time.sleep(2)


def run_inbox_mode(driver):
    _run_inbox_like(
        driver,
        page_url=f"{BASE_URL}/inbox/",
        sheet_name="Inbox",
        section="Replies",
        export_filename="inbox.csv",
    )
