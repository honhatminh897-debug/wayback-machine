import os
import io
import re
import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlsplit
import json

import aiohttp
import pandas as pd
from dateutil import tz
from telegram import Update, constants
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
TIMEZONE = tz.gettz("Asia/Bangkok")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "10"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

CDX_BASE = "https://web.archive.org/cdx/search/cdx?output=json&fl=timestamp,original&collapse=digest&sort=asc&from=1996"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("wayback-age-bot")

def _normalize_domain(raw: str) -> str:
    s = raw.strip()
    if not s:
        return ""
    if re.match(r"^[a-zA-Z]+://", s):
        s = urlsplit(s).netloc or s
    s = s.split("/")[0]
    s = s.split(":")[0]
    if s.startswith("www."):
        s = s[4:]
    return s.lower().strip(".")

def _years_between(d1: datetime, d2: datetime) -> float:
    return (d2 - d1).days / 365.2425

async def _http_get_json_or_text(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
        if resp.status in (429, 502, 503, 504):
            raise aiohttp.ClientResponseError(
                request_info=resp.request_info, history=resp.history,
                status=resp.status, message=f"Transient HTTP {resp.status}"
            )
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "json" in ct.lower():
            try:
                return await resp.json(content_type=None)
            except Exception:
                pass
        # fallback text
        return await resp.text()

def _parse_num_pages(payload) -> int | None:
    """
    Cố gắng trích số trang (numPages) từ phản hồi showNumPages=true.
    Trả về None nếu không trích được.
    """
    # Kiểu 1: JSON object có key numPages
    if isinstance(payload, dict) and "numPages" in payload:
        try:
            return int(payload["numPages"])
        except Exception:
            return None
    # Kiểu 2: JSON list có phần tử đầu chứa dict/tuple string "numPages"
    if isinstance(payload, list) and payload and isinstance(payload[0], dict) and "numPages" in payload[0]:
        try:
            return int(payload[0]["numPages"])
        except Exception:
            return None
    # Kiểu 3: trả về text "numPages: N"
    if isinstance(payload, str):
        import re as _re
        m = _re.search(r'numPages\s*:\s*(\d+)', payload)
        if m:
            return int(m.group(1))
    return None

async def _cdx_attempt(session: aiohttp.ClientSession, url: str):
    try:
        return await _http_get_json_or_text(session, url)
    except Exception as e:
        logger.warning("CDX request failed: %r", e)
        return None

async def fetch_counts(session: aiohttp.ClientSession, domain: str) -> tuple[int | None, int | None]:
    """
    Đếm số snapshot: (total, only200). Dùng showNumPages=true & pageSize=1.
    """
    base = "https://web.archive.org/cdx/search/cdx?showNumPages=true&pageSize=1"
    q = f"&matchType=prefix&url={domain}/*"
    # total
    total_payload = await _cdx_attempt(session, base + q)
    total = _parse_num_pages(total_payload)
    # only 200
    ok_payload = await _cdx_attempt(session, base + q + "&filter=statuscode:200")
    ok = _parse_num_pages(ok_payload)
    return total, ok

async def fetch_earliest_snapshot(session: aiohttp.ClientSession, domain: str, semaphore: asyncio.Semaphore) -> dict:
    """Lấy snapshot sớm nhất + đếm số snapshot (tổng & 200)."""
    result = {"domain": domain, "earliest_snapshot": None, "age_years": None,
              "snapshots_total": None, "snapshots_200": None, "error": None}

    attempts = [
        f"{CDX_BASE}&matchType=host&url={domain}/*&filter=statuscode:200&limit=1",
        f"{CDX_BASE}&matchType=prefix&url={domain}/*&filter=statuscode:200&limit=1",
        f"{CDX_BASE}&matchType=prefix&url={domain}/*&limit=1",
    ]

    backoff = 1
    for _ in range(4):
        async with semaphore:
            for url in attempts:
                payload = await _cdx_attempt(session, url)
                if isinstance(payload, list) and len(payload) >= 2 and len(payload[1]) >= 1:
                    ts = payload[1][0]
                    if ts:
                        try:
                            dt = datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                            result["earliest_snapshot"] = dt.astimezone(TIMEZONE).strftime("%Y-%m-%d")
                            result["age_years"] = round(_years_between(dt, datetime.now(timezone.utc)), 2)
                        except Exception as e:
                            result["error"] = f"parse_ts: {repr(e)}"
                        break
        if result["earliest_snapshot"] is not None:
            break
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 20)

    # Đếm số snapshot (không khóa semaphore để song song nhiều domain)
    try:
        total, ok = await fetch_counts(session, domain)
        result["snapshots_total"] = total
        result["snapshots_200"] = ok
    except Exception as e:
        logger.warning("count error for %s: %r", domain, e)
        if result["error"]:
            result["error"] += f"; count: {repr(e)}"
        else:
            result["error"] = f"count: {repr(e)}"

    if result["earliest_snapshot"] is None and result["error"] is None:
        result["error"] = "cdx_failed_or_no_data"

    return result

async def process_file(file_bytes: bytes, filename: str) -> bytes:
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    if ext in ("xlsx", "xls"):
        df = pd.read_excel(io.BytesIO(file_bytes), header=None)
    elif ext in ("csv", "txt"):
        df = pd.read_csv(io.BytesIO(file_bytes), header=None)
    else:
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), header=None)
        except Exception:
            df = pd.read_csv(io.BytesIO(file_bytes), header=None)

    domains = []
    for val in df.iloc[:, 0].astype(str).tolist():
        n = _normalize_domain(val)
        if n:
            domains.append(n)
    domains = list(dict.fromkeys(domains))

    if not domains:
        out = pd.DataFrame([["(Không tìm thấy domain hợp lệ)"]], columns=["message"])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            out.to_excel(writer, index=False, sheet_name="result")
        buf.seek(0)
        return buf.read()

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_earliest_snapshot(session, d, semaphore) for d in domains]
        results = await asyncio.gather(*tasks)

    cols = ["domain", "earliest_snapshot", "age_years", "snapshots_total", "snapshots_200", "error"]
    result_df = pd.DataFrame(results)[cols]
    result_df.sort_values(by=["age_years"], ascending=[False], inplace=True, na_position="last")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        result_df.to_excel(writer, index=False, sheet_name="wayback_age")
        summary = pd.DataFrame({
            "total_domains": [len(domains)],
            "found_snapshots": [result_df["earliest_snapshot"].notna().sum()],
            "no_snapshot": [result_df["earliest_snapshot"].isna().sum()],
            "avg_snapshots_total": [result_df["snapshots_total"].dropna().mean() if result_df["snapshots_total"].notna().any() else None],
            "avg_snapshots_200": [result_df["snapshots_200"].dropna().mean() if result_df["snapshots_200"].notna().any() else None],
            "generated_at": [datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")]
        })
        summary.to_excel(writer, index=False, sheet_name="summary")
    buf.seek(0)
    return buf.read()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Chào bạn! 👋\\n\\n"
        "Gửi file Excel/CSV gồm *một cột* chứa danh sách domain.\\n"
        "Bot sẽ tìm *snapshot sớm nhất*, tính tuổi, \\n"
        "và đếm *tổng số snapshot* & *snapshot HTTP 200*.\\n\\n"
        "Hỗ trợ: .xlsx, .xls, .csv, .txt"
    )
    await update.message.reply_text(text, parse_mode=constants.ParseMode.MARKDOWN)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "❓ Cách dùng:\\n"
        "• Một cột domain/URL\\n"
        "• Gửi file (≤20MB)\\n"
        "• Nhận lại Excel có cột `snapshots_total`, `snapshots_200`.",
        parse_mode=constants.ParseMode.MARKDOWN
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    if not doc:
        return

    filename = doc.file_name or "input.xlsx"
    await update.message.reply_text("📥 Đang tải file...")
    tg_file = await context.bot.get_file(doc.file_id)
    file_bytes = await tg_file.download_as_bytearray()

    await update.message.reply_text("🧮 Đang xử lý & gọi Wayback Machine...")
    try:
        result_bytes = await process_file(file_bytes, filename)
    except Exception as e:
        logger.exception("Process error")
        await update.message.reply_text(f"❌ Lỗi xử lý file: {repr(e)}")
        return

    out_name = f"wayback_age_{datetime.now(TIMEZONE).strftime('%Y%m%d_%H%M%S')}.xlsx"
    await update.message.reply_document(
        document=result_bytes,
        filename=out_name,
        caption="✅ Hoàn tất! Có kèm đếm snapshot (tổng & HTTP 200)."
    )

def main() -> None:
    token = BOT_TOKEN
    if not token:
        raise RuntimeError("Missing BOT_TOKEN env var")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
