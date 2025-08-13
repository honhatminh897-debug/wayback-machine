import os
import io
import re
import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlsplit

import aiohttp
import pandas as pd
from dateutil import tz
from telegram import Update, constants
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
TIMEZONE = tz.gettz("Asia/Bangkok")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "10"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
CDX_URL = ("https://web.archive.org/cdx/search/cdx"
           "?url={query}&output=json&fl=timestamp,original"
           "&filter=statuscode:200&limit=1&from=1996"
           "&matchType=host&collapse=digest&sort=asc")

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

async def fetch_earliest_snapshot(session: aiohttp.ClientSession, domain: str, semaphore: asyncio.Semaphore) -> dict:
    url = CDX_URL.format(query=domain)
    backoff = 1
    for _ in range(5):
        async with semaphore:
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 20)
                        continue
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                    ts = None
                    if isinstance(data, list) and len(data) >= 2 and len(data[1]) >= 1:
                        ts = data[1][0]
                    result = {"domain": domain, "earliest_snapshot": None, "age_years": None}
                    if ts:
                        try:
                            dt = datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                            result["earliest_snapshot"] = dt.astimezone(TIMEZONE).strftime("%Y-%m-%d")
                            result["age_years"] = round(_years_between(dt, datetime.now(timezone.utc)), 2)
                        except Exception:
                            pass
                    return result
            except Exception as e:
                logger.warning(f"CDX error for {domain}: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 20)
    return {"domain": domain, "earliest_snapshot": None, "age_years": None}

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

    domains = list(dict.fromkeys(_normalize_domain(d) for d in df.iloc[:, 0].astype(str) if _normalize_domain(d)))
    if not domains:
        out = pd.DataFrame([["(Không tìm thấy domain hợp lệ)"]], columns=["message"])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            out.to_excel(writer, index=False)
        buf.seek(0)
        return buf.read()

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[fetch_earliest_snapshot(session, d, semaphore) for d in domains])

    result_df = pd.DataFrame(results)
    result_df.sort_values(by=["age_years"], ascending=False, inplace=True, na_position="last")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        result_df.to_excel(writer, index=False, sheet_name="wayback_age")
    buf.seek(0)
    return buf.read()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Gửi file Excel/CSV có một cột domain, bot sẽ trả về tuổi domain dựa vào Wayback Machine."
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    if not doc:
        return
    filename = doc.file_name or "input.xlsx"
    tg_file = await context.bot.get_file(doc.file_id)
    file_bytes = await tg_file.download_as_bytearray()
    await update.message.reply_text("Đang xử lý...")
    try:
        result_bytes = await process_file(file_bytes, filename)
        await update.message.reply_document(
            document=result_bytes,
            filename=f"wayback_age_{datetime.now(TIMEZONE).strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
    except Exception as e:
        await update.message.reply_text(f"Lỗi: {e}")

def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Thiếu BOT_TOKEN")
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.run_polling()

if __name__ == "__main__":
    main()
