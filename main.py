import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from tqdm import tqdm

from isbndb.api import fetch_books
from isbndb.exceptions import DailyQuotaExceeded, RateLimitExceeded
from isbndb.ingest import parse_books, archive_books
from isbndb.db import Database
from config import DB_URL, ISBNDB_API_KEY


BATCH_SIZE = 1000
MAX_CALLS_PER_SEC = 5
MAX_CALLS_PER_DAY = 200_000
MANUAL_THROTTLE_SEC = 60

DOWNLOADS_FOLDER = Path.home() / "Downloads"
ISBNDB_DUMP_DIR = DOWNLOADS_FOLDER / "isbndb_dump"
ISBNDB_DUMP_DIR.mkdir(exist_ok=True)
STATE_FILE = DOWNLOADS_FOLDER / "isbndb_state.json"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def seconds_until_midnight_utc() -> int:
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int((tomorrow - now).total_seconds())


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"date": datetime.now(timezone.utc).date().isoformat(), "calls": 0}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"date": datetime.now(timezone.utc).date().isoformat(), "calls": 0}


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=4), encoding="utf-8")


async def wait_for_reset() -> None:
    sleep_time = seconds_until_midnight_utc()
    logging.info(f"🕒 Daily quota reached. Sleeping {sleep_time / 3600:.2f}h until reset (00:00 UTC).")
    await asyncio.sleep(sleep_time)
    state = {"date": datetime.now(timezone.utc).date().isoformat(), "calls": 0}
    save_state(state)


async def process_batch(db: Database, client: httpx.AsyncClient, batch: list[str], out_dir: Path) -> int:
    books_raw = await fetch_books(client, batch, ISBNDB_API_KEY)
    books = parse_books(books_raw)

    if not books:
        return 0

    await archive_books(books_raw, out_dir)
    await db.insert_books(books)
    await db.mark_done(batch)
    return len(books)


async def consume_batches(db: Database, out_dir: Path) -> None:
    state = load_state()
    today = datetime.now(timezone.utc).date()

    if state["date"] != today.isoformat():
        state = {"date": today.isoformat(), "calls": 0}

    total_pending = await db.count_pending()

    async with httpx.AsyncClient(http2=True, timeout=60) as client:
        with tqdm(total=total_pending, desc="Processing ISBNs", unit="isbn") as pbar:
            while True:
                if state["calls"] >= MAX_CALLS_PER_DAY:
                    await wait_for_reset()

                batch = await db.fetch_pending(limit=BATCH_SIZE)
                if not batch:
                    logging.info("No more pending ISBNs.")
                    break

                try:
                    num_results = await process_batch(db, client, batch, out_dir)
                except RateLimitExceeded:
                    logging.warning(f"Rate limit exceeded — backing off for {MANUAL_THROTTLE_SEC} seconds...")
                    await asyncio.sleep(MANUAL_THROTTLE_SEC)
                    continue
                except DailyQuotaExceeded:
                    await wait_for_reset()
                    continue

                state["calls"] += 1
                save_state(state)
                pbar.update(num_results)
                await asyncio.sleep(1 / MAX_CALLS_PER_SEC)


async def main() -> None:
    setup_logging()
    logging.info("Starting to dump ISBNdb...")

    db = Database(DB_URL)
    await consume_batches(db, ISBNDB_DUMP_DIR)

    logging.info("All ISBNs scraped. Shutting down gracefully.")


if __name__ == "__main__":
    asyncio.run(main())
