import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from tqdm import tqdm

from isbndb.api import fetch_books
from isbndb.ingest import parse_books, archive_books
from isbndb.db import Database
from config import DB_URL, ISBNDB_API_KEY


BATCH_SIZE = 1000
MAX_CALLS_PER_SEC = 10
MAX_CALLS_PER_DAY = 200_000

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


async def process_batch(
        db: Database, client: httpx.AsyncClient, batch: list[str], out_dir: Path
) -> bool:
    try:
        data: dict[str, Any] = await fetch_books(client, batch, ISBNDB_API_KEY)
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logging.warning(f"Batch {batch[0]}–{batch[-1]} failed: {e}")
        return False
    except Exception as e:
        logging.exception(f"Unexpected error on batch {batch[0]}–{batch[-1]}: {e}")
        return False

    books = parse_books(data)
    if not books:
        return False

    await archive_books(data, out_dir)
    await db.insert_books(books)
    await db.mark_done(batch)
    return True


async def consume_batches(db: Database, out_dir: Path) -> None:
    state = load_state()
    today = datetime.now(timezone.utc).date()

    if state["date"] != today.isoformat():
        state = {"date": today.isoformat(), "calls": 0}

    total_pending = await db.count_pending()

    async with httpx.AsyncClient(http2=True, timeout=60) as client:
        with tqdm(total=total_pending, desc="Processing ISBNs", unit="isbn") as progress:
            while True:
                if state["calls"] >= MAX_CALLS_PER_DAY:
                    sleep_time = seconds_until_midnight_utc()
                    logging.info(f"🕒 Daily quota reached. Sleeping {sleep_time / 3600:.2f}h until reset (00:00 UTC).")
                    await asyncio.sleep(sleep_time)
                    state = {"date": datetime.now(timezone.utc).date().isoformat(), "calls": 0}
                    save_state(state)

                batch = await db.fetch_pending(limit=BATCH_SIZE)
                if not batch:
                    logging.info("✅ No more pending ISBNs.")
                    break

                success = await process_batch(db, client, batch, out_dir)
                state["calls"] += 1
                save_state(state)

                if success:
                    progress.update(len(batch))

                await asyncio.sleep(1 / MAX_CALLS_PER_SEC)


async def main() -> None:
    setup_logging()
    logging.info("🚀 Starting ISBNdb sequential consumer service...")

    db = Database(DB_URL)
    await consume_batches(db, ISBNDB_DUMP_DIR)

    logging.info("🏁 All ISBNs processed. Shutting down gracefully.")


if __name__ == "__main__":
    asyncio.run(main())
