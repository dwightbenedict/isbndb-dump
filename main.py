import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from aiolimiter import AsyncLimiter
from tqdm import tqdm

from isbndb.api import fetch_books
from isbndb.ingest import parse_books, archive_books
from isbndb.db import Database
from config import DB_URL, ISBNDB_API_KEY


BATCH_SIZE = 1000
MAX_CALLS_PER_SEC = 10
MAX_CALLS_PER_DAY = 200_000
MAX_CONCURRENT_REQUESTS = 10  # parallel tasks

DOWNLOADS_FOLDER = Path.home() / "Downloads"
ISBNDB_DUMP_DIR = DOWNLOADS_FOLDER / "isbndb_dump"
ISBNDB_DUMP_DIR.mkdir(exist_ok=True)


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

    await archive_books(data, out_dir)
    books = parse_books(data)
    await db.insert_books(books)
    await db.mark_done(batch)
    return True


async def consume_batches(db: Database, limiter: AsyncLimiter, out_dir: Path) -> None:
    batch_count = 0
    total_pending = await db.count_pending()
    processed_count = 0
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with httpx.AsyncClient(http2=True, timeout=60) as client:
        with tqdm(total=total_pending, desc="Processing ISBNs", unit="isbn") as progress:
            tasks = []
            while True:
                batch = await db.fetch_pending(limit=BATCH_SIZE)
                if not batch:
                    break

                if batch_count >= MAX_CALLS_PER_DAY:
                    sleep_time = seconds_until_midnight_utc()
                    logging.info(f"🕒 Daily quota reached. Sleeping {sleep_time / 3600:.2f}h until reset (00:00 UTC).")
                    await asyncio.sleep(sleep_time)
                    batch_count = 0

                await semaphore.acquire()
                async with limiter:
                    task = asyncio.create_task(process_batch(db, client, batch, out_dir))
                    task.add_done_callback(lambda t: semaphore.release())
                    tasks.append(task)

                    batch_count += 1

                    def _update_progress(t: asyncio.Task) -> None:
                        if t.result():
                            progress.update(BATCH_SIZE)

                    task.add_done_callback(_update_progress)

            if tasks:
                await asyncio.gather(*tasks)

    logging.info(f"✅ All {processed_count:,} ISBNs processed successfully.")


async def main() -> None:
    setup_logging()
    logging.info("🚀 Starting ISBNdb consumer service...")

    db = Database(DB_URL)
    limiter = AsyncLimiter(MAX_CALLS_PER_SEC, time_period=1)

    await consume_batches(db, limiter, ISBNDB_DUMP_DIR)
    logging.info("🏁 All ISBNs processed. Shutting down gracefully.")


if __name__ == "__main__":
    asyncio.run(main())
