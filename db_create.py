import asyncio

from isbndb.db import Database
from config import DB_URL


async def main():
    db = Database(DB_URL)
    await db.create_tables()


if __name__ == "__main__":
    asyncio.run(main())
