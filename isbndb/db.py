from typing import Sequence

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from isbndb.schema import Base, Book, ScraperQueue, ScrapeStatus


class Database:
    def __init__(self, db_url: str, echo: bool = False) -> None:
        self.engine = create_async_engine(db_url, echo=echo, future=True)
        self._session_factory = async_sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False,
        )

    async def create_tables(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def count_pending(self) -> int:
        async with self._session_factory() as session:
            query = select(func.count()).where(ScraperQueue.status == ScrapeStatus.PENDING)
            result = await session.scalar(query)
            return int(result or 0)

    async def fetch_pending(self, limit: int = 1000) -> list[str]:
        async with self._session_factory() as session:
            async with session.begin():
                query = (
                    select(ScraperQueue.isbn13)
                    .where(ScraperQueue.status == ScrapeStatus.PENDING)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
                result = await session.scalars(query)
                batch = list(result)

                if batch:
                    await session.execute(
                        update(ScraperQueue)
                        .where(ScraperQueue.isbn13.in_(batch))
                        .values(status=ScrapeStatus.PROCESSING)
                    )

            return batch

    async def mark_done(self, isbns: list[str]) -> None:
        if not isbns:
            return

        async with self._session_factory() as session:
            await session.execute(
                update(ScraperQueue)
                .where(ScraperQueue.isbn13.in_(isbns))
                .values(status=ScrapeStatus.DONE)
            )
            await session.commit()

    async def insert_books(self, books: Sequence[Book]) -> None:
        if not books:
            return

        async with self._session_factory() as session:
            async with session.begin():
                session.add_all(books)
