from enum import StrEnum

from sqlalchemy import (
    String, Text, Integer, Float, Date
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import datetime


class Base(DeclarativeBase):
    pass


class ScrapeStatus(StrEnum):
    PENDING = "pending"
    DONE = "done"


class ScraperQueue(Base):
    __tablename__ = "scraper_queue"

    isbn13: Mapped[str] = mapped_column(String, primary_key=True)
    status: Mapped[str] = mapped_column(String, default=ScrapeStatus.PENDING, index=True, nullable=False)


class Book(Base):
    __tablename__ = "books"

    isbn13: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    long_title: Mapped[str] = mapped_column(String, nullable=False)
    authors: Mapped[str] = mapped_column(String, nullable=False)
    publisher: Mapped[str] = mapped_column(String, nullable=False)
    date_published: Mapped[datetime.date] = mapped_column(Date, nullable=True)
    synopsis: Mapped[str] = mapped_column(Text, nullable=True)
    language: Mapped[str] = mapped_column(String, nullable=True)
    subjects: Mapped[str] = mapped_column(String, nullable=True)
    edition: Mapped[str] = mapped_column(String, nullable=True)
    isbn: Mapped[str] = mapped_column(String, nullable=True)
    isbn10: Mapped[str] = mapped_column(String, nullable=True)
    dewey_decimal: Mapped[str] = mapped_column(String, nullable=True)
    cover: Mapped[str] = mapped_column(String, nullable=True)
    binding: Mapped[str] = mapped_column(String, nullable=True)
    dimensions: Mapped[str] = mapped_column(String, nullable=True)
    pages: Mapped[int] = mapped_column(Integer, nullable=True)
    msrp: Mapped[float] = mapped_column(Float, nullable=True)


