from datetime import datetime
from typing import Any
from pathlib import Path
import gzip
import asyncio

import orjson

from isbndb.schema import Book
from isbndb.utils import sanitize_str, sanitize_int, sanitize_float


def parse_books(data: dict[str, Any]) -> list[Book]:
    return [
        Book(
            isbn13=sanitize_str(book.get("isbn13")),
            title=sanitize_str(book.get("title")),
            long_title=sanitize_str(book.get("title_long")),
            authors=sanitize_str(book.get("authors")),
            publisher=sanitize_str(book.get("publisher")),
            date_published=sanitize_str(book.get("date_published")),
            synopsis=sanitize_str(book.get("synopsis")),
            language=sanitize_str(book.get("language")),
            subjects=sanitize_str(book.get("subjects")),
            edition=sanitize_str(book.get("edition")),
            isbn=sanitize_str(book.get("isbn")),
            isbn10=sanitize_str(book.get("isbn10")),
            dewey_decimal=sanitize_str(book.get("dewey_decimal")),
            cover=sanitize_str(book.get("image_original")),
            binding=sanitize_str(book.get("binding")),
            dimensions=sanitize_str(book.get("dimensions")),
            pages=sanitize_int(book.get("pages")),
            msrp=sanitize_float(book.get("msrp")),
        )
        for book in data.get("data", [])
    ]


def _append_books(data: dict[str, Any], out_file: Path) -> None:
    books = data.get("data", [])
    with gzip.open(out_file, "ab") as f:
        for book in books:
            f.write(orjson.dumps(book) + b"\n")


async def archive_books(data: dict[str, Any], out_dir: Path) -> None:
    date_str = datetime.now().strftime("%Y%m%d")
    out_file = out_dir / f"{date_str}_books.jsonl.gz"
    await asyncio.to_thread(_append_books, data, out_file)