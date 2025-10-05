from datetime import datetime, date
from typing import Any
from pathlib import Path
import gzip
import asyncio

import orjson

from isbndb.schema import Book


def parse_books(data: dict[str, Any]) -> list[Book]:
    return [
        Book(
            title=book["title"],
            long_title=book["title_long"],
            authors=", ".join(book["authors"]),
            publisher=book["publisher"],
            date_published=date.fromisoformat(book["date_published"]),
            synopsis=book["synopsis"],
            language=book["language"],
            subjects=", ".join(book["subjects"]),
            edition=book["edition"],
            isbn=book["isbn"],
            isbn10=book["isbn10"],
            isbn13=book["isbn13"],
            dewey_decimal=", ".join(book["dewey_decimal"]),
            cover=book["image_original"],
            binding=book["binding"],
            dimensions=book["dimensions"],
            pages=book["pages"],
            msrp=book["msrp"],
        )
        for book in data["data"]
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