from datetime import datetime
from typing import Any
from pathlib import Path
import gzip
import asyncio

import orjson

from isbndb.schema import Book


def parse_books(data: dict[str, Any]) -> list[Book]:
    books: list[Book] = []

    for book in data.get("data", []):
        dim = book.get("dimensions")
        dimensions = ", ".join(dim) if isinstance(dim, list) else str(dim or "")

        books.append(
            Book(
                isbn13=book.get("isbn13") or "",
                title=book.get("title") or "",
                long_title=book.get("title_long") or book.get("title") or "",
                authors=", ".join(book.get("authors") or []),
                publisher=book.get("publisher") or "",
                date_published=book.get("date_published") or "",
                synopsis=book.get("synopsis") or "",
                language=book.get("language") or "",
                subjects=", ".join(book.get("subjects") or []),
                edition=str(book["edition"]) if book.get("edition") else "",
                isbn=book.get("isbn") or "",
                isbn10=book.get("isbn10") or "",
                dewey_decimal=", ".join(book.get("dewey_decimal") or []),
                cover=book.get("image_original") or "",
                binding=book.get("binding") or "",
                dimensions=dimensions,
                pages=(
                    int(book["pages"])
                    if isinstance(book.get("pages"), (int, str)) and str(book["pages"]).isdigit()
                    else 0
                ),
                msrp=(
                    float(book["msrp"])
                    if isinstance(book.get("msrp"), (int, float, str)) and str(book["msrp"]).replace(".", "", 1).isdigit()
                    else 0.0
                ),
            )
        )

    return books


def _append_books(data: dict[str, Any], out_file: Path) -> None:
    books = data.get("data", [])
    with gzip.open(out_file, "ab") as f:
        for book in books:
            f.write(orjson.dumps(book) + b"\n")


async def archive_books(data: dict[str, Any], out_dir: Path) -> None:
    date_str = datetime.now().strftime("%Y%m%d")
    out_file = out_dir / f"{date_str}_books.jsonl.gz"
    await asyncio.to_thread(_append_books, data, out_file)