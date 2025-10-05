import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")
ISBNDB_API_KEY = os.getenv("ISBNDB_API_KEY")