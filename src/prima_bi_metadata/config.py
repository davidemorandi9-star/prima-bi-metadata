import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    BI_API_BASE: str = os.getenv("BI_API_BASE", "https://api.example.com")
    BI_API_KEY: str = os.getenv("BI_API_KEY", "")
    DB_URL: str = os.getenv("DB_URL", "sqlite:///metadata.db")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

settings = Settings()
