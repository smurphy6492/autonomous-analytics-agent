"""Configuration and settings loaded from environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings loaded from environment."""

    anthropic_api_key: str
    data_dir: Path
    output_dir: Path
    model: str
    cache_dir: Path

    def __init__(self) -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY is not set. "
                "Copy .env.example to .env and add your key."
            )
        self.anthropic_api_key = api_key
        self.data_dir = Path(os.getenv("DATA_DIR", "data/raw/olist"))
        self.output_dir = Path(os.getenv("OUTPUT_DIR", "output"))
        self.model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
        self.cache_dir = Path(os.getenv("CACHE_DIR", ".cache"))


def get_settings() -> Settings:
    """Return a Settings instance (raises if API key is missing)."""
    return Settings()
