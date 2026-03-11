from dataclasses import dataclass
from pathlib import Path

DEFAULT_BASE_URL = "https://oatcakefanzine.proboards.com"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_DELAY = 1.5
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF = 1.5
DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_CHECKPOINT_FILE = DEFAULT_OUTPUT_DIR / "checkpoints.json"
DEFAULT_DB_PATH = DEFAULT_OUTPUT_DIR / "archive.db"
DEFAULT_EXPORT_DIR = Path("exports")
DEFAULT_IDLE_MIN_SECONDS = 600
DEFAULT_IDLE_MAX_SECONDS = 21600
DEFAULT_JITTER_RATIO = 0.2
DEFAULT_MAX_BACKOFF_SECONDS = 1800
DEFAULT_MAX_CONSECUTIVE_FAILURES = 8
DEFAULT_RETRY_AFTER_CAP_SECONDS = 1800
DEFAULT_LOCK_FILE = DEFAULT_OUTPUT_DIR / "worker.lock"
DEFAULT_CAUGHT_UP_CYCLES = 3


@dataclass(frozen=True)
class ScraperConfig:
    base_url: str = DEFAULT_BASE_URL
    delay: float = DEFAULT_DELAY
    max_retries: int = DEFAULT_MAX_RETRIES
    backoff_factor: float = DEFAULT_BACKOFF
    user_agent: str = DEFAULT_USER_AGENT
    output_dir: Path = DEFAULT_OUTPUT_DIR
    checkpoint_file: Path = DEFAULT_CHECKPOINT_FILE
    db_path: Path = DEFAULT_DB_PATH
    export_dir: Path = DEFAULT_EXPORT_DIR
    jitter_ratio: float = DEFAULT_JITTER_RATIO
    max_backoff_seconds: int = DEFAULT_MAX_BACKOFF_SECONDS
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES
    retry_after_cap_seconds: int = DEFAULT_RETRY_AFTER_CAP_SECONDS
    idle_min_seconds: int = DEFAULT_IDLE_MIN_SECONDS
    idle_max_seconds: int = DEFAULT_IDLE_MAX_SECONDS
    lock_file: Path = DEFAULT_LOCK_FILE
    caught_up_cycles: int = DEFAULT_CAUGHT_UP_CYCLES
