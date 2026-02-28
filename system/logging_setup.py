from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(log_path: str | None = None, level: int = logging.INFO) -> None:
    path = Path(log_path or "./bot.log")
    path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(path),
            logging.StreamHandler(),
        ],
    )
