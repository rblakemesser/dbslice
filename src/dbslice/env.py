#!/usr/bin/env python3
import os
from typing import Tuple

def load_dotenv(path: str) -> Tuple[int, str]:
    """Simple .env loader: KEY=VALUE lines → os.environ. Returns (count, path)."""
    count = 0
    try:
        with open(path, 'r') as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith('#'):
                    continue
                if '#' in line:
                    before, _after = line.split('#', 1)
                    line = before.strip()
                if '=' not in line:
                    continue
                key, val = line.split('=', 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key:
                    os.environ[key] = val
                    count += 1
    except FileNotFoundError:
        return 0, path
    return count, path
