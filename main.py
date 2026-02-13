#!/usr/bin/env python3
"""DMX Grabber — точка входа.

Запуск:
    python main.py -i data/input -o output/results.csv
    python main.py --help
"""

import sys
from pathlib import Path

# Добавляем src в путь для прямого запуска
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dmx_grabber.cli import main

if __name__ == "__main__":
    sys.exit(main())
