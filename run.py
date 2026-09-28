import os
import sys
import asyncio

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from app import main

if __name__ == "__main__":
    asyncio.run(main())
