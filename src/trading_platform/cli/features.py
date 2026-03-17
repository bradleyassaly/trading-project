from __future__ import annotations

import sys

from trading_platform.cli.__main__ import main

if __name__ == "__main__":
    sys.argv.insert(1, "features")
    main()