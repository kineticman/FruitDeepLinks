#!/usr/bin/env python3
"""Legacy wrapper.

This script now delegates to fruit_build_lanes.py.
Use `fruit_build_lanes.py` directly going forward.
"""

from fruit_build_lanes import main

if __name__ == "__main__":
    raise SystemExit(main())
