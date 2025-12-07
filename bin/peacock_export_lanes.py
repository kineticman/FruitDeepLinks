#!/usr/bin/env python3
"""Legacy wrapper.

This script now delegates to fruit_export_lanes.py.
Use `fruit_export_lanes.py` directly going forward.
"""

from fruit_export_lanes import main

if __name__ == "__main__":
    raise SystemExit(main())
