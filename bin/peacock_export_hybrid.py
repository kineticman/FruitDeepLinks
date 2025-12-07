# bin/peacock_export_hybrid.py
#!/usr/bin/env python3
"""Legacy wrapper.

This script now delegates to fruit_export_hybrid.py.
Use `fruit_export_hybrid.py` directly going forward.
"""

from fruit_export_hybrid import main

if __name__ == "__main__":
    raise SystemExit(main())
