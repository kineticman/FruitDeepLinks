# bin/appletv_to_peacock.py
#!/usr/bin/env python3
"""Legacy wrapper.

This script now delegates to fruit_import_appletv.py.
Use `fruit_import_appletv.py` directly going forward.
"""

from fruit_import_appletv import main

if __name__ == "__main__":
    raise SystemExit(main())
