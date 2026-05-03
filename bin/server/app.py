#!/usr/bin/env python3
"""
server/app.py - Flask application factory

Usage:
    from server.app import create_app
    app = create_app()
    app.run(host="0.0.0.0", port=6655)
"""

import sys
from pathlib import Path

from flask import Flask
try:
    from flask_cors import CORS
except ImportError:
    CORS = None


def _find_template_dir() -> Path:
    """Locate the templates directory (Docker vs. local dev)."""
    candidates = [
        Path("/app/templates"),
        Path(__file__).parent.parent.parent / "templates",  # project root
        Path(__file__).parent.parent / "templates",         # bin/templates
    ]
    for p in candidates:
        if p.is_dir():
            return p
    return candidates[0]  # fallback; Flask will raise a useful error


def create_app() -> Flask:
    """Create and configure the Flask application."""
    template_dir = _find_template_dir()
    app = Flask(__name__, template_folder=str(template_dir),
                static_folder=str(template_dir), static_url_path="/static")
    app.config["TEMPLATES_AUTO_RELOAD"] = True

    if CORS is not None:
        CORS(app)

    # Ensure bin/ is on sys.path so local helpers (adb_provider_mapper etc.) import cleanly
    bin_dir = str(Path(__file__).parent.parent)
    if bin_dir not in sys.path:
        sys.path.insert(0, bin_dir)

    # Suppress noisy Werkzeug access logs
    from server.logging_setup import configure_werkzeug
    configure_werkzeug()

    # Register blueprints
    from server.routes.dashboard import bp as dashboard_bp
    from server.routes.api.admin import bp as admin_bp
    from server.routes.api.filters import bp as filters_bp
    from server.routes.api.events import bp as events_bp
    from server.routes.api.lanes import bp as lanes_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(filters_bp)
    app.register_blueprint(events_bp)
    app.register_blueprint(lanes_bp)

    return app
