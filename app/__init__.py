from flask import Flask

from config import Config
from .extensions import db, login_manager
from .models import User, ROLE_ADMIN


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config.setdefault("SQLALCHEMY_ENGINE_OPTIONS", {})
    app.config["SQLALCHEMY_ENGINE_OPTIONS"].update({
    "pool_pre_ping": True,
    "pool_recycle": 300,
})


    # Init extensions
    db.init_app(app)
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    # Blueprints
    from .auth.routes import auth_bp
    from .admin.routes import admin_bp
    from .main.routes import main_bp
    from .catalog.routes import catalog_bp
    from .purchases.routes import purchases_bp
    from .sales.routes import sales_bp
    from .reports.routes import reports_bp
    from .search.routes import search_bp
    from .saved_searches.routes import saved_searches_bp
    from .admin.cache_routes import admin_cache_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(catalog_bp)
    app.register_blueprint(purchases_bp)
    app.register_blueprint(sales_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(search_bp)
    app.register_blueprint(saved_searches_bp)
    app.register_blueprint(admin_cache_bp)

    # Create tables + bootstrap first admin if needed
    with app.app_context():
        # Ensure models are registered before create_all()
        from . import models  # noqa: F401

        db.create_all()
        _bootstrap_admin_if_needed(app)

    # Simple health endpoint for Koyeb checks
    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


def _bootstrap_admin_if_needed(app: Flask):
    """
    If the DB has no users, create a first admin user from env vars.
    This runs on startup and will only create a user once.
    """
    if User.query.count() > 0:
        return

    email = (app.config.get("BOOTSTRAP_ADMIN_EMAIL") or "").strip().lower()
    password = app.config.get("BOOTSTRAP_ADMIN_PASSWORD") or ""
    name = app.config.get("BOOTSTRAP_ADMIN_NAME") or "Admin"

    if not email or not password:
        # No bootstrap info provided; leave DB empty.
        return

    admin = User(email=email, name=name, role=ROLE_ADMIN, is_active=True)
    admin.set_password(password)
    db.session.add(admin)
    db.session.commit()
