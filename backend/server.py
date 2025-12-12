# server.py
import os
import time
import urllib3
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from utils.config import config
from utils.models import User, db
from utils.auth import require_auth
from routes.run_sql_routes import run_sql_bp
from routes.schema_routes import schema_bp
from routes.query_routes import query_bp
from routes.user_routes import user_bp
from routes.auth_routes import auth_bp
from routes.sync_routes import sync_bp
from routes.fetch_routes import fetch_bp
from utils.scheduler_app import SchedulerApp
from utils.build_views import build_materialize_view
from make_arrimate import Dhis2ArrimateMaker
from utils.dates_utils import build_dhis2_period_list
from utils.logger import get_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# App Factory (robuste, production-ready)
# ---------------------------------------------------------------------------
def create_app(init_scheduler: bool = False):
    app = Flask(__name__, static_folder="frontend_build")

    # SQLAlchemy config
    app.config.update(
        SQLALCHEMY_DATABASE_URI=config.SQLALCHEMY_DATABASE_URI,
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        JSON_SORT_KEYS=False,
        SCHEDULER_API_ENABLED=config.SCHEDULER_API_ENABLED,
    )

    # CORS sécurisé
    frontend_origin = os.getenv("FRONTEND_ORIGIN", "*")
    CORS(app, resources={r"/api/*": {"origins": frontend_origin}})

    # Init DB
    db.init_app(app)

    # EXÉCUTE DB.CREATE_ALL + SCHEDULER *UNE SEULE FOIS*
    with app.app_context():
        db.create_all()
        User.create_default_admin()  # Crée automatiquement l'admin si nécessaire
        logger.info("Database tables ensured (use Alembic in production).")

        # Scheduler optionnel
        if init_scheduler is True:
            # SECURITY: prevents scheduler restart on reload/debug
            if not hasattr(app, "scheduler_started"):
                app.scheduler = SchedulerApp(app)
                app.scheduler_started = True
                logger.info("Scheduler initialized successfully.")
            else:
                logger.warning("Scheduler already initialized — skipped duplicate initialization.")

    # Register blueprints
    app.register_blueprint(run_sql_bp)
    app.register_blueprint(schema_bp)
    app.register_blueprint(query_bp)
    app.register_blueprint(user_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(sync_bp)
    app.register_blueprint(fetch_bp)

    # ---------------------------
    # API Routes
    # ---------------------------

    @app.get("/api/health")
    def health():
        return jsonify({"status": "ok"}), 200

    @app.post("/api/build-matview")
    @require_auth
    def build_matview():
        result, success = build_materialize_view()
        if success:
            return jsonify({"matview": 1}), 200
        return jsonify(result), 500

    @app.post("/api/arrimate-indicators")
    @require_auth
    def arrimage_with_dhis2():
        payload = request.get_json(silent=True) or {}
        start_date = payload.get("start_date")
        end_date =  payload.get("end_date")
        orgunits =  payload.get("orgunits") or []

        periods = build_dhis2_period_list(start_date, end_date)
        orgunit_ids = [orgunits] if orgunits and isinstance(orgunits,str) else orgunits

        arr = Dhis2ArrimateMaker(send_to_dhis2 = True, save_to_local_file = False)
        outputs = arr.start_indicators_arrimage_with_dhis2(periods,orgunit_ids)


        result = { "success":0, "error":0 }
        for output in outputs:
            if output["status"] is True:
                result["success"] += output["size"]
            else:
                result["error"] += output["size"]

        if result["success"] > 0 or result["success"] == 0 and result["error"] == 0:
            status = 201 if result["error"] > 0 else 200
            return jsonify({"status":status, "success": f'SUCCESS de {result["success"]}',"error": f'ECHEC de {result["error"]}'}), 200
        
        return jsonify({"status":500, "error": "Erreur lors de l'arrimage","status": "ERROR"}), 500
        

    # ---------------------------
    # FRONTEND (React/Vue/Angular SPA)
    # ---------------------------
    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def serve_frontend(path):
        if path.startswith("api"):
            return jsonify({"error": "not found"}), 404

        file_path = os.path.join(app.static_folder, path)
        if path and os.path.exists(file_path):
            return send_from_directory(app.static_folder, path)

        return send_from_directory(app.static_folder, "index.html")

    # ---------------------------
    # ERROR HANDLING GLOBAL
    # ---------------------------
    @app.errorhandler(400)
    def bad_request(e):
        return jsonify({"msg": "bad request"}), 400

    @app.errorhandler(401)
    def unauthorized(e):
        return jsonify({"msg": "unauthorized"}), 401

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"msg": "not found"}), 404

    @app.errorhandler(405)
    def method_not_allowed(e):
        logger.warning(f"Method not allowed")
        return jsonify({"msg": "method not allowed"}), 405

    @app.errorhandler(500)
    def server_error(e):
        logger.exception("Server error")
        return jsonify({"msg": "internal server error"}), 500

    @app.errorhandler(Exception)
    def handle_exception(e):
        logger.exception("Unhandled global exception")
        return jsonify({"error": str(e)}), 500

    # Cleanup SQLAlchemy
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db.session.remove()

    return app


# ---------------------------------------------------------------------------
# EntryPoint (DEV ONLY — Gunicorn is required for production)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        app = create_app(init_scheduler=False)

        host = config.API_HOST
        port = int(config.API_PORT)
        debug = bool(int(os.getenv("DEBUG", "0")))

        logger.info(f"Starting app on {host}:{port} debug={debug}")

        # IMPORTANT : Disable reloader to avoid multiple schedulers
        app.run(host=host,port=port,debug=debug,use_reloader=False)

    except Exception as e:
        logger.critical("Fatal error on app startup", exc_info=True)
        raise ValueError(f"❌ PostgreSQL connection error: {e}") from e
