from flask import Blueprint, jsonify
from utils.auth import require_auth
from clients.postgres_client import PostgresClient

from utils.logger import get_logger
logger = get_logger(__name__)


fetch_bp = Blueprint("fetch", __name__, url_prefix="/api/fetch")
           


@fetch_bp.route("/orgunits", methods=["GET"])
@require_auth
def fetch_orgunits():
    """
    Retourne la liste des objets à synchroniser.
    """
    try:
        pg = PostgresClient() 
        result = pg.list_orgunits()
        return jsonify(result), 200
    except Exception as e:
        logger.exception("Failed to fetch data")
        return jsonify({"error": "Failed to fetch data"}), 500