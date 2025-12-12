from flask import Blueprint, request, jsonify
from utils.auth import require_auth
from routes.sync_routes_utils import sync_orgunits, sync_dataelements, sync_teis_enrollments_events_attributes


sync_bp = Blueprint("sync", __name__, url_prefix="/api/sync")


@sync_bp.post("/orgunits")
@require_auth
def sync_orgunits_query():
    result, status = sync_orgunits()
    return jsonify(result), status
    

@sync_bp.post("/dataElements")
@require_auth
def sync_dataelements_query():
    result, status = sync_dataelements()
    return jsonify(result), status


@sync_bp.post("/teis_enrollments_events_attributes")
@require_auth
def sync_teis_enrollments_events_attributes_query():
    try:
        payload = request.get_json(silent=True) or {}
        orgunit_id = payload.get("orgunit_id")
        doTei =  True if payload.get("teis") == True else False
        doEnroll =  True if payload.get("enrollments") == True else False
        doAttribute =  True if payload.get("attributes") == True else False
        doEvent =  True if payload.get("events") == True else False

        result, status = sync_teis_enrollments_events_attributes(orgunit_id, doTei, doEnroll, doAttribute, doEvent)
        return jsonify(result), status
    
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500

