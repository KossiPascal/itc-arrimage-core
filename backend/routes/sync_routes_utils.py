from datetime import datetime, timezone
from clients.postgres_client import PostgresClient
from clients.itc_dhis2_source_client import ItcDhis2SourceClient
from utils.config import config

from utils.logger import get_logger
logger = get_logger(__name__)



def sync_orgunits():
    """ Lance la synchronisation DHIS2 côté serveur. """
    try:
        pg = PostgresClient()            
        dhis = ItcDhis2SourceClient(store_in_db=True)
        orgunits = dhis.fetch_organisation_units(level=5)
        pg.bulk_upsert_data("organisationUnits", orgunits)
        return ({"status": "ok", "synced": len(orgunits)}, 200)
    except Exception as ex:
        logger.exception("Sync failed")
        return ({"error": "sync failed", "detail": str(ex)}, 500)
    
def sync_dataelements():
    """ Lance la synchronisation DHIS2 côté serveur. """
    try:
        dhis = ItcDhis2SourceClient(store_in_db=True)
        elements = dhis.fetch_dataelements()
        return ({"status": "ok", "synced": len(elements)}, 200)
    except Exception as ex:
        logger.exception("Sync failed")
        return ({"error": "sync failed", "detail": str(ex)}, 500)

def sync_teis_enrollments_events_attributes(orgunit_id=None, doTei =  True, doEnroll = True, doAttribute = True, doEvent = True):
    try:
        pg = PostgresClient()            
        dhis = ItcDhis2SourceClient(store_in_db=True)
        last_sync_date: datetime = pg.get_last_sync()
        program = config.PROGRAM_TRACKER_ID
        # Détermination des payloads
        orgunit_ids = ([orgunit_id] if orgunit_id else [ou["id"] for ou in pg.list_orgunits(level=5) if ou.get("id")])
        # Combinaisons (ou_id, index)
        payloads = [(program, ou_id, ou_index,doTei,doEnroll,doAttribute,doEvent,last_sync_date) for ou_index, ou_id  in enumerate(orgunit_ids)]
        # Appel async multipayload
        data = dhis.get_multi_async_request(payload_method=dhis.fetch_teis_enrollments_events_attributes,payloads=payloads)
        if doTei and doEnroll and doAttribute and doEvent:
            now = datetime.now(timezone.utc)
            pg.update_last_sync(now)
        return ({
            "teis": len(data.get("teis", [])),
            "enrollments": len(data.get("enrollments", [])),
            "events": len(data.get("events", [])),
            "attributes": len(data.get("attributes", [])),
        }, 200)
    except Exception as ex:
        return ({"error": str(ex)}, 500)

