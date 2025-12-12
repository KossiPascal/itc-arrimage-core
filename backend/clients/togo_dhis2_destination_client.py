import os
import json
import time
import asyncio
import aiohttp
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Tuple, Optional

import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth

from utils.config import config
from utils.db import get_connection
from utils.functions import generate_dhis2_dates
from utils.logger import get_logger

logger = get_logger(__name__)


def build_date(date_str: str, start: bool = True) -> str:
    """
    Transforme 'YYYY-MM-DD' en timestamp DHIS2 ISO8601
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: '{date_str}', expected YYYY-MM-DD")
    
    if start:
        date_obj = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        date_obj = date_obj.replace(hour=23, minute=59, second=0, microsecond=0)
    
    return date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]



class AsyncDhis2Sender:
    def __init__(self, api_base: str, username: str, password: str, timeout: int = 30, use_ssl: bool = True):
        self.api_base = api_base.rstrip('/')
        self.auth = aiohttp.BasicAuth(username, password)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.verify_ssl = use_ssl

    async def _send_payload(self, session: aiohttp.ClientSession, payload: dict) -> dict:
        url = f"{self.api_base}/dataValueSets"
        data_set = payload.get("dataSet")
        period = payload.get("period")
        orgunit = payload.get("orgUnit")

        try:
            async with session.post(url, json=payload, timeout=self.timeout) as resp:
                resp_json = await resp.json()
                if resp.status in (200, 201):
                    logger.info(f"✅ Sent {data_set} | {period} | {orgunit} successfully")
                    return {"success": True, "payload": payload}
                else:
                    logger.warning(f"⚠️ Failed {data_set} | {period} | {orgunit} - HTTP {resp.status}: {resp_json}")
                    return {"success": False, "payload": payload, "error": resp_json}
        except Exception as e:
            logger.error(f"❌ Exception sending {data_set} | {period} | {orgunit}: {e}")
            return {"success": False, "payload": payload, "error": str(e)}

    async def send_all(self, payloads: List[dict], max_concurrent: int = 5) -> List[dict]:
        """
        Envoi simultané de plusieurs payloads
        :param payloads: liste de payloads à envoyer
        :param max_concurrent: nombre max de requêtes simultanées
        """
        connector = aiohttp.TCPConnector(ssl=self.verify_ssl, limit=max_concurrent)
        async with aiohttp.ClientSession(auth=self.auth, timeout=self.timeout, connector=connector) as session:
            tasks = [self._send_payload(session, p) for p in payloads]
            results = await asyncio.gather(*tasks, return_exceptions=False)
        return results

    def run(self, payloads: List[dict], max_concurrent: int = config.MAX_WORKERS) -> List[dict]:
        """
        Wrapper pour exécuter l'envoi asynchrone depuis du code synchrone
        """
        return asyncio.run(self.send_all(payloads, max_concurrent=max_concurrent))
    


class TogoDhis2DestinationClient:
    def __init__(self, send_to_dhis2: bool = False, save_to_local_file: bool = False, send_multi_async:bool = True):
        self.api_base = config.TOGO_DHIS2_URL.rstrip('/')
        self.username = config.TOGO_DHIS2_USER
        self.password = config.TOGO_DHIS2_PASS
        self.TIMEOUT = config.TIMEOUT or 30
        self.RETRY_DELAY = config.RETRY_DELAY or 2
        self.MAX_RETRIES = config.MAX_RETRIES or 3

        self.send_to_dhis2 = send_to_dhis2
        self.save_to_local_file = save_to_local_file
        self.send_multi_async = send_multi_async
        
        self.conn = get_connection()
        self.dataset_id = "mxX2xHChatk"
        self.verify_ssl = config.USE_SSL

        if not self.conn:
            raise ValueError("❌ PostgreSQL connection is null")

        if not all([self.api_base, self.username, self.password]):
            raise ValueError("DHIS2 URL, username and password must be defined")

        self.auth = HTTPBasicAuth(self.username, self.password)
        
        # Session persistante avec retry
        self.session = requests.Session()
        retries = Retry(total=self.MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

    def _safe_request(self, method: str, url: str, **kwargs) -> dict:
        """Request avec retry et logging"""
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self.session.request(method, url, timeout=self.TIMEOUT, verify=self.verify_ssl, **kwargs)
                if response.status_code in (200, 201):
                    return response.json()
                else:
                    logger.warning(f"[DHIS2] HTTP {response.status_code}: {response.text}")
            except requests.RequestException as e:
                logger.warning(f"[DHIS2] Attempt {attempt} failed: {e}")
            
            time.sleep(self.RETRY_DELAY * attempt)
        
        raise ConnectionError(f"Failed to request {url} after {self.MAX_RETRIES} retries")

    def _create_or_update_aggregated_data(self, payload: dict) -> dict:
        """Envoi DHIS2 dataValueSets"""
        if not payload or not isinstance(payload, dict):
            raise ValueError("Payload must be a non-empty dict")

        base_url = f"{self.api_base}/dataValueSets"

        # Check existence
        check_url = f"{base_url}?dataSet={payload['dataSet']}&period={payload['period']}&orgUnit={payload['orgUnit']}"
        try:
            existing = self._safe_request("GET", check_url)
        except Exception as e:
            logger.error(f"Cannot check existing data: {e}")
            raise

        data_exists = len(existing.get("dataValues") or []) > 0

        # POST to DHIS2 (create/update)
        try:
            result = self._safe_request("POST", base_url, data=json.dumps(payload))
            status = "Updated" if data_exists else "Created"
            logger.info(f"{status} data sent successfully to DHIS2 for orgUnit {payload['orgUnit']}, period {payload['period']}")
            return {"success": True, "status": status, "response": result}
        except Exception as e:
            logger.error(f"Failed to send data to DHIS2: {e}")
            return {"success": False, "status": "Failed", "error": str(e)}

    def _fetch_matview_indicators(self, queries: List[str], params=None) -> List[List[dict]]:
        """Exécution sécurisée des requêtes PostgreSQL"""
        results = []
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                for i, query in enumerate(queries):
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    serializable = [{k: int(v) if isinstance(v, Decimal) else v for k, v in row.items()} for row in rows]

                    if self.save_to_local_file:
                        with open(f"query{i}.json", "w") as f:
                            json.dump(serializable, f, indent=2)
                    results.append(serializable)
        except Exception as e:
            logger.error(f"Query failed: {e}", exc_info=True)
        return results

    def build_dhis2_datavalues(self, queries: List[str], period: Optional[str] = None, orgunit_id: Optional[str] = None) -> Tuple[str, bool, int]:
        """Transformation en datavalues DHIS2"""
        params = (period, orgunit_id) if period and orgunit_id else None
        results = self._fetch_matview_indicators(queries, params)

        try:
            with open("helpers/indicators_map.json", "r", encoding="utf-8") as f:
                indicators_map = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load indicators map: {e}")
            return ("Indicators map load failed", False, 0)

        dates = generate_dhis2_dates()
        completedDate = dates.get("completion_date")

        data_maps: Dict[str, dict] = {}
        for rows in results:
            for row in rows:
                key = f"{row['period']}-{row['orgunit_id']}"
                if key not in data_maps:
                    data_maps[key] = {
                        "dataSet": self.dataset_id,
                        "period": row['period'],
                        "orgUnit": row['orgunit_id'],
                        "completedDate": completedDate,
                        "dataValues": []
                    }

                for k, v in row.items():
                    if k in ("period", "orgunit_id") or v is None or int(v) <= 0:
                        continue

                    de_combo = indicators_map.get(k)
                    if not de_combo:
                        continue

                    data_maps[key]["dataValues"].append({
                        "dataElement": str(de_combo.get("de")),
                        "categoryOptionCombo": str(de_combo.get("combo")),
                        "value": int(v)
                    })

        data_to_send = [v for v in data_maps.values() if v["dataValues"]]
        dataToSendLength = len(data_to_send)
        if not data_to_send or dataToSendLength == 0:
            return ("No data to send", True, dataToSendLength)

        success_all = True

        if self.send_to_dhis2:
            if self.send_multi_async is True:
                sender = AsyncDhis2Sender(
                    api_base=self.api_base,
                    username=self.username,
                    password=self.password,
                    timeout=self.TIMEOUT,
                    use_ssl=config.USE_SSL
                )
                # Envoi max 10 payloads simultanément
                results = sender.run(data_to_send)
                success_all = all(r["success"] for r in results) if results else True
            else:
                res_results = []
                for payload in data_to_send:
                    res = self._create_or_update_aggregated_data(payload)
                    res_results.append(res["success"])
                success_all = all(res_results) if res_results else True


        if self.save_to_local_file:
            with open("data_to_send.json", "w") as f:
                json.dump(data_to_send, f, indent=2)

        message = "Successfully sent to DHIS2" if self.send_to_dhis2 else "Success transformed"
        return (message, success_all, dataToSendLength)


    def fetch_togo_dataelements(self) -> List[dict]:
        """Récupération des dataElements ITC"""
        try:
            url = f"{self.api_base}/dataElements.json"
            params = {"fields": "id,name,valueType,categoryCombo,dataSetElements", "paging": "false"}
            res = self.session.get(url, params=params, timeout=self.TIMEOUT, verify=self.verify_ssl)
            res.raise_for_status()
            data = res.json().get("dataElements", [])
            filtered = [d for d in data if "-ITC_" in d.get("name", "")]
            if self.save_to_local_file:
                with open("togo_dataelements.json", "w") as f:
                    json.dump(data, f, indent=2)
                with open("togo_dataelements_itc.json", "w") as f:
                    json.dump(filtered, f, indent=2)
            logger.info(f"Fetched {len(filtered)} ITC dataElements from DHIS2")
            return filtered
        except Exception as e:
            logger.error(f"Failed to fetch dataElements: {e}", exc_info=True)
            return []

