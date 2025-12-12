import requests
from requests.auth import HTTPBasicAuth
from utils.config import config
from time import sleep
from typing import Any, List, Dict, Union, Callable
from datetime import datetime, timezone
import concurrent.futures
from utils.interfaces import EndpointSpec
from clients.postgres_client import PostgresClient
from utils.functions import clean_object_from_data, clean, store_to_local_file, build_date

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging
from utils.logger import get_logger
logger = get_logger(__name__)



class ItcDhis2SourceClient:
    """Client DHIS2 (TEI, Enrollments, Events) singleton pour éviter réinitialisation répétée."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, store_in_db: bool = False, store_in_local_file: bool = False):
        self.force_init = config.FORCE_INIT_CLASS

        if self._initialized and self.force_init is not True:
            return

        self.pg = PostgresClient()  # Singleton PostgresClient
        self.base_url = config.DHIS2_URL.rstrip('/')
        self.username = config.DHIS2_USER
        self.password = config.DHIS2_PASS
        self.store_in_local_file = store_in_local_file
        self.store_in_db = store_in_db

        if not all([self.base_url, self.username, self.password]):
            raise ValueError("DHIS2_URL, DHIS2_USER, DHIS2_PASS doivent être définis.")
        if self.store_in_db and not self.pg:
            raise ValueError("store_in_db=True mais postgres_client non fourni.")

        # Session HTTP unique
        self.auth = HTTPBasicAuth(self.username, self.password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

        # Caches internes (facultatif, ex: cache endpoint si très volumineux)
        self._endpoint_cache: Dict[str, Any] = {}

        self._initialized = True


    def _get(self, endpoint, params=None):
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        for attempt in range(1, config.MAX_RETRIES + 1):
            try:
                res = self.session.get(url, params=params, timeout=config.TIMEOUT, verify=config.USE_SSL)
                res.raise_for_status()
                return res.json()
            except requests.exceptions.RequestException as e:
                if attempt < config.MAX_RETRIES:
                    logger.warning("⏳ Erreur DHIS2 GET %s (tentative %d/%d): %s", endpoint, attempt, config.MAX_RETRIES, e)
                    # logger.warning(f"⏳ Retry {attempt}/{config.MAX_RETRIES} dans {config.RETRY_DELAY}s...")
                    sleep(config.RETRY_DELAY)
                    continue
        raise Exception(f"Échec après {config.MAX_RETRIES} tentatives sur {endpoint}")
    
    def _paginate(self, endpoint, params=None, keys_to_remove: List[str] = None, page_size=100):
        """
        Pagination automatique DHIS2 (pour TEI, Enrollments, Events)
        endpoint peut être : "trackedEntityInstances.json" ("trackedEntityInstances.json", "trackedEntityInstances")
        """
        # --- Résolution du endpoint et de la clé contenant les données ---
        if isinstance(endpoint, (list, tuple)):
            dhis2_endpoint, data_key = endpoint
        else:
            dhis2_endpoint = endpoint
            data_key = endpoint.replace(".json", "") # ex : "trackedEntityInstances.json" -> "trackedEntityInstances"

        all_results = []
        page = 1
        params = params.copy() if params else {}
        params.update({"paging": "true","pageSize": page_size, "page": page})

        while True:
            data = self._get(dhis2_endpoint, params=params)
            results = data.get(data_key) or []
            all_results.extend(results)
            pager = data.get("pager")
            if pager and pager.get("page") < pager.get("pageCount"):
                page += 1
                params["page"] = page
            else:
                break
        # Nettoyage (optionnel)
        if keys_to_remove:
            return clean_object_from_data(all_results, keys_to_remove)

        return all_results
    
    # --- Multi async request ---
    def get_multi_async_request(self,payload_method: Callable[..., Any],payloads: List[tuple]) -> Any:
        """
        Exécute plusieurs appels asynchrones avec ThreadPoolExecutor.
        payload_method: fonction qui prend un payload et retourne soit:
            - dict[str, list] si plusieurs cibles
            - list ou single item si une seule cible
        payloads: liste de payloads à traiter
        Retourne:
            - dict[cible -> liste] si plusieurs cibles
            - list si une seule cible
        """

        result_dict: Dict[str, List[Any]] = {}

        # Normalisation des payloads : toujours tuple
        normalized_payloads = [p if isinstance(p, tuple) else (p,) for p in payloads]

        def safe_call(*args) -> Dict[str, List[Any]]:
            try:
                result = payload_method(*args)
                # toujours retourner dict si c'est un dict
                if isinstance(result, dict):
                    return result
                # convertir en dict si c'est une liste ou un seul élément
                return {"data": result if isinstance(result, list) else [result]}
            except Exception as e:
                logger.error("Erreur lors de l'appel de payload_method: %s", e)
                return {"data": []}

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            futures = [executor.submit(safe_call, *p) for p in normalized_payloads]
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    # Vérification du type
                    if isinstance(data, dict):
                        for key, items in data.items():
                            if key not in result_dict:
                                result_dict[key] = []
                            # Vérifie que items est bien itérable
                            if isinstance(items, list):
                                result_dict[key].extend(items)
                            else:
                                result_dict[key].append(items)
                    else:
                        logger.warning(f"Data non itérable reçue: {data} (type={type(data)})")

                except Exception as e:
                    logger.error("Erreur lors de la récupération asynchrone: %s", e)
        # Décider du retour
        if len(result_dict) == 1:
            return list(result_dict.values())[0]  # retourne juste la liste
        return result_dict


    # Sauvegarde
    def _store(self, dataToStore: List[Any], dataEndpoint:Union[str, EndpointSpec], dataIdsToDelete: List[str] = None):
        """ Stockage générique avec EndpointSpec : """
        # Normaliser en liste
        data = dataToStore if isinstance(dataToStore, list) else ([dataToStore] if dataToStore else [])
        data_ids_to_delete = dataIdsToDelete if isinstance(dataIdsToDelete, list) else ([dataIdsToDelete] if dataIdsToDelete else [])

        data_length = len(data)
        data_to_delete_length = len(data_ids_to_delete)

        if data_length == 0 and data_to_delete_length == 0:
            return False
        
        if isinstance(dataEndpoint, EndpointSpec):
            spec = EndpointSpec.parse(dataEndpoint)
            endpoint, index = spec.to_tuple()
        else:
            endpoint = endpoint
            index = 0

        # Si pas de DB → sauver en local et sortir
        if not getattr(self, "store_in_db", False):
            if data_length > 0:
                store_to_local_file(data, endpoint, data_length, index, self.store_in_local_file)
            return True

        if not getattr(self, "pg", None):
            logger.debug("store_in_db=True mais aucun postgres_client (pg) n'est défini.")
            return False
        

        if self.store_in_db:
            # Stockage en base selon le type
            if data_length > 0:
                logger.debug(f"📦 Stockage endpoint='{endpoint}', index={index}, deleted_ids={len(data_ids_to_delete or [])}\n\n")
                saved = []
                errors = []

                for item in data:
                    try:
                        if self.pg.upsert_data(endpoint, item):
                            saved.append(item)
                    except Exception as e:
                        logger.error("Erreur lors du upsert %s : %s", endpoint, e)
                        errors.append(item)
                # try:
                #     if self.pg.bulk_upsert_data(endpoint, data):
                #         saved = data
                # except Exception as e:
                #     logger.error("Erreur lors du upsert %s : %s", endpoint, e)
                #     return False

                if len(saved) > 0:
                    # Sauvegarde locale des enregistrements réellement insérés en DB
                    store_to_local_file(saved, endpoint, data_length, index, self.store_in_local_file)

                if len(errors) > 0:
                    # Sauvegarde locale des enregistrements réellement insérés en DB
                    store_to_local_file(errors, f'{endpoint}/data_errors', len(errors), index, self.store_in_local_file)

            if data_to_delete_length > 0:
                deleteError = []
                for d_id in data_ids_to_delete:
                    try:
                        if self.pg.delete_data(endpoint, d_id):
                            pass
                    except Exception as e:
                        # print(f'd_id: {d_id}')
                        logger.error("❌ Erreur lors du delete %s : %s", endpoint, e)
                        deleteError.append({f"{endpoint}": d_id})

                if len(deleteError) > 0:
                    store_to_local_file(deleteError, f'{endpoint}/delete_errors', len(deleteError), index, self.store_in_local_file)


                # try:
                #     if self.pg.bulk_delete_data(endpoint, data_ids_to_delete):
                #         logger.debug(f"📦 deleted_ids={data_to_delete_length}")
                #     return True
                # except Exception as e:
                #     # print(f'd_id: {d_id}')
                #     logger.error("❌ Erreur lors du delete %s : %s", endpoint, e)
                #     return False
        else:
            if data_length > 0:
                # Sauvegarde locale des enregistrements réellement insérés en DB
                store_to_local_file(data, endpoint, data_length, index, self.store_in_local_file)
            return False

    def fetch_organisation_units(self, level: int = None, fetch_index:int = 0) -> List[Dict]:
        """
        Récupère la liste des unités d’organisation (orgUnits) depuis DHIS2,
        filtrées par niveau si fourni, avec gestion dynamique de la pagination
        et aplatissement automatique des résultats.
        """
        # === Mode sans pagination ===
        params = {
            "fields": "id,name,shortName,level,parent[id,name,shortName,level]",
            "paging": "false"
        }

        if level and isinstance(level, int):
            params["filter"] = f"level:eq:{level}"

        endpoint = "organisationUnits"

        logger.info("🏢 Récupération des unités d’organisation...")
        orgunits = self._paginate(endpoint, params=params)
        # Aplatissement au cas où DHIS2 renverrait un tableau imbriqué
        if any(isinstance(o, list) for o in orgunits):
            orgunits = [item for sublist in orgunits for item in (sublist if isinstance(sublist, list) else [sublist])]

        data = sorted(orgunits, key=lambda x: x.get("level", 0))
        self._store(data, EndpointSpec(endpoint, fetch_index))
        return data

    def get_sync_range(self,start_date: str|datetime| None, end_date: str|datetime|None, last_sync_time: str|datetime|None, today: str|datetime) -> tuple[str, str]:
        """
        Détermine la plage de synchronisation DHIS2 en fonction
        des paramètres fournis par l'utilisateur.

        3 modes :
        - Mode 1 : start & end fournis → plage exacte
        - Mode 2 : start seul → start → today
        - Mode 3 : aucun → incrémentale last_sync_time → today
        """

        # Si jamais last_sync_time n'existe pas encore, on initialise
        if not last_sync_time:
            last_sync_time = "2022-01-01T00:00:00"

        # --- MODE 1 : PLAGE COMPLÈTE ---
        if start_date and end_date:
            return (build_date(start_date, start=True),build_date(end_date, start=False))

        # --- MODE 2 : START SEUL ---
        if start_date and not end_date:
            return (build_date(start_date, start=True),build_date(today, start=False))

        # --- MODE 3 : INCRÉMENTALE AUTOMATIQUE ---
        # on utilise last_sync_time comme début
        return (build_date(last_sync_time, start=True),build_date(today, start=False))
    

    # TEI, Enrollments, Events
    def fetch_teis_enrollments_events_attributes(self,program:str,orgunit_id:str,fetch_index:int=0,doTei=True,doEnroll=True,doAttribute=True,doEvent=True,last_sync_time:datetime=None,start_date=None,end_date=None):
        """ Construction robuste des paramètres DHIS2 pour synchronisation TEI. """
        if not program or not orgunit_id:
            raise ValueError("program et orgunit_id doivent être définis.")
        
        params = { "paging": "false", "program": program, "ou": orgunit_id, "fields": "*,attributes[*],enrollments[*,events[*]]," }
        # params["ouMode"] = "DESCENDANTS"

        # Date actuelle en format DHIS2
        today_date = datetime.now(timezone.utc)
        start_iso, end_iso = self.get_sync_range(start_date,end_date,last_sync_time,today_date)

        params["lastUpdatedStartDate"] = start_iso
        params["lastUpdatedEndDate"] = end_iso

        logger.info("Récupération des TEI et ses Enrollments et ses Events depuis DHIS2...")
        # "/".join(["trackedEntityInstances", tei_id])
        endpoint = "trackedEntityInstances"
        keys_to_remove = ["lastUpdatedAtClient","lastUpdatedByUserInfo","createdByUserInfo","storedBy","href"]
        
        raw_data = self._paginate(endpoint, params=params,keys_to_remove=keys_to_remove)

        # enrollment_data = extend_from_json(raw_data)

        # Résultats nettoyés
        teis: List[Dict] = []
        enrollments: List[Dict] = []
        attributes: List[Dict] = []
        events: List[Dict] = []

        # IDs à supprimer
        teis_to_delete_ids: List[str] = []
        enrollments_to_delete_ids: List[str] = []
        attributes_to_delete_ids: List[str] = []
        events_to_delete_ids: List[str] = []

        for tei in raw_data:
            if not isinstance(tei, dict):
                continue

            # 🔹 Copie locale pour éviter toute modification sur les données d'origine
            tei = tei.copy()
            is_tei_deleted = tei.get("deleted", False)

            # 🔸 Extraire et nettoyer les événements
            enrollments_list = tei.pop("enrollments", [])
            for enrollment in enrollments_list:
                if not isinstance(enrollment, dict):
                    continue

                # 🔹 Copie locale pour éviter toute modification sur les données d'origine
                enrollment = enrollment.copy()
                is_enrollment_deleted = enrollment.get("deleted", False)

                # 🔸 Extraire et nettoyer les événements
                event_list = enrollment.pop("events", [])
                for event in event_list:
                    if not isinstance(event, dict):
                        continue

                    # 🔹 Copie locale pour éviter toute modification sur les données d'origine
                    event = event.copy()
                    is_event_deleted = event.get("deleted", False)

                    if is_tei_deleted or is_enrollment_deleted or is_event_deleted:
                        events_to_delete_ids.append(event.get("event"))
                        continue
                
                    for key in ["relationships", "notes"]:
                        event.pop(key, None)

                    eventToStore = {
                        "id": event.get('event'),
                        "due_date": event.get('dueDate'),
                        "program": program,
                        "program_stage_id": event.get('programStage'),
                        "orgunit_id": event.get('orgUnit'),
                        # "orgunit_name": event.get('orgUnitName'),
                        "enrollment_id": event.get('enrollment'),
                        "tei_id": event.get('trackedEntityInstance'),
                        "enrollment_status": event.get('enrollmentStatus'),
                        "status": event.get('status'),
                        "event_date": event.get('eventDate'),
                        "attribute_category_options": event.get('attributeCategoryOptions'),
                        "last_updated": event.get('lastUpdated'),
                        "created": event.get("createdAtClient") or event.get("created"),
                        "deleted": bool(event.get("deleted", False)),
                        "attribute_option_combo": event.get('attributeOptionCombo'),
                        # "dataValues": [
                        #     {"value": dv.get('value'), "dataElement": dv.get('dataElement')} 
                        #     for dv in (event.get('dataValues') or [])
                        #     if isinstance(dv, dict)
                        # ]
                    }

                    # Ajouter dynamiquement les dataValues comme colonnes
                    data_values = event.get("dataValues") or []

                    for dv in data_values:
                        if isinstance(dv, dict):
                            de_id = dv.get("dataElement")
                            value = dv.get("value")

                            if de_id:  # éviter une clé None
                                eventToStore[de_id] = value

                    eventToStore = {k: clean(v) for k, v in eventToStore.items()}
                    events.append(eventToStore)


                trackedEntityInstanceId = enrollment.get('trackedEntityInstance')
                enrollmentId = enrollment.get('enrollment')
                # 🔸 Extraire et nettoyer les événements
                attributes_list = enrollment.pop("attributes", [])

                attribute_id = f"{trackedEntityInstanceId}-{enrollmentId}"

                # Chaque enrollment_id sera une ligne
                formated_attributs = {
                    "id": attribute_id,
                    "tei_id": trackedEntityInstanceId,
                    "enrollment_id": enrollmentId,
                    "orgunit_id": enrollment.get("orgUnit"),
                    "program": program,
                    "created": enrollment.get("created"),
                    "status": enrollment.get("status"),
                    "deleted": enrollment.get("deleted"),
                }

                for attribute in attributes_list:
                    if not isinstance(attribute, dict):
                        continue

                    # 🔹 Copie locale pour éviter toute modification sur les données d'origine
                    attribute = attribute.copy()
                    is_attribute_deleted = attribute.get("deleted", False)

                    if is_tei_deleted or is_enrollment_deleted or is_attribute_deleted:
                        attributes_to_delete_ids.append(attribute_id)
                        continue

                    attribute_id = attribute.get("attribute")
                    # displayName = attribute.get("displayName")
                    # valueType = attribute.get("valueType")
                    value = clean(attribute.get("value"))

                    # 🔹 Ajouter la valeur sous la colonne correspondant à attribute_id
                    formated_attributs[attribute_id] = value

                attributeToStore = {k: clean(v) for k, v in formated_attributs.items()}
                attributes.append(attributeToStore)

                # Ajouter ou supprimer l’enrollment
                if is_tei_deleted or is_enrollment_deleted:
                    enrollments_to_delete_ids.append(enrollment.get("enrollment"))
                    continue

                for key in ["relationships", "notes"]:
                    enrollment.pop(key, None)

                enrollmentToStore = {
                    "id": enrollmentId,
                    "program": program,
                    "orgunit_id": enrollment.get("orgUnit"),
                    "tei_id": trackedEntityInstanceId,
                    "tei_type": enrollment.get("trackedEntityType"),
                    # "orgunit_name": enrollment.get("orgUnitName"),
                    "enrollment_date": enrollment.get("enrollmentDate"),
                    "incident_date": enrollment.get("incidentDate"),
                    "last_updated": enrollment.get("lastUpdated"),
                    "created": enrollment.get("createdAtClient") or enrollment.get("created"),
                    "status": enrollment.get("status"),
                    "deleted": bool(enrollment.get("deleted", False)),
                }
                enrollmentToStore = {k: clean(v) for k, v in enrollmentToStore.items()}
                enrollments.append(enrollmentToStore)

            # Ajouter TEI nettoyé
            if is_tei_deleted:
                teis_to_delete_ids.append(tei.get("trackedEntityInstance"))
                continue

            # 🔸 Supprimer les champs inutiles dans l'enrollment
            for key in ["attributes","potentialDuplicate", "featureType", 
                        "inactive", "relationships", "notes", "programOwners"]:
                tei.pop(key, None)

            teiToStore = {
                "id": tei.get("trackedEntityInstance"),
                "orgunit_id": tei.get("orgUnit"),
                "created": tei.get("createdAtClient") or tei.get("created"),
                "last_updated": tei.get("lastUpdated"),
                "type": tei.get("trackedEntityType"),
                "deleted": bool(tei.get("deleted", False)),
                "program": program
            }
            teiToStore = {k: clean(v) for k, v in teiToStore.items()}
            teis.append(teiToStore)

        # Enregistrement dans la DB
        if doTei == True:
            self._store(teis, EndpointSpec("trackedEntityInstances", fetch_index), teis_to_delete_ids)
        if doEnroll == True:
            self._store(enrollments, EndpointSpec("enrollments", fetch_index), enrollments_to_delete_ids)
        if doAttribute == True:
            self._store(attributes, EndpointSpec("attributes", fetch_index), attributes_to_delete_ids)
        if doEvent == True:
            self._store(events, EndpointSpec("events", fetch_index), events_to_delete_ids)

        
        return {
            "teis": len(teis) if doTei == True else 0, 
            "events": len(events) if doEvent == True else 0, 
            "enrollments": len(enrollments) if doEnroll == True else 0, 
            "attributes": len(attributes) if doAttribute == True else 0
        }
    
    # Dataelements
    def fetch_dataelements(self, fetch_index:int = 0):
        """
        Récupère tous les Data Elements
        """
        params = { 
            "fields": "id,name,code,shortName,displayFormName,displayName,dataElementGroups,dimensionItemType,aggregationType,domainType,valueType,zeroIsSignificant,categoryCombo,optionSetValue,optionSet,dataSetElements,aggregationLevels,created",
            "paging": "false" 
        }
        endpoint = "dataElements"
        logger.info("Récupération des Data Elements depuis DHIS2...")
        data = self._paginate(endpoint, params=params)
        self._store(data, EndpointSpec(endpoint, fetch_index))
        return data
