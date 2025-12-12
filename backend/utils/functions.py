import os
import json
from typing import Any, List, Dict, Union
from datetime import date, datetime,timedelta
from decimal import Decimal
from uuid import UUID
import re

from utils.logger import get_logger
logger = get_logger(__name__)


def generate_dhis2_dates():
    today = datetime.today()
    tomorrow = today + timedelta(days=1)

    # Format ISO 8601 complet avec millisecondes
    iso_format = "%Y-%m-%dT%H:%M:%S.000"

    completion_date = today.strftime(iso_format)
    validation_date = today.strftime(iso_format)
    finished_date = tomorrow.strftime(iso_format)

    return {
        "completion_date": completion_date,
        "validation_date": validation_date,
        "finished_date": finished_date
    }


# ------------------------
# Utils
# ------------------------
def isNotEmpty(value) -> bool:
    """
    Vérifie si une variable (liste, dictionnaire, chaîne, etc.) n’est pas vide.

    Args:
        value: Objet à vérifier.

    Returns:
        bool: True si l’objet n’est pas vide ou non nul, False sinon.
    """
    if value is None:
        return False
    
    if (isinstance(value, list) and len(value) > 0):
        return True
    
    if (isinstance(value, dict) and len(value.keys()) > 0):
        return True

    # Cas des types standards
    if isinstance(value, (list, dict, set, tuple, str)):
        return len(value) > 0

    # Cas des nombres
    if isinstance(value, (int, float)):
        return value != 0

    # Pour tout autre type (objets personnalisés, etc.)
    return value not in (None, "", [], {}, ())

def clean_object_from_data(data: Any, keys_to_remove: List[str] = None) -> Any:
    """
    Nettoie récursivement les données :
    - Supprime toutes les clés spécifiées dans `keys_to_remove` à tous les niveaux.
    - Fonctionne sur des dictionnaires, listes ou structures imbriquées.

    Args:
        data: Données à nettoyer (dict, list, etc.)
        keys_to_remove: Liste des clés à supprimer (facultatif)

    Returns:
        Données nettoyées (même structure que l’entrée)
    """
    if not keys_to_remove:
        return data

    # Si c'est une liste → traiter chaque élément
    if isinstance(data, list):
        return [clean_object_from_data(item, keys_to_remove) for item in data]

    # Si c'est un dictionnaire → supprimer les clés, puis traiter récursivement les valeurs
    if isinstance(data, dict):
        for key in keys_to_remove:
            data.pop(key, None)

        for k, v in list(data.items()):
            if isinstance(v, (dict, list)):
                data[k] = clean_object_from_data(v, keys_to_remove)

    # Autres types → retourner tels quels
    return data

def extend_from_json(data: Dict) -> List[Dict]:
    """
    Reads a JSON file and extracts all non-empty items into a list.
    Handles both list and single object structures.
    """
    target_list = []
    for values in data.values():
        if isNotEmpty(values):
            # Si c'est une liste, on itère dedans
            if isinstance(values, list):
                for item in values:
                    if isNotEmpty(item):
                        target_list.append(item)
            # Si c'est un élément unique, on l'ajoute directement
            else:
                target_list.append(values)
    return target_list

def convert_to_float_if_pure(v: str):
    v = v.strip()
    v_numeric = v.replace(",", ".")
    if re.fullmatch(r"[+-]?\d*\.\d+(e[+-]?\d+)?", v_numeric, re.IGNORECASE):
        try:
            return float(v_numeric)
        except ValueError:
            return v
    return v

def convert_to_int_if_pure(v: str):
    v = v.strip()
    if v == "0":
        return 0
    if re.fullmatch(r"[+-]?[1-9]\d*", v):
        return int(v)
    return convert_to_float_if_pure(v)

def clean(value):
    if value is None:
        return None

    # Booléens natifs
    if isinstance(value, bool):
        return value

    # Entiers, floats, Decimal → type natif
    if isinstance(value, (int, float, Decimal)):
        return value

    # Dates et datetime → isoformat
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # UUID
    if isinstance(value, UUID):
        return str(value)

    # Listes et dicts → nettoyage récursif
    if isinstance(value, list):
        return [clean(v) for v in value]
    
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}

    # Si ce n'est pas une chaîne, retour direct
    if not isinstance(value, str):
        return value

    # Nettoyer les espaces et normaliser
    v = value.strip()
    v_lower = v.lower()

    # Valeurs nulles
    if v_lower in {"", "null", "none", "undefined", "nan"}:
        return None

    # Valeurs True/False
    if v_lower in {"true", "yes", "oui"}: # on évite "1" ici pour ne pas convertir id DHIS2
        return True
    
    if v_lower in {"false", "no", "non"}:  # idem pour "0"
        return False
    
    return v
    
def json_default(obj):
    """
    Fonction de sérialisation JSON pour types non natifs.
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8")
    raise TypeError(f"Type {type(obj)} not serializable")

def convert_dates(obj):
    """
    Convertit récursivement les objets datetime/date en ISO format dans dicts et lists.
    Gère aussi Decimal, UUID et bytes.
    """
    if isinstance(obj, dict):
        return {k: convert_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates(i) for i in obj]
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, UUID):
        return str(obj)
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    else:
        return obj

def store_to_local_file(data_to_store:Any, endpoint:str, bigdata_length:int, index:int=0, store_in_file:bool = True):
    if store_in_file == True:
        # JSON : sauvegarde dans fichier
        filename = f"outputs_files/{index}_{endpoint}.json"
        
        # 🔥 Créer automatiquement le dossier s'il n'existe pas
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        data_to_store = convert_dates(data_to_store)

        # 🔥 Sauvegarde JSON
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data_to_store, f, ensure_ascii=False, indent=2)

        logger.info("Stockage local OK → %s (%d/%d écrits)", filename, len(data_to_store), bigdata_length )
        # logger.info("%d %s stockés en DB", len(data_to_store), endpoint)

def build_date(date_input: Union[str, datetime], start: bool = True) -> str:
    """
    Transforme une date en timestamp DHIS2 ISO8601 millisecondes.
    Accepte : - 'YYYY-MM-DD'
              - 'YYYY-MM-DDTHH:MM:SS.mmm'
              - datetime()
    Retour : - start=True  -> YYYY-MM-DDT00:00:00.000  
             - start=False -> YYYY-MM-DDT23:59:00.000
    Si la date a déjà des heures, elles ne sont pas modifiées.
    """
    # 1. Conversion string → datetime
    if isinstance(date_input, datetime):
        date_obj = date_input
        has_time = not (date_obj.hour == 0 and date_obj.minute == 0 and date_obj.second == 0 and date_obj.microsecond == 0)
    else:
        date_str = date_input.strip()
        try:
            if "T" in date_str:  # format ISO
                date_obj = datetime.fromisoformat(date_str.replace("Z", ""))
                has_time = True
            else:  # format simple YYYY-MM-DD
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                has_time = False
        except Exception:
            raise ValueError(
                f"Invalid date format: '{date_input}'. "
                "Expected 'YYYY-MM-DD' or ISO8601 like 'YYYY-MM-DDTHH:MM:SS.mmm'."
            )

    # 2. Appliquer heure début/fin uniquement si date sans heure
    if not has_time:
        if start:
            date_obj = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            date_obj = date_obj.replace(hour=23, minute=59, second=0, microsecond=0)

    # 3. Format DHIS2 (millisecondes)
    # return date_obj.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return date_obj.strftime("%Y-%m-%dT%H:%M:%S")[:-3]

def to_datetime(date_input: Union[str, datetime]) -> datetime:
    """
    Convertit toute date (string ou datetime) en objet datetime Python.
    Si l'entrée est déjà 'datetime', elle est formatée selon le format DHIS2 standard.
    """
    DHIS2_FORMAT = "%Y-%m-%dT%H:%M:%S"
    formats = ["%Y-%m-%dT%H:%M:%S.%f","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S.%f","%Y-%m-%d %H:%M:%S","%Y-%m-%d"]

    # Si déjà datetime → on normalise au format DHIS2 puis reconvertit proprement
    if isinstance(date_input, datetime):
        try:
            normalized = date_input.strftime(DHIS2_FORMAT)
            return datetime.strptime(normalized, DHIS2_FORMAT)
        except Exception:
            pass

    if not date_input:
        raise ValueError("Empty date string")

    ds = date_input.strip().replace("Z", "")  # Nettoyage

    for fmt in formats:
        try:
            dt = datetime.strptime(ds, fmt)
            # Normalisation DHIS2 une fois le parsing réussi
            normalized = dt.strftime(DHIS2_FORMAT)
            return datetime.strptime(normalized, DHIS2_FORMAT)
        except ValueError:
            pass
        
    try:
        dt = datetime.fromisoformat(ds)
        normalized = dt.strftime(DHIS2_FORMAT)
        return datetime.strptime(normalized, DHIS2_FORMAT)
    except Exception:
        pass

    raise ValueError(f"Invalid datetime format: '{date_input}'")

