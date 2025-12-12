
from utils.config import config
from clients.togo_dhis2_destination_client import TogoDhis2DestinationClient
from typing import Tuple, List, Dict, Any

from utils.logger import get_logger
logger = get_logger(__name__)

class Dhis2ArrimateMaker:

    def __init__(self, send_to_dhis2: bool = False, save_to_local_file:bool = False):

        self.save_to_local_file = save_to_local_file

        self.table_name = config.MATVIEW_NAME
        self.dhis2 = TogoDhis2DestinationClient(send_to_dhis2, save_to_local_file)

    def _dynamic_sql_asc_rc_generation(self,indicators_list: list, user_where_clause:bool = True):
        sql_parts = ["period","orgunit_id"]
        statuses=["ASC", "RC"]
        for st in statuses:
            for ind in indicators_list:
                alias = f"{ind.lower()}_{st.lower()}"
                part = f"SUM({ind}) FILTER (WHERE status = '{st}') AS {alias}"
                sql_parts.append(part)
        
        columns = "\n    " + ",\n    ".join(sql_parts)
        where_clause = " \nWHERE period = %s AND orgunit_id = %s" if user_where_clause else ""
        sql = f"SELECT {columns} \nFROM {self.table_name}{where_clause} \nGROUP BY period, orgunit_id;"
        return sql

    # ou récupérés dynamiquement depuis la DB
    def _dynamic_sql_multiple_generation(self,indicators_list: list, user_where_clause:bool = True):
        sql_parts = ["period","orgunit_id"]
        age_groups = ["18-29", "30-44", "45-59", "60-75", "75+"]
        statuses=["ASC", "RC"]
        sexes=["M", "F"]

        for ag in age_groups:
            for sx in sexes:
                for st in statuses:
                    for ind in indicators_list:
                        alias = f"{ind.lower()}_{st.lower()}_{sx.lower()}_{ag.replace('-', '_').replace('+','plus').replace(' ','_')}"
                        part = f"SUM({ind}) FILTER (WHERE status = '{st}' AND sex = '{sx}' AND age_group = '{ag}') AS {alias}"
                        sql_parts.append(part)

        columns = "\n    " + ",\n    ".join(sql_parts)
        where_clause = " \nWHERE period = %s AND orgunit_id = %s" if user_where_clause else ""

        sql = f"SELECT {columns} \nFROM {self.table_name}{where_clause} \nGROUP BY period, orgunit_id;"
        return sql

    def _transform_and_send_data_to_dhis2(self,period=None, orgunit_id=None) -> Tuple[str, bool, int]:
        """
        Récupère les données d'une table PostgreSQL et les convertit au format DHIS2 dataValues.
        """
        user_where_clause = True if (period != None and orgunit_id != None and period != '' and orgunit_id != '') else False

        queries = []

        indicators = {
            #ASC_RC / PROPOSE_VALIDE
            "0": [
                    "proposition_faite",
                    "proposition_valide"
                ],
            #ASC_RC / Supervision
            "1": [
                    "supervision_rfs",
                    "supervision_rm",
                    "supervision_asc_superviseur",
                    "supervision_niveau_district",
                    "supervision_niveau_regions",
                    "supervision_niveau_central"
                ],
            #ASC_RC / Formations
            "2": [
                    "formation_pecimne",
                    "formation_paludisme",
                    "formation_pf_communautaire",
                    "formation_gestion_meg",
                    "formation_comm_vih",
                    "formation_malnutrition",
                    "formation_pec_pvvih",
                    "formation_promotion",
                    "formation_change_cptm",
                    "formation_assainissement",
                    "formation_coinfection_tb",
                    "formation_maladi_non_transmissible",
                    "formation_Maladi_tropicale",
                    "formation_suivi_rapportage",
                    "formation_qualite_soins_nc",
                    "formation_sante_mere",
                    "formation_surveil_epidemiologique",
                    "formation_others"
                ],
            #ASC_RC->Matériel En Bon Etat
            "3": [
                    "sac_good_state",
                    "velo_good_state",
                    "stylos_good_state",
                    "torche_good_state",
                    "bottes_good_state",
                    "caisse_good_state",
                    "affiches_good_state",
                    "powerbank_good_state",
                    "smartphone_good_state",
                    "thermometre_good_state",
                    "boites_a_images_good_state",
                    "impermeables_raglan_good_state",
                    "autres_equipement"
                ],
            #ASC_RC->Non opérationnel
            "4": [
                    "demission",
                    "abandon",
                    "licenciement",
                    "faute_grave"
                ],
            #ASC_RC / AGE / SEXE
            "5": [
                    "total",
                    "actif",
                    "decede",
                    "suivi_animateur_endogene",
                    "reunion_mensuelle",
                    "rapport_mensuel"
                ]
        }

        for k,v in indicators.items():
            query_build = None
            if k in ["0","1","2","3","4"]:
                query_build = self._dynamic_sql_asc_rc_generation(v, user_where_clause)
            elif k in ["5"]:
                query_build = self._dynamic_sql_multiple_generation(v, user_where_clause)

            if query_build:
                queries.append(query_build)

        if self.save_to_local_file is True:
            for i,query in enumerate(queries):
                with open(f"query{i}.sql", "w") as f:
                    f.write(query)
    
        message, status, length = self.dhis2.build_dhis2_datavalues(queries, period, orgunit_id)

        # if status:
        #     logger.info(message)
        # else:
        #     logger.error(message)

        return (message, status, length)
    

    def start_indicators_arrimage_with_dhis2(self,periods: List[str] = None,orgunit_ids: List[str] = None) -> List[Dict[str, Any]]:
        """
        Transforme et envoie les données DHIS2 pour les périodes/orgunit donnés.
        Retourne une liste d'objets { message, size, status }.
        """

        output: Dict[str, Dict[str, Any]] = {}

        # Normalisation des entrées
        periods = periods or []
        orgunit_ids = orgunit_ids or []

        # Mode multi (periode + orgunits)
        if periods and orgunit_ids:
            for orgunit_id in orgunit_ids:
                for period in periods:
                    message, status, length = self._transform_and_send_data_to_dhis2(period, orgunit_id)

                    if message not in output:
                        output[message] = {"message": message,"size": 0,"status": True}  # Par défaut tout va bien
                    # Accumuler la taille
                    output[message]["size"] += length
                    # Combiner le status (si un seul est False → False)
                    output[message]["status"] = output[message]["status"] and status

        # Mode single-run (pas d'entrée)
        else:
            message, status, length = self._transform_and_send_data_to_dhis2()
            output[message] = {"message": message,"size": length,"status": status}

        # Log final
        for message, data in output.items():
            logger.info(f'{message} -> size: {data["size"]} -> status: {data["status"]}')

        # Préparation du résultat final sous forme de liste
        results = list(output.values())

        return results
