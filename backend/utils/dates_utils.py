from datetime import datetime, date


def _parse_any_date(value):
    """
    Parse pratiquement n'importe quel format de date.
    Supporte : None, '', datetime, date, ISO, YYYY-MM-DD, YYYYMMDD, YYYY-MM, YYYY.
    Retourne datetime ou None.
    """
    if not value:
        return None

    # Déjà datetime ou date
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    # Sinon string
    value = str(value).strip()

    if not value:
        return None

    # ESSAIS MULTIPLES
    formats = [
        "%Y-%m-%d",
        "%Y%m%d",
        "%Y-%m",
        "%Y%m",
        "%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except:
            pass

    # Dernière tentative : ISO
    try:
        return datetime.fromisoformat(value.replace("Z", ""))
    except:
        raise ValueError(f"Impossible de parser la date : '{value}'")


def build_dhis2_period_list(start_date, end_date):
    """
    Génère une liste de périodes DHIS2 (YYYYMM) entre start_date et end_date.
    Gère tous les formats, valeurs None, inversions, etc.
    """

    d1 = _parse_any_date(start_date)
    d2 = _parse_any_date(end_date)

    # Cas 1 : aucune date → rien
    if not d1 or not d2:
        return []

    # Cas 2 : une seule date → retourner ce mois unique
    if d1 and not d2:
        return [f"{d1.year}{d1.month:02d}"]
    if d2 and not d1:
        return [f"{d2.year}{d2.month:02d}"]

    # Cas 3 : garantir d1 <= d2
    if d1 > d2:
        d1, d2 = d2, d1

    result = []

    year, month = d1.year, d1.month

    # Boucle jusqu'au mois d2 inclus
    while (year, month) <= (d2.year, d2.month):
        result.append(f"{year}{month:02d}")

        month += 1
        if month > 12:
            month = 1
            year += 1

    return result


from datetime import datetime, date

def get_previous_month(period_date=None) -> str:
    """
    Retourne le mois précédent au format YYYYMM.
    - period_date = None : utilise la date d'aujourd'hui
    - Accepte datetime, date, ou string de n'importe quel format raisonnable
    """

    # 1. Normaliser la date d'entrée
    if period_date is None:
        d = datetime.today()
    elif isinstance(period_date, datetime):
        d = period_date
    elif isinstance(period_date, date):
        d = datetime(period_date.year, period_date.month, period_date.day)
    else:
        # string, essayer ISO et autres formats fréquents
        txt = str(period_date).strip().replace("Z", "")
        try:
            d = datetime.fromisoformat(txt)
        except:
            # essai YYYYMM, YYYY-MM, YYYYMMDD
            for fmt in ("%Y%m", "%Y-%m", "%Y%m%d", "%Y-%m-%d"):
                try:
                    d = datetime.strptime(txt, fmt)
                    break
                except:
                    pass
            else:
                raise ValueError(f"Format de date non reconnue: {period_date}")

    # 2. Calcul du mois précédent
    year = d.year
    month = d.month

    if month == 1:
        prev_year = year - 1
        prev_month = 12
    else:
        prev_year = year
        prev_month = month - 1

    # 3. Retour YYYYMM
    return f"{prev_year}{prev_month:02d}"
