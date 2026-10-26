"""IANA timezone of every city abroad (UF "ZZ") where Brazilians voted.

Urnas abroad run on local time (voting 08h-17h local), so their clocks must be
converted with the offset in force on election day (DST included).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from config import ELECTION_DATE

CITY_TZ = {
    "ABIDJÃ": "Africa/Abidjan", "ABU DHABI": "Asia/Dubai", "ABUJA": "Africa/Lagos",
    "ACCRA": "Africa/Accra", "ADIS ABEBA": "Africa/Addis_Ababa", "AMSTERDÃ": "Europe/Amsterdam",
    "AMÃ": "Asia/Amman", "ANCARA": "Europe/Istanbul", "ARGEL": "Africa/Algiers",
    "ARTIGAS": "America/Montevideo", "ASSUNÇÃO": "America/Asuncion", "ASTANA": "Asia/Almaty",
    "ATENAS": "Europe/Athens", "ATLANTA": "America/New_York", "BAGDÁ": "Asia/Baghdad",
    "BAKU": "Asia/Baku", "BAMAKO": "Africa/Bamako", "BANGKOK": "Asia/Bangkok",
    "BARCELONA": "Europe/Madrid", "BAREIN": "Asia/Bahrain", "BEIRUTE": "Asia/Beirut",
    "BELGRADO": "Europe/Belgrade", "BELMOPAN": "America/Belize", "BERLIM": "Europe/Berlin",
    "BISSAU": "Africa/Bissau", "BOGOTÁ": "America/Bogota", "BOSTON": "America/New_York",
    "BRATISLAVA": "Europe/Bratislava", "BRAZZAVILLE": "Africa/Brazzaville",
    "BRIDGETOWN": "America/Barbados", "BRUXELAS": "Europe/Brussels",
    "BUCARESTE": "Europe/Bucharest", "BUDAPESTE": "Europe/Budapest",
    "BUENOS AIRES": "America/Argentina/Buenos_Aires", "CAIENA": "America/Cayenne",
    "CAIRO": "Africa/Cairo", "CAMBERRA": "Australia/Sydney", "CANTÃO": "Asia/Shanghai",
    "CARACAS": "America/Caracas", "CASTRIES": "America/St_Lucia", "CHICAGO": "America/Chicago",
    "CHUY": "America/Montevideo", "CIDADE DO CABO": "Africa/Johannesburg",
    "CIUDAD DEL ESTE": "America/Asuncion", "CIUDAD GUAYANA": "America/Caracas",
    "COBIJA": "America/La_Paz", "COCHABAMBA": "America/La_Paz", "COLOMBO": "Asia/Colombo",
    "CONACRI": "Africa/Conakry", "CONCEPCIÓN": "America/Asuncion",
    "COPENHAGUE": "Europe/Copenhagen", "COTONOU": "Africa/Porto-Novo",
    "CÓRDOBA": "America/Argentina/Cordoba", "DACAR": "Africa/Dakar", "DACCA": "Asia/Dhaka",
    "DAMASCO": "Asia/Damascus", "DAR ES SALAAM": "Africa/Dar_es_Salaam", "DOHA": "Asia/Qatar",
    "DUBLIN": "Europe/Dublin", "DÍLI": "Asia/Dili", "EDIMBURGO": "Europe/London",
    "ENCARNACIÓN": "America/Asuncion", "ESTOCOLMO": "Europe/Stockholm", "FARO": "Europe/Lisbon",
    "FRANKFURT": "Europe/Berlin", "GABORONE": "Africa/Gaborone", "GENEBRA": "Europe/Zurich",
    "GEORGETOWN": "America/Guyana", "GUATEMALA": "America/Guatemala",
    "HAMAMATSU": "Asia/Tokyo", "HANÓI": "Asia/Ho_Chi_Minh", "HARARE": "Africa/Harare",
    "HARTFORD": "America/New_York", "HAVANA": "America/Havana", "HELSINQUE": "Europe/Helsinki",
    "HONG KONG": "Asia/Hong_Kong", "HOUSTON": "America/Chicago", "IAUNDÊ": "Africa/Douala",
    "IEREVAN": "Asia/Yerevan", "IQUITOS": "America/Lima", "ISLAMABADE": "Asia/Karachi",
    "ISTAMBUL": "Europe/Istanbul", "JACARTA": "Asia/Jakarta", "KATMANDU": "Asia/Kathmandu",
    "KIEV": "Europe/Kyiv", "KINGSTON-JAMAICA": "America/Jamaica",
    "KINSHASA": "Africa/Kinshasa", "KUAITE": "Asia/Kuwait", "KUALA LUMPUR": "Asia/Kuala_Lumpur",
    "LA PAZ": "America/La_Paz", "LAGOS": "Africa/Lagos", "LIBREVILLE": "Africa/Libreville",
    "LILONGUE": "Africa/Blantyre", "LIMA": "America/Lima", "LISBOA": "Europe/Lisbon",
    "LIUBLIANA": "Europe/Ljubljana", "LOMÉ": "Africa/Lome", "LONDRES": "Europe/London",
    "LOS ANGELES": "America/Los_Angeles", "LUANDA": "Africa/Luanda", "LUSACA": "Africa/Lusaka",
    "MADRI": "Europe/Madrid", "MALABO": "Africa/Malabo", "MANILA": "Asia/Manila",
    "MANÁGUA": "America/Managua", "MAPUTO": "Africa/Maputo", "MARSELHA": "Europe/Paris",
    "MASCATE": "Asia/Muscat", "MENDOZA": "America/Argentina/Mendoza",
    "MEXICO": "America/Mexico_City", "MIAMI": "America/New_York", "MILÃO": "Europe/Rome",
    "MONTEVIDÉU": "America/Montevideo", "MONTREAL": "America/Toronto", "MOSCOU": "Europe/Moscow",
    "MUMBAI": "Asia/Kolkata", "MUNIQUE": "Europe/Berlin", "NAGÓIA": "Asia/Tokyo",
    "NAIRÓBI": "Africa/Nairobi", "NASSAU": "America/Nassau", "NICOSIA": "Asia/Nicosia",
    "NOVA DELHI": "Asia/Kolkata", "NOVA YORK": "America/New_York", "ORLANDO": "America/New_York",
    "OSLO": "Europe/Oslo", "OTTAWA": "America/Toronto", "PANAMA": "America/Panama",
    "PARAMARIBO": "America/Paramaribo", "PARIS": "Europe/Paris",
    "PASO LOS LIBRES": "America/Argentina/Buenos_Aires",
    "PEDRO JUAN CABALLERO": "America/Asuncion", "PEQUIM": "Asia/Shanghai",
    "PORT OF SPAIN": "America/Port_of_Spain", "PORTO": "Europe/Lisbon",
    "PORTO PRÍNCIPE": "America/Port-au-Prince", "PRAGA": "Europe/Prague",
    "PRAIA": "Atlantic/Cape_Verde", "PRETÓRIA": "Africa/Johannesburg",
    "PUERTO IGUAZÚ": "America/Argentina/Buenos_Aires", "PUERTO QUIJARRO": "America/La_Paz",
    "PYONGYANG": "Asia/Pyongyang", "QUITO": "America/Guayaquil", "RABAT": "Africa/Casablanca",
    "RAMALLAH": "Asia/Hebron", "RIADE": "Asia/Riyadh", "RIO BRANCO": "America/Montevideo",
    "RIVERA": "America/Montevideo", "ROMA": "Europe/Rome", "SAINT JOHNS": "America/Antigua",
    "SALTO DEL GUAIRÁ": "America/Asuncion", "SANTA CRUZ DE LA SIERRA": "America/La_Paz",
    "SANTA ELENA DE UAIRÉN": "America/Caracas", "SANTIAGO": "America/Santiago",
    "SARAJEVO": "Europe/Sarajevo", "SEUL": "Asia/Seoul", "SINGAPURA": "Asia/Singapore",
    "ST GEORGES DE LOYAPOCK": "America/Cayenne", "SYDNEY": "Australia/Sydney",
    "SÃO DOMINGOS": "America/Santo_Domingo", "SÃO FRANCISCO": "America/Los_Angeles",
    "SÃO JOSÉ": "America/Costa_Rica", "SÃO SALVADOR": "America/El_Salvador",
    "SÃO TOMÉ": "Africa/Sao_Tome", "SÓFIA": "Europe/Sofia", "TAIPÉ": "Asia/Taipei",
    "TALIN": "Europe/Tallinn", "TBILISI": "Asia/Tbilisi", "TEERÃ": "Asia/Tehran",
    "TEGUCIGALPA": "America/Tegucigalpa", "TEL AVIV": "Asia/Jerusalem", "TIRANA": "Europe/Tirane",
    "TORONTO": "America/Toronto", "TRÍPOLI": "Africa/Tripoli", "TUNIS": "Africa/Tunis",
    "TÓQUIO": "Asia/Tokyo", "UAGADUGU": "Africa/Ouagadougou", "VANCOUVER": "America/Vancouver",
    "VARSÓVIA": "Europe/Warsaw", "VIENA": "Europe/Vienna", "WASHINGTON": "America/New_York",
    "WELLINGTON": "Pacific/Auckland", "WINDHOEK": "Africa/Windhoek", "XANGAI": "Asia/Shanghai",
    "YANGON": "Asia/Yangon", "ZAGREB": "Europe/Zagreb", "ZURIQUE": "Europe/Zurich",
}


def utc_offset_seconds(city: str) -> int:
    """UTC offset in force at 12:00 local time on election day."""
    d, m, y = map(int, ELECTION_DATE.split("/"))
    noon = datetime(y, m, d, 12, tzinfo=ZoneInfo(CITY_TZ[city]))
    return int(noon.utcoffset().total_seconds())
