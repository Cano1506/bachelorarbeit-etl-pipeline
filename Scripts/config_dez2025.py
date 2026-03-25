from pathlib import Path

# Projektwurzel relativ zur aktuellen Datei.
# Erwartet wird, dass sich dieses Skript in einem Unterordner des Projektverzeichnisses befindet (In diesem Fall unter raw_json).
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Standard-Verzeichnisse für Rohdaten und verarbeitete Ausgabedateien.
RAW_JSON_DIR = PROJECT_ROOT / "raw_json"
PROCESSED_DIR = PROJECT_ROOT / "processed"

# Referenzkoordinaten der Messposition in der Parkstraße.
# Diese Koordinaten werden verwendet, um Umweltdaten räumlich auf den relevanten Standort zu filtern (ermittelt aus Google Maps).
PARK_LAT = 50.365545
PARK_LON = 8.742600

# Toleranz für den Koordinatenfilter in Grad.
# Damit werden Messpunkte im räumlichen Umfeld der Parkstraße berücksichtigt. (es gibt dafür unterschiedliche Herangehensweisen, in diesem Fall 30 Meter).
COORD_TOL = 0.0003

# Zielparameter der Umweltdaten im PT10S-Format.
# Diese Parameter werden im Umwelt-Skript eingelesen und anschließend auf Stundenbasis aggregiert.
PARAMS_PT10S = [
    "CO_PT10S",
    "O3_PT10S",
    "NO2_PT10S",
    "NO_PT10S",
    "PM10_PT10S",
    "PM25_PT10S",
    "PM100_PT10S",
    # Basis-Meteorologie, PA war in den Auwertungen nicht von Relevanz, jedoch zur Vollständigkeit hinzugezogen.
    "TA_PT10S",
    "RH_PT10S",
    "PA_PT10S",
]

# Analysezeitraum des Dezember-Datensatzes.
# Dieser Zeitraum bildet die betrachtete Woche der zweiten Lieferung ab.
WEEK_START = "2025-12-06"
WEEK_END = "2025-12-12"

# Präfix der Umweltdaten-Dateien.
ENV_PREFIX = "sensordata_environment_"

# Erwartete Zeitscheiben pro Tag in der Dezember-Lieferung.
# Die Umweltdateien liegen hier in drei Teildateien pro Tag vor.
ENV_SUFFIXES = ("00-08", "08-16", "16-00")

# JSON-Datei mit den Verkehrsdaten der betrachteten Dezember-Woche.
TRAFFIC_WEEK_JSON = RAW_JSON_DIR / "sensordata_traffic_2025-12-06_2025-12-12.json"

# Kennzeichnung für Output-Dateien der Dezember-Verarbeitung,
# damit Ergebnisse nicht mit anderen Zeiträumen überschrieben werden.
TAG = "dez2025"