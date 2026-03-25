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
# Damit werden Messpunkte im räumlichen Umfeld der Parkstraße berücksichtigt (es gibt dafür unterschiedliche Herangehensweisen, in diesem Fall 30 Meter).
COORD_TOL = 0.0003

# Zielparameter der Umweltdaten im PT10S-Format.
# Diese Parameter werden im Umwelt-Skript eingelesen und anschließend auf Stundenbasis aggregiert.
PARAMS_PT10S_7 = [
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

# Standard-Datei für Umweltdaten, falls dem Skript keine Datei explizit übergeben wird (das ist ein statisches Verfahren, d.h. der Dateiname ist manuell gegeben).
DEFAULT_UMWELT_JSON = RAW_JSON_DIR / "sensordata_badnauheim_umwelt_2026-01-12_12-0-Uhr.json"

# Präfix für erzeugte Output-Dateien der Umweltverarbeitung.
OUTPUT_PREFIX = "parkstrasse_umwelt_hourly"

# Analysezeitraum des September-Datensatzes.
# Dieser Zeitraum bildet die betrachtete Woche für die Auswertung ab. Dieser Abschnitt war wg. Bugfixing von nöten.
WEEK_START = "2025-09-08"
WEEK_END = "2025-09-14"

# Präfix der Umweltdaten-Dateien.
# Die erwarteten Dateinamen folgen dem Muster:
# sensordata_environment_YYYY-MM-DD_00-12.json bzw. _12-00.json
ENV_PREFIX = "sensordata_environment_"

# JSON-Datei mit den Verkehrsdaten der betrachteten September-Woche.
TRAFFIC_WEEK_JSON = RAW_JSON_DIR / "sensordata_traffix_2025-09-08_2025-09-14.json"

# Textfilter für relevante Verkehrszählstellen in der Parkstraße.
# Der Abgleich erfolgt über das Feld "locationName".
TRAFFIC_PARK_NEEDLE = "Kreuzung Parkstraße"