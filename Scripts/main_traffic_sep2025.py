import ijson
import pandas as pd
from collections import defaultdict
from datetime import datetime
from .config_sep2025 import PROCESSED_DIR, TRAFFIC_WEEK_JSON  

# Relevante Verkehrszählstellen entlang der Parkstraße.
# Es werden nur Datensätze dieser beiden Standorte berücksichtigt.
# Wichtiger Hinweis: PARK_LOCATIONS ist ein essenzieller Schritt, weil es zu Bugs kam bei einem Standort. Grund ist nicht bekannt.
# Teilweise funktioniert es auch mit einem Standort, jedoch nicht konsistent. Beim Filtern der zwei Kreuzungen kann man hierbei den Vekehrsdruck der Parkstraße besser abdecken.
PARK_LOCATIONS = {
    "Kreuzung Parkstraße / Zanderstraße",
    "Kreuzung Parkstraße / Friedrichstraße",
}


def is_parkstrasse(item):
    """
    Prüft, ob ein Verkehrsdatensatz zu einer der betrachteten Parkstraßen-Zählstellen gehört.
    """
    return item.get("locationName") in PARK_LOCATIONS


def main():
    # Letztbekannter Rohcounter pro Kombination aus Standort und Zone.
    # Diese Werte werden benötigt, um aus dem kumulativen Counter stündliche Deltas abzuleiten.
    last_counter = {}  # key -> last_raw_counter

    # Stündliche Aggregation:
    # hourly_count      = Summe der Fahrzeugdeltas pro Stunde
    # hourly_speed_num  = Zähler für den gewichteten Geschwindigkeitsmittelwert (delta * speed)
    # hourly_speed_den  = Nenner für den gewichteten Geschwindigkeitsmittelwert (Summe delta)
    hourly_count = defaultdict(int)       # hour -> sum(delta)
    hourly_speed_num = defaultdict(float) # hour -> sum(delta * speed)
    hourly_speed_den = defaultdict(int)   # hour -> sum(delta)

    # Streaming-Einlesen der JSON-Datei mit ijson,
    # damit auch größere Dateien speicherschonend verarbeitet werden können.
    with open(TRAFFIC_WEEK_JSON, "rb") as f:
        for item in ijson.items(f, "sensordata.item"):

            if not is_parkstrasse(item):
                continue

            # Es werden ausschließlich PKW-Datensätze verwendet.
            if item.get("type") != "Car":
                continue

            ts = item.get("timestamp")
            raw = item.get("counter")
            speed = item.get("avgSpeed")
            loc = item.get("locationName")
            zone = item.get("zoneId")

            # Unvollständige Datensätze werden übersprungen.
            if ts is None or raw is None or loc is None or zone is None:
                continue

            # Zeitstempel in ein datetime-Objekt umwandeln und anschließend auf volle Stundenbasis kürzen.
            dt = datetime.fromisoformat(ts.replace("Z", ""))
            hour = dt.replace(minute=0, second=0, microsecond=0)

            # Schlüssel zur Counter-Verfolgung je Standort und Zone.
            key = (loc, zone)

            # Delta-Berechnung des kumulativen Fahrzeugzählers. Beim ersten Auftreten ist noch kein Delta bestimmbar.
            # Falls der Counter sinkt, wird dies als Reset/Overflow/Drop behandelt.
            prev = last_counter.get(key)
            if prev is None:
                delta = 0  # erstes Auftreten: kein Delta ableitbar
            else:
                delta = raw - prev
                if delta < 0:
                    # Reset/Overflow/Drop -> als Reset behandeln
                    delta = raw

            last_counter[key] = raw

            # Nur positive Deltas fließen in die Stundenaggregation ein. Dies war auch erforderlich aus dem Bugfixing.
            if delta <= 0:
                continue

            # Stündliche Summierung der PKW-Anzahl.
            hourly_count[hour] += int(delta)

            # Gewichtete Aggregation der Durchschnittsgeschwindigkeit.
            # Gewichtet wird mit dem Delta, damit Stunden mit mehr Fahrzeugen stärker eingehen.
            if speed is not None:
                sp = float(speed)
                hourly_speed_num[hour] += delta * sp
                hourly_speed_den[hour] += int(delta)

    # Aufbau der Ausgabetabelle auf Stundenbasis.
    rows = []
    for hour in sorted(hourly_count.keys()):
        cnt = hourly_count[hour]

        if hourly_speed_den[hour] > 0:
            avg_speed = hourly_speed_num[hour] / hourly_speed_den[hour]
        else:
            avg_speed = None

        rows.append({
            "datetime": hour,
            "traffic_pkw_total": cnt,
            "traffic_pkw_avg_speed": avg_speed,
        })

    df = pd.DataFrame(rows)

    # Export der stündlich aggregierten Verkehrsdaten als CSV.
    out = PROCESSED_DIR / "parkstrasse_traffic_hourly_delta_pkw_sep2025.csv"
    df.to_csv(out, sep=";", decimal=",", index=False)

    print("Rows:", len(df))
    print("Saved:", out)


if __name__ == "__main__":
    main()