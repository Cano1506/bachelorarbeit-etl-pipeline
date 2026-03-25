import ijson
import pandas as pd
from collections import defaultdict
from datetime import datetime

from .config_dez2025 import PROCESSED_DIR, TRAFFIC_WEEK_JSON

# Relevante Verkehrszählstellen entlang der Parkstraße.
# Es werden nur Datensätze dieser beiden Standorte berücksichtigt.
# Wichtiger Hinweis: PARK_LOCATIONS ist ein essenzieller Schritt, weil es zu Bugs kam bei einem Standort. Grund ist nicht bekannt.
# Teilweise funktioniert es auch mit einem Standort, jedoch nicht konsistent. Beim Filtern der zwei Kreuzungen kann man hierbei den Vekehrsdruck der Parkstraße besser abdecken.
PARK_LOCATIONS = {
    "Kreuzung Parkstraße / Zanderstraße",
    "Kreuzung Parkstraße / Friedrichstraße",
}

# Erwarteter vollständiger Analysezeitraum der Dezember-Woche auf Stundenbasis.
EXPECTED_START = pd.Timestamp("2025-12-06 00:00:00")
EXPECTED_END   = pd.Timestamp("2025-12-12 23:00:00")


def is_parkstrasse(item: dict) -> bool:
    """
    Prüft, ob ein Verkehrsdatensatz zu einer der betrachteten Parkstraßen-Zählstellen gehört.
    """
    return item.get("locationName") in PARK_LOCATIONS


def main():
    # Sicherstellen, dass die erwartete Eingabedatei vorhanden ist.
    if not TRAFFIC_WEEK_JSON.exists():
        raise FileNotFoundError(f"TRAF: input JSON not found: {TRAFFIC_WEEK_JSON}")

    # Letztbekannter Rohcounter pro Kombination aus Standort und Zone.
    # Diese Werte werden benötigt, um aus dem kumulativen Counter stündliche Deltas abzuleiten.
    last_counter = {}  # (location, zone) -> last raw counter

    # Stündliche Aggregation:
    # hourly_count      = Summe der Fahrzeugdeltas pro Stunde
    # hourly_speed_num  = Zähler für den gewichteten Geschwindigkeitsmittelwert (delta * speed)
    # hourly_speed_den  = Nenner für den gewichteten Geschwindigkeitsmittelwert (Summe delta)
    hourly_count = defaultdict(int)
    hourly_speed_num = defaultdict(float)
    hourly_speed_den = defaultdict(int)

    # Streaming-Einlesen der JSON-Datei mit ijson,
    # damit auch größere Dateien speicherschonend verarbeitet werden können.
    with open(TRAFFIC_WEEK_JSON, "rb") as f:
        for item in ijson.items(f, "sensordata.item"):
            # Nur die definierten Parkstraßen-Standorte berücksichtigen.
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

            # Counter-Wert in int umwandeln; nicht auswertbare Werte werden verworfen.
            try:
                raw = int(raw)
            except Exception:
                continue

            # Zeitstempel parsen und auf volle Stundenbasis kürzen.
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", ""))
            except Exception as e:
                raise ValueError(f"TRAF: timestamp parse failed: {ts}") from e

            hour = dt.replace(minute=0, second=0, microsecond=0)

            # Schlüssel zur Counter-Verfolgung je Standort und Zone.
            key = (loc, zone)

            # Delta-Berechnung des kumulativen Fahrzeugzählers.
            # Beim ersten Auftreten ist noch kein Delta bestimmbar.
            # Falls der Counter sinkt, wird dies als Reset/Overflow/Drop behandelt.
            prev = last_counter.get(key)
            if prev is None:
                delta = 0
            else:
                delta = raw - prev
                if delta < 0:
                    # Reset/Overflow/Drop
                    delta = raw

            last_counter[key] = raw

            # Nur positive Deltas fließen in die Stundenaggregation ein.
            if delta <= 0:
                continue

            # Stündliche Summierung der PKW-Anzahl.
            hourly_count[hour] += int(delta)

            # Gewichtete Aggregation der Durchschnittsgeschwindigkeit.
            # Gewichtet wird mit dem Delta, damit Stunden mit mehr Fahrzeugen stärker eingehen.
            if speed is not None:
                try:
                    sp = float(speed)
                    hourly_speed_num[hour] += delta * sp
                    hourly_speed_den[hour] += int(delta)
                except Exception:
                    pass

    # Aufbau eines vollständigen 168-Stunden-Rasters für die Analysewoche.
    # Fehlende Stunden werden explizit mit 0 Fahrzeugen bzw. NA für die Geschwindigkeit geführt.
    full_range = pd.date_range(EXPECTED_START, EXPECTED_END, freq="h")

    rows = []
    for hour in full_range:
        h = hour.to_pydatetime()  # dict-Keys liegen als naive datetime-Objekte vor
        cnt = int(hourly_count.get(h, 0))
        den = int(hourly_speed_den.get(h, 0))

        if den > 0:
            avg_speed = hourly_speed_num[h] / den
        else:
            avg_speed = pd.NA

        rows.append({
            "datetime": hour,
            "traffic_pkw_total": cnt,
            "traffic_pkw_avg_speed": avg_speed,
        })

    df = pd.DataFrame(rows)

    # Konsistenzprüfungen für den erwarteten Wochenrahmen.
    assert len(df) == 168, f"Expected 168 rows, got {len(df)}"
    assert df["datetime"].min() == EXPECTED_START, f"Datetime min mismatch: {df['datetime'].min()} != {EXPECTED_START}"
    assert df["datetime"].max() == EXPECTED_END, f"Datetime max mismatch: {df['datetime'].max()} != {EXPECTED_END}"
    assert df["datetime"].duplicated().sum() == 0, "Duplicate datetimes found."

    # Exportvorbereitung für eine stabile Weiterverarbeitung, z. B. in Excel.
    # Die Geschwindigkeitswerte werden numerisch erzwungen und auf 6 Nachkommastellen gerundet.
    df["traffic_pkw_avg_speed"] = pd.to_numeric(df["traffic_pkw_avg_speed"], errors="coerce").round(6)

    out = PROCESSED_DIR / "parkstrasse_traffic_hourly_delta_pkw_dez2025.csv"
    df.to_csv(out, sep=";", decimal=",", index=False)

    print("Rows:", len(df))
    print("Datetime min/max:", df["datetime"].min(), df["datetime"].max())
    print("Saved:", out)


if __name__ == "__main__":
    main()