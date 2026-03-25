import json
from datetime import datetime, date, timedelta
from collections import defaultdict

import pandas as pd

from .config_dez2025 import (
    RAW_JSON_DIR, PROCESSED_DIR,
    PARK_LAT, PARK_LON, COORD_TOL,
    PARAMS_PT10S,
    WEEK_START, WEEK_END,
    ENV_PREFIX, ENV_SUFFIXES,
    TAG,
)

# Die meteorologischen Basisvariablen werden bewusst direkt hinter die Schadstoff-/PM-Werte gesetzt.
ORDERED_COLS = [
    "datetime",
    "CO", "NO", "NO2", "O3", "PM10", "PM100", "PM25",
    "TA", "RH", "PA",
]

# Erwartetes vollständiges Zeitfenster der Dezember-Woche auf Stundenbasis.
# Dieses Intervall wird später für den Reindex verwendet, damit die Ausgabe genau 168 Stunden umfasst.
EXPECTED_START = pd.Timestamp(f"{WEEK_START} 00:00:00")
EXPECTED_END = pd.Timestamp(f"{WEEK_END} 23:00:00")


def is_parkstrasse(d: dict) -> bool:
    """
    Prüft, ob ein Datensatz räumlich dem betrachteten Standort in der Parkstraße zugeordnet werden kann.
    Der Abgleich erfolgt über eine Koordinatentoleranz um die Referenzposition.
    """
    lat = d.get("lat")
    lon = d.get("lon")
    if lat is None or lon is None:
        return False
    return abs(lat - PARK_LAT) <= COORD_TOL and abs(lon - PARK_LON) <= COORD_TOL


def daterange(d0: date, d1: date):
    """
    Erzeugt alle Kalendertage zwischen Start- und Enddatum einschließlich.
    Wird genutzt, um die erwarteten Tagesdateien der Woche systematisch zu durchlaufen.
    """
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)


def _hard_assert(cond: bool, msg: str):
    """
    Bricht die Verarbeitung mit einer klaren Fehlermeldung ab,
    wenn eine erwartete Bedingung nicht erfüllt ist.
    """
    if not cond:
        raise ValueError(msg)


def process_one_env_file(path) -> pd.DataFrame:
    """
    Liest eine einzelne Umwelt-JSON-Datei eines 8-Stunden-Blocks ein,
    filtert auf den Standort Parkstraße und die gewünschten PT10S-Parameter
    und aggregiert die Werte stündlich.

    Rückgabe:
        DataFrame im Wide-Format mit einer Zeile pro Stunde und einer Spalte pro Parameter.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("sensordata", [])

    # Filter auf den relevanten Standort und die fachlich vorgesehenen PT10S-Parameter.
    park = [d for d in records if is_parkstrasse(d) and d.get("parameter") in PARAMS_PT10S]

    # Sammelstruktur für die stündliche Aggregation:
    # Schlüssel = (Stunde, Parameter), Wert = Liste aller Messwerte innerhalb dieser Stunde.
    buckets = defaultdict(list)

    for d in park:
        ts = d.get("timestamp")
        val = d.get("value")
        param = d.get("parameter")

        # Unvollständige Datensätze werden übersprungen.
        if ts is None or val is None or param is None:
            continue

        # Zeitstempel parsen.
        # Das "Z" wird entfernt, damit datetime.fromisoformat den Wert direkt verarbeiten kann.
        try:
            dt = datetime.fromisoformat(ts.replace("Z", ""))
        except Exception as e:
            raise ValueError(f"ENV: timestamp parse failed: {ts} in {path.name}") from e

        # Aggregation auf volle Stundenbasis durch Abschneiden von Minuten/Sekunden.
        hour = dt.replace(minute=0, second=0, microsecond=0)

        # Nur numerische Werte können in die Mittelwertbildung eingehen.
        # Nicht numerische Werte werden bewusst übersprungen.
        try:
            v = float(val)
        except Exception:
            continue

        buckets[(hour, param)].append(v)

    # Umwandlung der gesammelten Stundenwerte in Zeilen mit stündlichem Mittelwert je Parameter.
    rows = []
    for (hour, param), vals in buckets.items():
        rows.append({
            "datetime": pd.Timestamp(hour),
            "parameter": param.replace("_PT10S", ""),
            "value": sum(vals) / len(vals),
        })

    # Falls keine relevanten Datensätze gefunden wurden, wird ein leeres DataFrame mit datetime-Spalte zurückgegeben.
    if not rows:
        return pd.DataFrame(columns=["datetime"])

    df_long = pd.DataFrame(rows)

    # Umwandlung vom Long- ins Wide-Format:
    # pro Stunde eine Zeile, pro Parameter eine eigene Spalte.
    df_wide = (
        df_long.pivot(index="datetime", columns="parameter", values="value")
        .reset_index()
        .sort_values("datetime")
    )

    return df_wide


def _reindex_to_full_week(df: pd.DataFrame) -> pd.DataFrame:
    """
    Erzwingt ein vollständiges Stundenraster für die gesamte Analysewoche.
    Fehlende Stunden bleiben als NA erhalten.
    Das ist im Dezember-Datensatz wichtig, da Ausfälle im Messzeitraum sichtbar bleiben sollen.
    """
    full_range = pd.date_range(EXPECTED_START, EXPECTED_END, freq="h")

    _hard_assert(df["datetime"].isna().sum() == 0, "ENV: datetime contains NA after processing.")

    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="raise").dt.floor("h")

    # Vor dem Reindex dürfen keine doppelten Stundenwerte vorhanden sein.
    dup = df["datetime"].duplicated().sum()
    _hard_assert(dup == 0, f"ENV: found {dup} duplicate datetimes before reindex (should be 0).")

    df = df.set_index("datetime").reindex(full_range).reset_index().rename(columns={"index": "datetime"})

    # Harte Konsistenzprüfungen für den erwarteten Wochenrahmen.
    _hard_assert(len(df) == 168, f"ENV: expected 168 rows after reindex, got {len(df)}.")
    _hard_assert(df["datetime"].min() == EXPECTED_START, f"ENV: datetime min mismatch: {df['datetime'].min()} != {EXPECTED_START}")
    _hard_assert(df["datetime"].max() == EXPECTED_END, f"ENV: datetime max mismatch: {df['datetime'].max()} != {EXPECTED_END}")

    return df


def _apply_column_order(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stellt die gewünschte Spaltenreihenfolge sicher.
    Fehlende erwartete Spalten werden als NA ergänzt, damit die Ausgabe konsistent bleibt.
    """
    df = df.copy()

    # Erwartete Standardspalten ergänzen, falls sie im Zeitraum nicht vorhanden sind.
    for col in ORDERED_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # Eventuell zusätzlich vorhandene Spalten bleiben erhalten und werden hinten angehängt.
    ordered = ORDERED_COLS + [c for c in df.columns if c not in ORDERED_COLS]
    return df[ordered]


def _print_na_report(df: pd.DataFrame):
    """
    Gibt einen kompakten Überblick über fehlende Werte in den relevanten Messspalten aus.
    """
    na_counts = df.isna().sum()
    cols = [c for c in ORDERED_COLS if c != "datetime"]
    print("NA report (selected):")
    for c in cols:
        print(f"  {c:>6}: {int(na_counts.get(c, 0))}")


def main():
    """
    Lädt alle Umweltdateien der Dezember-Woche,
    aggregiert sie auf Stundenbasis,
    vereinheitlicht das Wochenraster
    und speichert das Ergebnis als CSV.
    """
    d0 = date.fromisoformat(WEEK_START)
    d1 = date.fromisoformat(WEEK_END)

    parts = []
    for d in daterange(d0, d1):
        # In der Dezember-Lieferung liegt jeder Tag in drei 8-Stunden-Blöcken vor.
        for suffix in ENV_SUFFIXES:
            fname = f"{ENV_PREFIX}{d.isoformat()}_{suffix}.json"
            path = RAW_JSON_DIR / fname

            # Alle erwarteten Teil-Dateien müssen vorhanden sein.
            if not path.exists():
                raise FileNotFoundError(f"ENV: missing required file: {path}")

            df = process_one_env_file(path)
            print("OK:", fname, "rows:", len(df))
            parts.append(df)

    df_week = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    _hard_assert(not df_week.empty, "ENV: no data loaded at all. Check input files / filters.")

    # Falls Stunden durch mehrere Teil-Dateien mehrfach vorkommen,
    # werden sie auf Stundenebene nochmals zusammengeführt.
    df_week["datetime"] = pd.to_datetime(df_week["datetime"], errors="raise").dt.floor("h")
    df_week = df_week.groupby("datetime", as_index=False).mean(numeric_only=True).sort_values("datetime")

    # Vollständiges Wochenraster mit 168 Stunden erzeugen; fehlende Stunden bleiben als NA bestehen.
    df_week = _reindex_to_full_week(df_week)

    # Konsistente Spaltenreihenfolge herstellen und fehlende erwartete Spalten ergänzen.
    df_week = _apply_column_order(df_week)

    # Abschließende Prüfung, ob alle erforderlichen Ausgabespalten vorhanden sind.
    required = ["datetime", "CO", "NO", "NO2", "O3", "PM10", "PM100", "PM25", "TA", "RH", "PA"]
    missing = [c for c in required if c not in df_week.columns]
    _hard_assert(len(missing) == 0, f"ENV: required columns missing after processing: {missing}")

    out = PROCESSED_DIR / f"parkstrasse_umwelt_hourly_{WEEK_START}_bis_{WEEK_END}_{TAG}.csv"
    df_week.to_csv(out, sep=";", decimal=",", index=False)

    # Kurze Konsolenausgabe zur Plausibilitätskontrolle nach dem Lauf.
    print("Rows total:", len(df_week))
    print("Datetime min/max:", df_week["datetime"].min(), df_week["datetime"].max())
    _print_na_report(df_week)
    print("Saved:", out)


if __name__ == "__main__":
    main()