import pandas as pd
from .config_sep2025 import PROCESSED_DIR

# Eingabedateien der bereits stündlich aufbereiteten Umwelt- und Verkehrsdaten.
UMWELT_WEEK = PROCESSED_DIR / "parkstrasse_umwelt_hourly_2025-09-08_bis_2025-09-14_sep2025.csv"
TRAFFIC_WEEK = PROCESSED_DIR / "parkstrasse_traffic_hourly_delta_pkw_sep2025.csv"

# Zieldatei des kombinierten Wochen-Datensatzes.
OUT_JOIN = PROCESSED_DIR / "parkstrasse_umwelt_traffic_hourly_2025-09-08_bis_2025-09-14_PKW_DELTA_sep2025.csv"

# Erwarteter Analysezeitraum der September-Woche auf Stundenbasis.
EXPECTED_START = pd.Timestamp("2025-09-08 00:00:00")
EXPECTED_END = pd.Timestamp("2025-09-14 23:00:00")

# Zielreihenfolge der wichtigsten Ausgabespalten. Zusätzliche Spalten bleiben erhalten und werden anschließend hinten angefügt.
ORDERED_BASE = [
    "datetime",
    "CO", "NO", "NO2", "O3", "PM10", "PM100", "PM25",
    "TA", "RH", "PA",
    "traffic_pkw_total", "traffic_pkw_avg_speed",
]


def _hard_assert(cond: bool, msg: str):
    """
    Bricht die Verarbeitung mit einer klaren Fehlermeldung ab,
    wenn eine erwartete Bedingung nicht erfüllt ist - sehr wichtig wg. Bugabfang.
    """
    if not cond:
        raise ValueError(msg)


def parse_datetime_best(series: pd.Series, label: str) -> pd.Series:
    """
    Es gibt verschiedene Arten und Typen von Parsern. Diese Variante hat sich am Besten herausgetellt:
    Parst Datumswerte robust für CSV-Importe, indem zwei mögliche Interpretationen getestet werden:
    - dayfirst=True
    - dayfirst=False

    Gewählt wird die Variante, die die meisten gültigen Werte innerhalb des erwarteten Analysefensters liefert.
    Anschließend werden die Zeitstempel auf volle Stunden gekürzt.
    """
    s = series.astype(str).str.strip()

    dt1 = pd.to_datetime(s, errors="coerce", dayfirst=True)
    dt2 = pd.to_datetime(s, errors="coerce", dayfirst=False)

    def score(dt: pd.Series) -> int:
        ok = dt.notna()
        if ok.sum() == 0:
            return -10_000

        # Höher bewertet werden Datumswerte,
        # die im erwarteten Analysefenster der betrachteten Woche liegen.
        in_window = ok & (dt >= EXPECTED_START) & (dt <= EXPECTED_END)
        return int(in_window.sum()) * 10 + int(ok.sum())

    sc1, sc2 = score(dt1), score(dt2)
    dt = dt1 if sc1 >= sc2 else dt2

    # Nicht interpretierbare Werte werden mit Beispielen gemeldet.
    bad = dt.isna().sum()
    if bad > 0:
        examples = s[dt.isna()].head(5).tolist()
        raise ValueError(f"{label}: {bad} datetime values could not be parsed. Examples: {examples}")

    dt = dt.dt.floor("h")
    return dt


def _validate_hourly_frame(df: pd.DataFrame, label: str):
    """
    Prüft, ob ein DataFrame den erwarteten vollständigen Wochenrahmen auf Stundenbasis erfüllt:
    - datetime-Spalte vorhanden
    - keine fehlenden Zeitstempel
    - keine doppelten Stunden
    - korrektes Start-/Enddatum
    - genau 168 Zeilen
    """
    _hard_assert("datetime" in df.columns, f"{label}: missing datetime column.")
    _hard_assert(df["datetime"].isna().sum() == 0, f"{label}: datetime contains NA.")
    _hard_assert(df["datetime"].duplicated().sum() == 0, f"{label}: duplicate datetimes found.")

    _hard_assert(df["datetime"].min() == EXPECTED_START, f"{label}: datetime min mismatch: {df['datetime'].min()} != {EXPECTED_START}")
    _hard_assert(df["datetime"].max() == EXPECTED_END, f"{label}: datetime max mismatch: {df['datetime'].max()} != {EXPECTED_END}")
    _hard_assert(len(df) == 168, f"{label}: expected 168 rows, got {len(df)}")


def main():
    """
    Lädt die stündlich aggregierten Umwelt- und Verkehrsdaten der September-Woche,
    validiert beide Datensätze,
    führt sie über die Stundenvariable 'datetime' zusammen
    und speichert den kombinierten Datensatz als CSV.
    """
    env = pd.read_csv(UMWELT_WEEK, sep=";", decimal=",")
    traf = pd.read_csv(TRAFFIC_WEEK, sep=";", decimal=",")

    # Datumswerte aus beiden Eingabedateien robust einlesen und auf Stundenbasis normieren.
    env["datetime"] = parse_datetime_best(env["datetime"], "ENV")
    traf["datetime"] = parse_datetime_best(traf["datetime"], "TRAF")

    # Doppelte Stundenwerte sollen in den Eingabedateien nicht vorkommen.
    env_dup = env["datetime"].duplicated().sum()
    traf_dup = traf["datetime"].duplicated().sum()
    _hard_assert(env_dup == 0, f"ENV: duplicate datetime rows in CSV: {env_dup}")
    _hard_assert(traf_dup == 0, f"TRAF: duplicate datetime rows in CSV: {traf_dup}")

    # Prüfen, ob beide Datenquellen den erwarteten vollständigen Wochenrahmen enthalten.
    _validate_hourly_frame(env, "ENV")
    _validate_hourly_frame(traf, "TRAF")

    # Inner Join auf Stundenbasis.
    # Da beide Tabellen bereits vollständig und konsistent 168 Stunden enthalten sollen,
    # wird hier bewusst ein inner join verwendet.
    joined = env.merge(traf, on="datetime", how="inner").sort_values("datetime")

    # Abschlussprüfung des Join-Ergebnisses.
    _hard_assert(len(joined) == 168, f"JOIN: expected 168 rows after merge, got {len(joined)}")
    _hard_assert(joined["datetime"].min() == EXPECTED_START, f"JOIN: datetime min mismatch: {joined['datetime'].min()} != {EXPECTED_START}")
    _hard_assert(joined["datetime"].max() == EXPECTED_END, f"JOIN: datetime max mismatch: {joined['datetime'].max()} != {EXPECTED_END}")
    _hard_assert(joined["datetime"].duplicated().sum() == 0, "JOIN: duplicate datetimes after merge.")

    # Gewünschte Spaltenreihenfolge herstellen.
    # Falls erwartete Spalten fehlen sollten, werden sie als NA ergänzt.
    for col in ORDERED_BASE:
        if col not in joined.columns:
            joined[col] = pd.NA

    ordered = ORDERED_BASE + [c for c in joined.columns if c not in ORDERED_BASE]
    joined = joined[ordered]

    joined.to_csv(OUT_JOIN, sep=";", decimal=",", index=False)

    print("Env rows:", len(env), "Traffic rows:", len(traf))
    print("Joined rows:", len(joined))
    print("Saved:", OUT_JOIN)


if __name__ == "__main__":
    main()