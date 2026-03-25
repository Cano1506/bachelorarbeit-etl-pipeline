import pandas as pd
from .config_dez2025 import PROCESSED_DIR

# Eingabedateien der bereits stündlich aufbereiteten Umwelt- und Verkehrsdaten.
UMWELT_WEEK = PROCESSED_DIR / "parkstrasse_umwelt_hourly_2025-12-06_bis_2025-12-12_dez2025.csv"
TRAFFIC_WEEK = PROCESSED_DIR / "parkstrasse_traffic_hourly_delta_pkw_dez2025.csv"

# Zieldatei des kombinierten Wochen-Datensatzes.
OUT_JOIN = PROCESSED_DIR / "parkstrasse_umwelt_traffic_hourly_2025-12-06_bis_2025-12-12_PKW_DELTA_dez2025.csv"

# Erwarteter Analysezeitraum der Dezember-Woche auf Stundenbasis.
EXPECTED_START = pd.Timestamp("2025-12-06 00:00:00")
EXPECTED_END   = pd.Timestamp("2025-12-12 23:00:00")


def parse_datetime(series: pd.Series, label: str) -> pd.Series:
    """
    Parst die Datumswerte einer CSV-Spalte und kürzt sie auf volle Stunden.
    Nicht interpretierbare Werte werden mit Beispielwerten gemeldet.
    """
    dt = pd.to_datetime(series, errors="coerce")

    bad = dt.isna().sum()
    if bad > 0:
        examples = series[dt.isna()].head(5).tolist()
        raise ValueError(
            f"{label}: {bad} datetime values could not be parsed. Examples: {examples}"
        )

    dt = dt.dt.floor("h")
    return dt


def main():
    """
    Lädt die stündlich aggregierten Umwelt- und Verkehrsdaten der Dezember-Woche,
    prüft die Eingabedateien,
    führt beide Datensätze über 'datetime' zusammen
    und speichert das Ergebnis als CSV.
    """
    # Prüfen, ob beide erwarteten Eingabedateien vorhanden sind.
    if not UMWELT_WEEK.exists():
        raise FileNotFoundError(f"ENV file not found: {UMWELT_WEEK}")
    if not TRAFFIC_WEEK.exists():
        raise FileNotFoundError(f"TRAFFIC file not found: {TRAFFIC_WEEK}")

    env = pd.read_csv(UMWELT_WEEK, sep=";", decimal=",")
    traf = pd.read_csv(TRAFFIC_WEEK, sep=";", decimal=",")

    # Datumswerte beider Eingabedateien einlesen und auf Stundenbasis normieren.
    env["datetime"] = parse_datetime(env["datetime"], "ENV")
    traf["datetime"] = parse_datetime(traf["datetime"], "TRAF")

    # Erwartet werden vollständige Wochen-Datensätze mit jeweils 168 Stundenwerten.
    assert len(env) == 168, f"ENV rows != 168 ({len(env)})"
    assert len(traf) == 168, f"TRAF rows != 168 ({len(traf)})"

    assert env["datetime"].min() == EXPECTED_START
    assert env["datetime"].max() == EXPECTED_END
    assert traf["datetime"].min() == EXPECTED_START
    assert traf["datetime"].max() == EXPECTED_END

    # Eventuelle doppelte Zeitstempel werden vor dem Join auf Stundenebene zusammengefasst.
    # Für Umweltdaten erfolgt dies über den Mittelwert numerischer Spalten.
    env = env.groupby("datetime", as_index=False).mean(numeric_only=True)

    # Für Verkehrsdaten wird die Fahrzeuganzahl summiert,
    # die Durchschnittsgeschwindigkeit jedoch gemittelt.
    traf = traf.groupby("datetime", as_index=False).agg({
        "traffic_pkw_total": "sum",
        "traffic_pkw_avg_speed": "mean",
    })

    # Zusammenführung beider Datensätze auf Stundenbasis.
    joined = (
        env.merge(traf, on="datetime", how="inner")
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    # Abschlussprüfung des Join-Ergebnisses.
    assert len(joined) == 168, f"JOIN rows != 168 ({len(joined)})"
    assert joined["datetime"].duplicated().sum() == 0

    joined.to_csv(OUT_JOIN, sep=";", decimal=",", index=False)

    print("ENV rows:", len(env))
    print("TRAF rows:", len(traf))
    print("JOIN rows:", len(joined))
    print("Saved:", OUT_JOIN)


if __name__ == "__main__":
    main()