import httpx
import polars as pl

API_KEY = "OSOegLs.PR2lwJ1dwCeje9vTj7FPOt3hvpYKtwKkhw"
CURRENT_ELECTION_PERIOD = 21
SELECTED_TYPES = ["Gesetzgebung", "Schriftliche Frage", "Mündliche Frage", "Große Anfrage", "Kleine Anfrage", "Antrag"]

def extract_tags(deskriptor):
    """Extrahiere fundstelle=True Namen"""
    return deskriptor.struct.unnest().filter(pl.col("fundstelle")).get_column("name").to_list()

def fetch(endpoint: str):
    c = httpx.Client(base_url="https://search.dip.bundestag.de/api/v1", headers={"Authorization": f"ApiKey {API_KEY}"})


    docs = []
    cursor = "*"
    while True:
        params={"f.wahlperiode": CURRENT_ELECTION_PERIOD, "cursor": cursor}
        r = c.get(endpoint, params=params).json()

        cursor = r["cursor"]

        if len(r["documents"]) == 0:
            break
        docs.extend(r["documents"])

    df = pl.DataFrame(docs).lazy()
    df = df.filter(pl.col("vorgangstyp").is_in(SELECTED_TYPES))
    
    return df

def get_gesetzgebung(df):
    beratungsstaende = [
        {"category": "In Bearbeitung", "staende": [
            'Dem Bundestag zugeleitet - Noch nicht beraten',
            'Dem Bundesrat zugeleitet - Noch nicht beraten',
            'Noch nicht beraten',
            'Einbringung beschlossen',
            'Den Ausschüssen zugewiesen',
            'Beschlussempfehlung liegt vor',
            '1. Durchgang im Bundesrat abgeschlossen',
            'Überwiesen'
        ]},
        {"category": "Fast geschafft", "staende": ['Verabschiedet', 'Bundesrat hat Vermittlungsausschuss nicht angerufen']},
        {"category": "Erfolgreich", "staende": [
            'Bundesrat hat zugestimmt',
            'Verkündet'
        ]},
        {"category": "Gescheitert", "staende": [
            'Abgelehnt',
            'Einbringung abgelehnt',
            'Bundesrat hat Zustimmung versagt',
            'Für mit dem Grundgesetz unvereinbar erklärt'
        ]},
        {"category": "Abgebrochen", "staende": [
            'Zurückgezogen',
            'Für erledigt erklärt', 
            'Erledigt durch Ablauf der Wahlperiode'
        ]},
        {"category": "Unklar", "staende": [
            'Zusammengeführt mit... (siehe Vorgangsablauf)', 
            'Abgeschlossen'
        ]}
    ]

    status_map = {}
    for item in beratungsstaende:
        for stand in item["staende"]:
            status_map[stand] = item["category"]

    return (
        df
        .filter(pl.col("vorgangstyp")=="Gesetzgebung")
        .with_columns(status=pl.col("beratungsstand").replace_strict(status_map, default="error"))
        .with_columns(
            tags=pl.col("deskriptor").map_elements(extract_tags, return_dtype=pl.List(pl.String))
        )
        .select(
            [
                "id",
                "beratungsstand",
                "status",
                "wahlperiode",
                "initiative",
                "sachgebiet",
                "titel",
                "datum", 
                "aktualisiert",
                "abstract",
                "tags"
            ]
        )
    )

def get_vorgangsposition(df: pl.LazyFrame, typ: str):
    relevant_fields = [
        "id",
        "vorgang_id",
        "vorgangsposition",
        "zuordnung",  # [ BT, BR, BV, EK ]
        "abstract",
        "vorgangstyp",
        "dokumentart",
        "datum",
        # structs
        "ueberweisung",
        "fundstelle",
        "aktivitaet_anzeige",
        "beschlussfassung"
    ]

    existing_fields = [f for f in relevant_fields if f in df.collect_schema().names()]
    return (
        df
        .filter(pl.col("vorgangstyp")==typ)
        .select(existing_fields)
        .sort("datum")
        .with_columns(
            pl.col("fundstelle").struct.field(["pdf_url", "dokumentnummer","drucksachetyp"]),
            # pl.col("beschlussfassung").list.eval(pl.element().struct.field("beschlusstenor")),
            # pl.col("beschlussfassung").list.eval(pl.element().struct.field("abstimmungsart")),
            # pl.col("beschlussfassung").list.eval(pl.element().struct.field("abstimm_ergebnis_bemerkung")),
            # pl.col("beschlussfassung").list.eval(pl.element().struct.field("mehrheit"))
            # pl.col("ueberweisung").list.eval(pl.element().struct.field("ausschuss")),
            # pl.col("aktivitaet_anzeige").list.eval(pl.element().struct.field("titel"))
        )
        .drop("fundstelle")
    )

def etl():
    df = fetch("vorgang")
    gesetzgebung_df = get_gesetzgebung(df)
    
    gesetzgebung_df.sink_parquet(f"db/gesetzgebung_{CURRENT_ELECTION_PERIOD}.parquet")

    df = fetch("vorgangsposition")
    df = get_vorgangsposition(df, "Gesetzgebung")
    df.sink_parquet(f"db/vorgangsposition_gesetzgebung_{CURRENT_ELECTION_PERIOD}.parquet")


if __name__ == "__main__":
    etl()