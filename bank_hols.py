import psycopg2
import requests


def get_bank_hols() -> pd.DataFrame:
    all_hols = []
    for year in range(2012, 2023):
        hols = requests.get(f"https://date.nager.at/api/v3/PublicHolidays/{year}/GB")
        hols_json = hols.json()
        eng_hols = [hol for hol in hols_json if hol["counties"] is None or "GB-ENG" in hol["counties"]]
        all_hols.extend(eng_hols)
    return all_hols


def persist_hols():
    hols = get_bank_hols()
    conn = psycopg2.connect(dbname="cyclehire")
    with conn.cursor() as cur:
        cur.executemany(
            """
                INSERT INTO bank_hols (date, name) 
                VALUES
                (%s, %s)
                ON CONFLICT (date) DO NOTHING
        """,
            [(hol["date"], hol["localName"]) for hol in hols],
        )
    conn.commit()
