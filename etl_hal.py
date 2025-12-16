# hal_morocco_struct_country.py
# pip install requests pandas pyarrow

import requests
import time
import pandas as pd

BASE = "https://api.hal.science/search/"

FL_FIELDS = [
    "docid", "label_s", "title_s", "abstract_s",
    "authFullName_s", "authAffiliation_s", "authOrganism_s",
    "structName_s", "structCity_s", "structCountry_s",
    "instStructName_s", "instStructCountry_s",
    "labStructName_s", "labStructCountry_s",
    "submittedDate_tdate", "publishedDateY_i", "doi_s"
]
FL = ",".join(FL_FIELDS)

def fetch_morocco_struct(rows=500, max_pages=2000):
    """
    Fetch HAL publications with structured country code 'ma' for Morocco.
    Falls back to text search only if needed.
    """
    # Use structCountry_s, instStructCountry_s, labStructCountry_s
    fq = '(structCountry_s:"ma" OR instStructCountry_s:"ma" OR labStructCountry_s:"ma")'

    cursor = "*"
    all_docs = []

    for page in range(max_pages):
        params = {
            "q": "*:*",       # all documents filtered by fq
            "fq": fq,
            "fl": FL,
            "wt": "json",
            "sort": "docid asc",
            "rows": rows,
            "cursorMark": cursor
        }

        r = requests.get(BASE, params=params, timeout=60)
        r.raise_for_status()
        j = r.json()

        docs = j["response"]["docs"]
        if not docs:
            break

        all_docs.extend(docs)
        next_cursor = j.get("nextCursorMark")
        if not next_cursor or next_cursor == cursor:
            break

        cursor = next_cursor
        print(f"Page {page+1} — {len(docs)} docs retrieved — total {len(all_docs)}")
        time.sleep(0.1)

    return all_docs


def save_results(docs):
    if not docs:
        print("No documents found.")
        return

    df = pd.json_normalize(docs)

    # Optional: remove duplicates by docid
    df.drop_duplicates(subset=['docid'], inplace=True)

    # Export
    df.to_json("hal_morocco.jsonl", orient="records", lines=True)
    df.to_json("hal_morocco.json", orient="records", indent=2)
    df.to_csv("hal_morocco.csv", index=False)
    try:
        df.to_parquet("hal_morocco.parquet", index=False)
    except Exception as e:
        print("Parquet export failed:", e)

    print("Extraction completed —", len(df), "documents")


if __name__ == "__main__":
    docs = fetch_morocco_struct()
    save_results(docs)
