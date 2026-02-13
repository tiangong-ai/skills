#!/usr/bin/env python3
"""Fetch journal articles from Crossref and append to journals with DOI dedup."""

import argparse
import html
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from tenacity import retry, stop_after_attempt, wait_fixed

LOG_DIR_ENV = "KB_LOG_DIR"


def configure_logging():
    log_dir_raw = os.environ.get(LOG_DIR_ENV, "").strip()
    if not log_dir_raw:
        raise RuntimeError(f"Missing required environment variable: {LOG_DIR_ENV}")
    log_dir = Path(log_dir_raw)
    log_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    log_filename = f"crossref-{run_ts}.log"
    log_path = log_dir / log_filename

    logging.basicConfig(
        filename=str(log_path),
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
        force=True,
    )
    return log_path


@retry(wait=wait_fixed(3), stop=stop_after_attempt(30))
def make_request(url, params):
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


def fetch_journal_data(issn, journal_title, num_results, from_date):
    base_url = f"https://api.crossref.org/journals/{issn}/works"
    query_params = {
        "rows": num_results,
        "filter": f"type:journal-article,from-pub-date:{from_date}",
        "cursor": "*",
        "select": "title,DOI,container-title,author,issued",
    }

    all_papers = []

    while True:
        response = make_request(url=base_url, params=query_params)
        response_json = response.json()

        if response.status_code != 200 or "message" not in response_json:
            print("Error:", response.status_code, response_json)
            break

        message = response_json["message"]

        if "items" not in message:
            break

        items = message["items"]

        if not items:
            break

        found_match = False
        for item in items:
            item_journal_title = html.unescape(item.get("container-title", [""])[0])
            if item_journal_title.upper() == journal_title.upper():
                all_papers.append(item)
                found_match = True

        if not found_match:
            break

        query_params["cursor"] = message.get("next-cursor")

    return all_papers


def create_dataframe(papers, journal_title):
    data = []

    for paper in papers:
        title = paper.get("title", [""])[0]
        doi = paper.get("DOI", "")
        journal = journal_title.upper()
        raw_authors = paper.get("author") or []
        if not isinstance(raw_authors, list):
            raw_authors = []
        authors = [
            f"{author.get('given', '').strip()} {author.get('family', '').strip()}".strip()
            for author in raw_authors
            if isinstance(author, dict)
        ]
        authors = [author for author in authors if author]
        if not authors:
            # Skip papers that have no valid author names.
            continue
        issued = paper.get("issued", {})
        date_parts = issued.get("date-parts", [[None]])
        if len(date_parts[0]) == 1 and date_parts[0][0] is not None:
            year = date_parts[0][0]
            publication_date = "{:04d}".format(year)
        elif len(date_parts[0]) >= 2:
            year, month = date_parts[0][:2]
            publication_date = "{:04d}-{:02d}".format(year, month)
        else:
            publication_date = None

        data.append([title, doi, journal, authors, publication_date])

    df = pd.DataFrame(data, columns=["title", "doi", "journal", "authors", "date"])

    return df


def get_db_config():
    load_dotenv()
    required_keys = ["KB_DB_HOST", "KB_DB_PORT", "KB_DB_NAME", "KB_DB_USER", "KB_DB_PASSWORD"]
    missing = [key for key in required_keys if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"Missing database env variables: {', '.join(missing)}")

    return {
        "host": os.environ.get("KB_DB_HOST"),
        "port": os.environ.get("KB_DB_PORT"),
        "database": os.environ.get("KB_DB_NAME"),
        "user": os.environ.get("KB_DB_USER"),
        "password": os.environ.get("KB_DB_PASSWORD"),
    }


def create_engine_pg():
    cfg = get_db_config()
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    engine = create_engine(url)
    return engine


def insert_dataframe_to_existing_table(df, table_name, engine):
    df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")


def select_to_dataframe(table_name, conn, columns=None):
    df = pd.read_sql_table(table_name, conn, columns=columns)
    return df


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch journal articles from Crossref and append to the journals table."
    )
    parser.add_argument(
        "--from-date",
        required=True,
        help="Earliest publication date to fetch from Crossref (e.g. 2024-05-01).",
    )
    return parser.parse_args()


def main():
    load_dotenv()
    log_path = configure_logging()

    args = parse_args()
    rows_per_journal = 1000

    db_cfg = get_db_config()
    logging.info("Log file: %s", log_path)

    engine = create_engine_pg()

    with engine.begin() as conn:
        df_issn = select_to_dataframe("journals_issn", conn, ["journal", "issn1"])

    engine.dispose()

    df_issn = df_issn.sort_values(by="journal")

    print(df_issn)

    conn_pg = psycopg2.connect(
        database=db_cfg["database"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        host=db_cfg["host"],
        port=db_cfg["port"],
    )

    insert_query = (
        "INSERT INTO journals (title, doi, journal, authors, date) "
        "VALUES %s ON CONFLICT (doi) DO NOTHING RETURNING 1"
    )
    total_inserted = 0

    for _, issn_row in df_issn.iterrows():
        papers = fetch_journal_data(
            issn_row["issn1"],
            issn_row["journal"],
            rows_per_journal,
            args.from_date,
        )
        df = create_dataframe(papers, issn_row["journal"])

        if df.empty:
            logging.info("No new papers found for %s", issn_row["journal"])
            continue

        with conn_pg.cursor() as cur:
            data_to_insert = []
            for _, paper_row in df.iterrows():
                authors = paper_row["authors"]
                if authors is None or (isinstance(authors, float) and pd.isna(authors)):
                    continue
                data_to_insert.append(
                    (
                        paper_row["title"],
                        paper_row["doi"],
                        paper_row["journal"],
                        psycopg2.extras.Json(authors),
                        paper_row["date"],
                    )
                )

            if not data_to_insert:
                logging.info("No insertable papers (all missing authors) for %s", issn_row["journal"])
                continue
            inserted_rows = psycopg2.extras.execute_values(
                cur, insert_query, data_to_insert, template=None, page_size=1000, fetch=True
            )
            inserted_count = len(inserted_rows)

        total_inserted += inserted_count
        logging.info("Inserted %s papers for %s", inserted_count, issn_row["journal"])
        conn_pg.commit()

    time_range_end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logging.info("此次更新%s条，时间范围是%s 至 %s", total_inserted, args.from_date, time_range_end)

    conn_pg.close()


if __name__ == "__main__":
    main()
