# news_producer.py (improved)
import os
import time
import json
import logging
import requests
import uuid
from datetime import datetime
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# CONFIG
API_KEY = os.getenv("NEWS_API_KEY")  # read from environment; do NOT hardcode
if not API_KEY:
    logging.error("NEWS_API_KEY not set. Set NEWS_API_KEY in your environment and restart.")
    raise SystemExit(1)

QUERY = os.getenv("QUERY", "technology")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
OUT_DIR = Path("data/incoming")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# requests session with retries
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"])
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_headlines():
    """
    Try /everything first (wide search), then fall back to /top-headlines.
    Returns a list of article dicts (may be empty).
    """
    url_every = "https://newsapi.org/v2/everything"
    url_top = "https://newsapi.org/v2/top-headlines"
    base_params = {"language": "en", "pageSize": 20, "apiKey": API_KEY}

    try:
        p = base_params.copy()
        p.update({"q": QUERY, "sortBy": "publishedAt"})
        resp = session.get(url_every, params=p, timeout=12)
        if resp.status_code == 429:
            # hit rate limit — log and return empty so producer backs off
            logging.warning("Rate limited by /everything (429).")
            return []
        resp.raise_for_status()
        data = resp.json()
        articles = data.get("articles", [])
        if articles:
            logging.debug("Fetched %d articles from /everything", len(articles))
            return articles
    except Exception as ex:
        logging.warning("Error fetching /everything: %s", ex)

    # Fallback to top-headlines
    try:
        resp = session.get(url_top, params=base_params, timeout=12)
        if resp.status_code == 429:
            logging.warning("Rate limited by /top-headlines (429).")
            return []
        resp.raise_for_status()
        articles = resp.json().get("articles", [])
        logging.debug("Fetched %d articles from /top-headlines", len(articles))
        return articles
    except Exception as ex:
        logging.error("Error fetching /top-headlines: %s", ex)
        return []

def _make_record(headline_text, source_obj, url):
    # Try to include source.name and source.id (if available)
    if isinstance(source_obj, dict):
        source_name = source_obj.get("name")
        source_id = source_obj.get("id")
        source = source_name or source_id or None
    else:
        source = source_obj

    # defensive truncation: keep headlines reasonable length
    if headline_text and len(headline_text) > 3000:
        headline_text = headline_text[:3000]

    ts = datetime.utcnow().isoformat() + "Z"
    return {
        "id": str(uuid.uuid4()),
        "text": headline_text,
        "source": source,
        "url": url,
        "timestamp": ts
    }

def write_json_record(headline_text, source, url):
    """
    Write one JSON file (atomic rename). Uses a .tmp file then os.replace to move into final filename.
    """
    record = _make_record(headline_text, source, url)
    tmp_fname = OUT_DIR / f".tmp_{uuid.uuid4().hex}.json"
    final_fname = OUT_DIR / f"news_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
    try:
        with open(tmp_fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_fname, final_fname)  # atomic on most OSes if same filesystem
        logging.info("Wrote: %s", final_fname)
    except Exception as ex:
        logging.error("Failed to write record %s: %s", final_fname, ex)
        # attempt cleanup of tmp file if exists
        try:
            if tmp_fname.exists():
                tmp_fname.unlink()
        except Exception:
            pass

# Optional: batch writer (commented). This reduces number of small files.
# def write_batch(records):
#     ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
#     tmp = OUT_DIR / f".tmp_batch_{uuid.uuid4().hex}.json"
#     final = OUT_DIR / f"news_batch_{ts}_{uuid.uuid4().hex[:8]}.json"
#     try:
#         with open(tmp, "w", encoding="utf-8") as f:
#             json.dump(records, f, ensure_ascii=False)
#             f.flush(); os.fsync(f.fileno())
#         os.replace(tmp, final)
#         logging.info("Wrote batch file: %s", final)
#     except Exception as ex:
#         logging.error("Failed to write batch file: %s", ex)
#         try:
#             if tmp.exists():
#                 tmp.unlink()
#         except Exception:
#             pass

def main():
    logging.info("Starting news producer. Press Ctrl+C to stop.")
    while True:
        try:
            data = fetch_headlines()

            if isinstance(data, dict):
                articles = data.get("articles", [])[:10]
            elif isinstance(data, list):
                articles = data[:10]
            else:
                articles = []

            for a in articles:
                headline = (a.get("title") or a.get("description") or "").strip()
                src = a.get("source")

                # normalize source to string: prefer name > id
                if isinstance(src, dict):
                    source_name = src.get("name")
                    source_id = src.get("id")
                    source = source_name or source_id or None
                else:
                    source = src

                url = a.get("url")

                if headline:
                    write_json_record(headline, source, url)

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            logging.info("Stopping producer.")
            break

        except Exception as e:
            logging.error("Producer error: %s", e)
            time.sleep(10)

if __name__ == "__main__":
    main()
