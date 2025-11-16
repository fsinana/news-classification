import os, requests, json, uuid
from datetime import datetime
from pathlib import Path

API_KEY = os.getenv("NEWS_API_KEY")
print("API_KEY set?:", bool(API_KEY))

url = "https://newsapi.org/v2/top-headlines"
params = {"q":"technology OR AI OR startup","language":"en","pageSize":5,"apiKey":API_KEY}
try:
    r = requests.get(url, params=params, timeout=12)
    print("HTTP", r.status_code)
    j = r.json()
    print("top-level keys:", list(j.keys()))
    articles = j.get("articles", [])
    print("articles count:", len(articles))
    out = Path("data/incoming"); out.mkdir(parents=True, exist_ok=True)
    for i,a in enumerate(articles[:5],1):
        title = a.get("title") or a.get("description") or ""
        print(f"{i}. {title}")
        record = {
            "id": str(uuid.uuid4()),
            "text": title,
            "source": (a.get("source") or {}).get("name"),
            "url": a.get("url"),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        fname = out / f"debug_{i}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
        with open(fname, "w", encoding="utf-8") as fh:
            json.dump(record, fh, ensure_ascii=False)
        print("wrote", fname)
except Exception as e:
    print("REQUEST ERROR:", e)
