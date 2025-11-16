import os, requests, json, uuid
from datetime import datetime
from pathlib import Path

API_KEY = os.getenv("NEWS_API_KEY")
QUERY = "technology"
out = Path("data/incoming"); out.mkdir(parents=True, exist_ok=True)

def try_fetch():
    url_every = "https://newsapi.org/v2/everything"
    url_top = "https://newsapi.org/v2/top-headlines"
    base_params = {"language":"en","pageSize":5,"apiKey":API_KEY}
    try:
        p = base_params.copy(); p.update({"q":QUERY,"sortBy":"publishedAt"})
        r = requests.get(url_every, params=p, timeout=12); r.raise_for_status()
        j = r.json(); print("every status", r.status_code, "totalResults", j.get("totalResults"))
        arts = j.get("articles", []) or []
        if arts:
            return arts
    except Exception as e:
        print("every failed:", e)
    try:
        r = requests.get(url_top, params=base_params, timeout=12); r.raise_for_status()
        j = r.json(); print("top status", r.status_code, "totalResults", j.get("totalResults"))
        return j.get("articles", []) or []
    except Exception as e:
        print("top failed:", e)
    return []

arts = try_fetch()
print("got", len(arts), "articles")
for i,a in enumerate(arts[:5],1):
    title = a.get("title") or a.get("description") or ""
    record = {"id":str(uuid.uuid4()), "text":title, "source": (a.get("source") or {}).get("name"), "url":a.get("url"), "timestamp":datetime.utcnow().isoformat()+"Z"}
    fname = out / f"debug_{i}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.json"
    with open(fname,"w",encoding="utf-8") as fh:
        json.dump(record, fh, ensure_ascii=False)
    print("wrote", fname)
