# check_ids.py
import glob, json
from collections import Counter

incoming = glob.glob("data/incoming/*.json")
ids = []
for f in incoming:
    try:
        j = json.load(open(f, "r", encoding="utf-8"))
        ids.append(j.get("id") or j.get("url") or "")
    except Exception:
        ids.append("ERROR:"+f)

c = Counter(ids)
dups = [(k,v) for k,v in c.items() if v>1]
print("total incoming files:", len(incoming))
print("duplicates (id/count) sample:", dups[:20])
