# from pymongo import MongoClient
# import os

# client = MongoClient(str(os.getenv("MONGO_URI")))
# db = client.cse_gsp_22_26
# col = db.facultycredentials

# seen = set()
# for doc in col.find():
#     mail = doc["mailId"]
#     if mail in seen:
#         col.delete_one({"_id": doc["_id"]})
#     else:
#         seen.add(mail)


import pandas as pd
from pymongo import MongoClient
import os

# Connect to MongoDB
client = MongoClient(os.getenv("MONGO_URI"))
db = client.cse_gsp_22_26
col = db.facultylist

# Load the CSV (no headers, so we define them manually)
df = pd.read_csv(
    "../GSP Upgraded Data/2022-2026 STUDENTS PROJECT GUIDE LIST.csv",
    header=None,
    encoding="latin1",
    engine="python",
)
df.columns = ["S.NO", "NAME", "BATCHES"]

# Get all emails from DB for manual checking
emails = list(col.find({}, {"University EMAIL ID": 1, "_id": 0}))
email_list = [e.get("University EMAIL ID", "N/A") for e in emails]

unmatched = []

# Loop and update
for idx, row in df.iterrows():
    name = str(row["NAME"]).strip()
    try:
        count = int(row["BATCHES"])
    except ValueError:
        print(f"[{idx + 1}] ⚠️ Invalid batch number for: '{name}'")
        continue

    res = col.update_one(
        {"NAME OF THE FACULTY": name}, {"$set": {"TOTAL BATCHES": count}}
    )

    if res.matched_count == 0:
        print(f"[{idx + 1}] ❌ No match for: '{name}'")
        unmatched.append(name)
    else:
        print(f"[{idx + 1}] ✅ Updated: '{name}' → TOTAL BATCHES = {count}")

# Print unmatched and all emails for manual help
if unmatched:
    print("\n--- Unmatched Faculty Names ---")
    for name in unmatched:
        print(f"- {name}")

    print("\n--- Faculty Emails in MongoDB ---")
    for mail in email_list:
        print(f"• {mail}")
