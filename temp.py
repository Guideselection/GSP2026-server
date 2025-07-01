from pymongo import MongoClient
import os
from collections import defaultdict
from collections import Counter
from collections import defaultdict
from collections import Counter

client = MongoClient(os.getenv("MONGO_URI"))
# db = client.cse_gsp_22_26
db = client.backup_cse_gsp_22_26


def backup_db():
    print("Starting backup...")

    os.makedirs("backup_cse_gsp_22_26", exist_ok=True)
    print("Backup folder created.")

    for name in db.list_collection_names():
        print(f"Backing up collection: {name}")
        data = list(db[name].find())
        for doc in data:
            doc["_id"] = str(doc["_id"])
        with open(f"backup_cse_gsp_22_26/{name}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"{name}.json created with {len(data)} documents.")

    print("✅ Backup complete!")


# backup_db()

# src = client.cse_gsp_22_26
# dst = client.backup_cse_gsp_22_26

src = client.backup_cse_gsp_22_26
dst = client.deploy_cse_gsp_22_26


def clone_db():
    for name in src.list_collection_names():
        data = list(src[name].find())
        if data:
            dst[name].insert_many(data)


# clone_db()


# Fix regNo from string to int
def fix_regno_type():
    for doc in db.registeredStudentsData.find():
        reg = doc.get("regNo")
        if isinstance(reg, str):
            db.registeredStudentsData.update_one(
                {"_id": doc["_id"]}, {"$set": {"regNo": int(reg)}}
            )
            print(f"✅ Updated regNo: {reg} → {int(reg)}")


# fix_regno_type()


# Fix p2regNo from string to int
def fix_p2regno_type():
    for doc in db.registeredStudentsData.find():
        p2reg = doc.get("p2regNo")
        if isinstance(p2reg, str):
            db.registeredStudentsData.update_one(
                {"_id": doc["_id"]}, {"$set": {"p2regNo": int(p2reg)}}
            )
            print(f"✅ Updated p2regNo: {p2reg} → {int(p2reg)}")


# fix_p2regno_type()


def copy_registered_users(dst, src):
    data = list(dst.registeredUsers.find())
    if data:
        src.registeredUsers.insert_many(data)
        print(f"✅ Inserted {len(data)} documents from backup into live DB.")
    else:
        print("⚠️ No documents found in backup.")


# copy_registered_users(dst, src)


def replace_dst_registered_students_data(dst, src):
    # Step 1: Delete existing backup
    deleted = dst.registeredStudentsData.delete_many({})
    print(
        f"🗑️ Deleted {deleted.deleted_count} documents from dst.registeredStudentsData."
    )

    # Step 2: Copy from src to dst
    data = list(src.registeredStudentsData.find())
    if data:
        dst.registeredStudentsData.insert_many(data)
        print(
            f"✅ Inserted {len(data)} documents from src into dst.registeredStudentsData."
        )
    else:
        print("⚠️ No documents found in src.registeredStudentsData.")


# replace_dst_registered_students_data(dst, src)


def preview_null_password_docs():
    docs = list(db.registeredStudentsData.find({"password": None}))
    print(f"⚠️ Found {len(docs)} documents with password: null\n")
    cnt = 0
    for d in docs:
        cnt += 1
        print(f"🆔 {d['_id']} | teamId: {d.get('teamId')} | regNo: {d.get('regNo')}")
    print(f"{cnt=}")


# preview_null_password_docs()


def find_duplicate_team_ids():
    pipeline = [
        {"$group": {"_id": "$teamId", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
    ]

    duplicates = list(db.registeredStudentsData.aggregate(pipeline))

    if not duplicates:
        print("✅ No duplicate teamIds found.")
        return

    print(f"⚠️ Found {len(duplicates)} duplicate teamIds:\n")

    for dup in duplicates:
        team_id = dup["_id"]
        count = dup["count"]
        print(f"🧩 teamId: {team_id} | Count: {count}")


# find_duplicate_team_ids()


def delete_duplicate_team_ids_keep_oldest_with_password():
    print("🔍 Scanning for duplicate teamIds...\n")

    team_groups = defaultdict(list)

    # Group documents by teamId
    for doc in db.registeredStudentsData.find():
        team_id = doc.get("teamId")
        if team_id:
            team_groups[team_id].append(doc)

    delete_ids = []

    for team_id, docs in team_groups.items():
        if len(docs) <= 1:
            continue

        # Separate by password existence
        non_null_pw = [d for d in docs if d.get("password") is not None]
        to_sort = non_null_pw if non_null_pw else docs

        # Keep the oldest (smallest ObjectId)
        to_keep = sorted(to_sort, key=lambda d: d["_id"])[0]["_id"]

        for d in docs:
            if d["_id"] != to_keep:
                delete_ids.append(d["_id"])

    print(f"🗑️ Deleting {len(delete_ids)} duplicate documents...\n")

    result = db.registeredStudentsData.delete_many({"_id": {"$in": delete_ids}})
    print(f"✅ Deleted {result.deleted_count} duplicates from registeredStudentsData.")


# delete_duplicate_team_ids_keep_oldest_with_password()


def find_and_delete_duplicate_team_ids():
    pipeline = [
        {"$group": {"_id": "$teamId", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
    ]

    duplicates = list(db.registeredStudentsData.aggregate(pipeline))

    if not duplicates:
        print("✅ No duplicate teamIds found.")
        return

    print(f"⚠️ Found {len(duplicates)} duplicate teamIds.\n")

    delete_ids = []

    for entry in duplicates:
        team_id = entry["_id"]
        docs = list(db.registeredStudentsData.find({"teamId": team_id}))
        sorted_docs = sorted(docs, key=lambda d: d["_id"])
        keep_id = sorted_docs[0]["_id"]

        print(f"\n🧩 teamId: {team_id} | Count: {entry['count']}")
        print(f"✅ Keeping document _id: {keep_id} (teamId: {team_id})")

        for doc in sorted_docs[1:]:
            print(f"🗑️ Deleting document _id: {doc['_id']} (teamId: {team_id})")
            delete_ids.append(doc["_id"])

    result = db.registeredStudentsData.delete_many({"_id": {"$in": delete_ids}})
    print(
        f"\n🧹 Deleted {result.deleted_count} duplicate documents from registeredStudentsData."
    )


# find_and_delete_duplicate_team_ids()


def clean_team_and_student_duplicates_in_faculty_list():
    for doc in db.facultylist.find():
        doc_id = doc["_id"]
        email = doc.get("University EMAIL ID")
        teams = doc.get("allTeams", [])
        students = doc.get("allStudents", [])
        total_batches = doc.get("TOTAL BATCHES", 0)

        team_counts = Counter(teams)
        student_counts = Counter(students)

        # Count how many duplicate team IDs (each extra counts)
        team_dup_count = sum(count - 1 for count in team_counts.values() if count > 1)
        print(f"\n📘 Faculty: {email}")
        print(f"🧩 Duplicate teamIds removed: {team_dup_count}")

        # Remove duplicate teams (keep first occurrence)
        seen_teams = set()
        teams_clean = []
        for t in teams:
            if t not in seen_teams:
                teams_clean.append(t)
                seen_teams.add(t)

        # Remove duplicate students (keep first occurrence)
        seen_students = set()
        students_clean = []
        for s in students:
            if s not in seen_students:
                students_clean.append(s)
                seen_students.add(s)

        # Update in DB
        db.facultylist.update_one(
            {"_id": doc_id},
            {
                "$set": {"allTeams": teams_clean, "allStudents": students_clean},
                "$inc": {"TOTAL BATCHES": team_dup_count},
            },
        )

        print(f"✅ Cleaned. TOTAL BATCHES incremented by {team_dup_count}.")


# clean_team_and_student_duplicates_in_faculty_list()


def delete_users_without_team_id():
    to_delete = list(
        db.registeredUsers.find(
            {"$or": [{"teamId": {"$exists": False}}, {"teamId": None}, {"teamId": ""}]}
        )
    )

    if not to_delete:
        print("✅ No users found without teamId.")
        return

    for doc in to_delete:
        print(f"🗑️ Deleting: {doc.get('email', '[no email]')}")

    ids = [doc["_id"] for doc in to_delete]
    result = db.registeredUsers.delete_many({"_id": {"$in": ids}})
    print(f"\n✅ Deleted {result.deleted_count} documents from registeredUsers.")


# delete_users_without_team_id()


def add_alloted_batches_field():
    count = 0
    for doc in db.facultylist.find():
        total = doc.get("TOTAL BATCHES", 0)
        db.facultylist.update_one(
            {"_id": doc["_id"]}, {"$set": {"ALLOTED BATCHES": total}}
        )
        print(
            f"✅ Added 'ALLOTED BATCHES': {total} for {doc.get('University EMAIL ID')}"
        )
        count += 1

    print(f"\n🧾 Total documents updated: {count}")


# add_alloted_batches_field()


def update_total_batches_from_allTeams():
    faculty_collection = db["facultylist"]
    for doc in faculty_collection.find():
        all_teams = doc.get("allTeams", [])
        total_batches = len(all_teams)

        faculty_collection.update_one(
            {"_id": doc["_id"]}, {"$set": {"TOTAL BATCHES": total_batches}}
        )

        print(
            f"✅ Updated TOTAL BATCHES to {total_batches} for: {doc.get('University EMAIL ID', 'N/A')}"
        )


# update_total_batches_from_allTeams()


def update_users_without_team_id():
    users_col = db["users"]

    result = users_col.update_many(
        {"teamId": {"$exists": False}},
        {"$set": {"firstTime": True, "password": "studentcse"}},
    )

    print(f"✅ Updated {result.modified_count} users missing teamId.")


# update_users_without_team_id()


def fix_null_passwords():
    users_col = db["users"]

    res = users_col.update_many(
        {"password": None}, {"$set": {"password": "studentcse"}}
    )

    print(f"✅ Updated {res.modified_count} users with null password.")


# fix_null_passwords()
