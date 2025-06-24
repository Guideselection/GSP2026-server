from flask import Flask, render_template, request, jsonify, redirect
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId
from flask_cors import CORS
from flask_mail import Mail, Message
from pymongo.errors import InvalidOperation, DuplicateKeyError
import random
import jwt
from datetime import datetime, timedelta
import time
import driveAPI
import os
from werkzeug.utils import secure_filename
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


app = Flask(__name__)
# CORS(app)
CORS(app, supports_credentials=True)
# CORS(app, resources={r"/api/*": {"origins": "http://localhost:5173"}})


load_dotenv()


# app = Flask(__name__)
app.config["MAIL_SERVER"] = "smtp.gmail.com"
app.config["MAIL_PORT"] = 465
app.config["MAIL_USE_TLS"] = False
app.config["MAIL_USE_SSL"] = True
# print(os.getenv("TEMP_MAIL"))
# print(os.getenv("TEMP_MAIL_PASSWORD"))
app.config["MAIL_USERNAME"] = str(os.getenv("ADMIN_MAILID"))
app.config["MAIL_PASSWORD"] = str(os.getenv("TEMP_MAIL_PASSWORD"))
# print(str(os.getenv("TEMP_MAIL")))
# print(str(os.getenv("TEMP_MAIL_PASSWORD")))


mail = Mail(app)

# Elastic Mail Service
# app.config["MAIL_SERVER"] = "smtp.elasticemail.com"  # Replace with your email server
# app.config["MAIL_PORT"] = 2525
# # app.config['MAIL_USE_TLS'] = False
# # app.config['MAIL_USE_SSL'] = True
# app.config["MAIL_USERNAME"] = (
#     "guideselectionportal@cse-soc.com"  # Replace with your email address
# )
# app.config["MAIL_PASSWORD"] = str(
#     os.getenv("MAIL_PASSWORD")
# )  # Replace with your email password


# mail = Mail(app)


client = MongoClient(str(os.getenv("MONGO_URI")), server_api=ServerApi('1'))
# print(os.getenv("MONGO_URI"))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client["cse_gsp_22_26"]

# CORS(app)
CORS(app, supports_credentials=True)

# check health in render (hosting service)
@app.route("/healthz")
def healthz():
    return "OK", 200

@app.route("/")
def index():
    return jsonify({"message": "Backend is working."})


@app.route("/users", methods=["POST", "GET"])
def data():
    if request.method == "POST":
        body = request.json
        fname = body["firstName"]
        lname = body["lastName"]
        emailId = body["emailId"]

        db["users"].insert_one(
            {"firstName": fname, "lastName": lname, "emailId": emailId}
        )

        return jsonify(
            {
                "status": "Data is posted to MongoDB",
                "firstName": fname,
                "lastName": lname,
                "emailId": emailId,
            }
        )


@app.route("/data", methods=["GET"])
def get_data():
    collection = db.users  # Replace <collection_name> with the name of your collection
    data = collection.find()  # Retrieve all documents from the collection

    result = []
    i = 0  # Store the retrieved data
    for document in data:
        result.append({})
        result[i]["Course_Code"] = document["Course_Code"]
        result[i]["Course_name"] = document["Course_Name"]
        result[i]["Course_Credit"] = document["Course_Credit"]
        i += 1
    return jsonify(result)  # Return the data as a JSON response


@app.route("/add", methods=["POST"])
def add_data():
    # Get the data from the request
    data = request.get_json()
    # data = request.

    # Insert the data into the collection
    collection = db.users  # Replace <collection_name> with the name of your collection
    result = collection.insert_one(data)

    # Return the ID of the inserted document
    return jsonify({"inserted_id": str(result.inserted_id)})


@app.route("/api/update/<string:id>", methods=["PUT"])
def update_data(id):
    # Get the update data from the request
    data = request.get_json()
    filter = {"_id": ObjectId(id)}
    update = {"$set": data}

    # Update the data in the collection
    collection = db.users  # Replace <collection_name> with the name of your collection
    result = collection.update_many(filter, update)

    # Return the number of documents updated
    return jsonify({"updated_count": result.modified_count})


@app.route("/api/update", methods=["PUT"])
def update_all_data():
    # Get the update data from the request
    data = request.get_json()
    filter = {}
    update = {"$rename": {"EMAIL ID (University Mail ID)": "EMAIL ID"}}

    # Update the data in the collection
    collection = (
        db.facultylist
    )  # Replace <collection_name> with the name of your collection
    result = collection.update_many(filter, update)

    # Return the number of documents updated
    return jsonify({"updated_count": "updated"})


secret_key = str(os.getenv("SECRET_KEY"))


def generate_token(email):
    # Define the payload for the token (you can include additional claims if needed)
    payload = {"email": email, "exp": datetime.utcnow() + timedelta(minutes=100)}

    # Define the secret key used to sign the token
    # Make sure to keep this key secure and preferably stored in a configuration file

    # Generate the token with the payload and secret key
    token = jwt.encode(payload, secret_key, algorithm="HS256")

    return token


@app.route("/checkAuthentication/<string:mailId>", methods=["GET"])
def checkAuthentication(mailId):
    token = request.headers.get("Authorization")
    # print(token)
    try:
        decoded_token = jwt.decode(token, secret_key, algorithms=["HS256"])
        print(f"{decoded_token=}")
        email = decoded_token["email"]
        print(f"{email=}")
        if str(mailId) == str(email):
            print("Authenticated")
            return jsonify({"message": "Authenticated"})
        else:
            return jsonify({"message": "Token Tampered"})
    except:
        return jsonify({"message": "Not Authenticated"})


@app.route("/api/check/<string:mail>", methods=["GET"])
def check_account_avalable(mail):
    collection = db.users
    filter = {"email": mail}
    result = collection.find_one(filter)
    print(result)
    if result:
        return jsonify({"first_time": result["firstTime"]})
    else:
        return jsonify({"data": "mail not found"})


@app.route("/api/check/<string:mailid>/<string:password1>", methods=["POST"])
def check_data(mailid, password1):
    # Get the update data from the request
    data = request.json
    password = data.get("passcode")
    if str(mailid)[:6] == "CSE-26":
        filter = {"teamId": mailid}
        collection = (
            db.users
        )  # Replace <collection_name> with the name of your collection
        result = collection.find_one(filter)

        # token = generate_token(result["email"])

        if result is None:
            return jsonify({"is_account_available": "false"})
        else:
            token = generate_token(result["email"])

            if str(password) == str(result["password"]):
                return jsonify(
                    {
                        "is_account_available": "true",
                        "is_password_correct": "true",
                        "token": token,
                        "first_time": "false",
                        "Is_Email_sent": "false",
                        "userEmail": result["email"],
                        "teamId": mailid,
                    }
                )
            else:
                return jsonify(
                    {
                        "is_account_available": "true",
                        "is_password_correct": "false",
                        password: result["password"],
                        "first_time": "false",
                        "Is_Email_sent": "false",
                    }
                )

    filter = {"email": mailid}

    # Update the data in the collection
    collection = db.users  # Replace <collection_name> with the name of your collection
    result = collection.find_one(filter)
    # print(result)
    if result is None:
        return jsonify({"is_account_available": "false"})

    id = result["_id"]

    token = generate_token(mailid)

    tokenForFirstTime = generate_token(mailid)
    # print(result)
    if result["firstTime"]:
        # return jsonify({"is_account_available":"true","_id":str(id), "token":token, "first_login":"true"})
        # print(result["firstTime"])
        otp = random.randint(100000, 999999)
        print(f"\n\n{otp=}\n\n")

        # print(result)
        if result:
            try:
                msg = Message(
                    "One-Time Password (OTP) for Registration",  # Email subject
                    sender="guideselection.cse@sathyabama.ac.in",  # Replace with your email address
                    recipients=[mailid],
                )  # Replace with the recipient's email address

                msg.body = "This is a test email sent from Flask-Mail"  # Email body
                msg.html = f"""
                            <html>
                            <body>
                                <p>Dear {result["Full Name"]},</p>
                                <p>Your One-Time Password (OTP) for registration is:</p>
                                <div style="display: flex; justify-content: center; align-items:center;">
                                    <h2 style="color: #007bff; display:flex;  align-items:center;  justify-content: center; font-size: 24px; font-weight: bold;">{otp}</h2>

                                </div>
                                <p>Please use this OTP to complete your registration process.</p>
                                <p>If you did not request this OTP or have any questions, please contact our support team.</p>
                                <p>Thank you for choosing to register with us.</p>
                                <br/><br/><br/>
                                <p>Best Regards,</p>
                                <p>School of Computing,</p>
                                <p>Sathyabama Institute of Science & Technology</p>
                            </body>
                            </html>
                            """

                mail.send(msg)

                return jsonify(
                    {
                        "is_account_available": "true",
                        "Is_Email_sent": "true",
                        "_id": str(id),
                        "OTP": otp,
                        "token": token,
                        "token_for_first_time": tokenForFirstTime,
                        "name": result["Full Name"],
                        "regNo": result["regNo"],
                        "phoneNo": result["Mobile Number"],
                        "section": result["section"],
                        "first_time": "true",
                    }
                )
            except Exception as e:
                print(e, "error")
                return jsonify(
                    {
                        "is_account_available": "true",
                        "_id": str(id),
                        "Is_Email_sent": "false",
                        "first_time": result["firstTime"],
                    }
                )
        else:
            return jsonify({"is_account_available": "false", "Is_Email_sent": "false"})

    elif str(password) == result["password"]:
        return jsonify(
            {
                "is_account_available": "true",
                "is_password_correct": "true",
                "_id": str(id),
                "token": token,
                "first_time": "false",
                "Is_Email_sent": "false",
                "teamId": result["teamId"],
                "userEmail": mailid,
            }
        )

    else:
        return jsonify(
            {
                "is_account_available": "true",
                "is_password_correct": "false",
                password: result["password"],
                "first_time": "false",
                "Is_Email_sent": "false",
            }
        )

    # Return the number of documents updated
    # if result:
    #     return jsonify({'is_account_available': "true", "_id":str(id)})
    # else:
    #     return jsonify({'is_account_available': "false"})


@app.route("/api/check/verified/<string:mailid>/<string:id>", methods=["GET"])
def Send_otp(id, mailid):
    # Get the update data from the request
    # data = request.get_json()

    otp = random.randint(100000, 999999)
    print(f"\n\n{otp=}\n\n")

    try:
        msg = Message(
            f"Your OTP is {otp}",  # Email subject
            sender="guideselection.cse@sathyabama.ac.in",  # Replace with your email address
            recipients=[mailid],
        )  # Replace with the recipient's email address
        msg.body = "This is a test email sent from Flask-Mail"  # Email body

        mail.send(msg)
        return jsonify({"Is_Email_sent": "true", "OTP": otp})
    except Exception as e:
        print(e)
        return jsonify({"Is_Email_sent": "false"})


@app.route("/api/delete/<string:id>", methods=["DELETE"])
def delete_data(id):
    # Delete the data from the collection
    collection = db.users  # Replace <collection_name> with the name of your collection
    result = collection.delete_one({"_id": ObjectId(id)})

    # Return the number of documents deleted
    return jsonify({"deleted_count": result.deleted_count})


# @app.route("/guide_list", methods=["GET"])
# def get_Guide_List():
#     collection = db.facultylist

#     data = collection.find()  # Retrieve all documents from the collection

#     result = []
#     i = 0  # Store the retrieved data
#     for document in data:
#         result.append({})
#         result[i]["id"] = i + 1
#         # result[i]["SL"] = document["SL"]["No"]
#         result[i]["NAME"] = document["NAME OF THE FACULTY"]
#         result[i]["VACANCIES"] = document["TOTAL BATCHES"]
#         result[i]["DESIGNATION"] = document["DESIGNATION"]
#         result[i]["DOMAIN1"] = document["DOMAIN 1"]
#         result[i]["DOMAIN2"] = document["DOMAIN 2"]
#         result[i]["DOMAIN3"] = document["DOMAIN 3"]
#         result[i]["UniversityEMAILID"] = document["University EMAIL ID"]
#         result[i]["IMAGE"] = document["IMAGE"]
#         result[i]["EMPID"] = document["EMP ID"]
#         i += 1

#     # print(result)
#     return jsonify(result)


# @app.route("/guide_list", methods=["GET"])
# def get_guide_list():
#     collection = db.facultylist
#     try:
#         page = int(request.args.get("page", 1))
#         limit = int(request.args.get("limit", 10))
#         search = request.args.get("search", "").strip().lower()
#         skip = (page - 1) * limit

#         query = {}
#         if search:
#             query["NAME OF THE FACULTY"] = {"$regex": search, "$options": "i"}

#         total_count = collection.count_documents(query)
#         data = collection.find(query).skip(skip).limit(limit)

#         result = []
#         for i, document in enumerate(data, start=skip + 1):
#             result.append(
#                 {
#                     "id": i,
#                     "NAME": document["NAME OF THE FACULTY"],
#                     "VACANCIES": document["TOTAL BATCHES"],
#                     "DESIGNATION": document["DESIGNATION"],
#                     "DOMAIN1": document["DOMAIN 1"],
#                     "DOMAIN2": document["DOMAIN 2"],
#                     "DOMAIN3": document["DOMAIN 3"],
#                     "UniversityEMAILID": document["University EMAIL ID"],
#                     "IMAGE": document["IMAGE"],
#                     "EMPID": document["EMP ID"],
#                 }
#             )

#         return jsonify(
#             {
#                 "total": total_count,
#                 "page": page,
#                 "limit": limit,
#                 "totalPages": (total_count + limit - 1) // limit,
#                 "guides": result,
#             }
#         )

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


@app.route("/guide_list", methods=["GET"])
def get_guide_list():
    collection = db.facultylist

    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 10))
        search = request.args.get("search", "").strip()
        skip = (page - 1) * limit

        query = {}
        if search:
            query["NAME OF THE FACULTY"] = {"$regex": search, "$options": "i"}

        total_count = collection.count_documents(query)
        data = collection.find(query).skip(skip).limit(limit)

        result = []
        for i, doc in enumerate(data, start=skip + 1):
            result.append(
                {
                    "id": i,
                    "NAME": doc.get("NAME OF THE FACULTY", ""),
                    "VACANCIES": doc.get("TOTAL BATCHES", 0),
                    "DESIGNATION": doc.get("DESIGNATION", ""),
                    "DOMAIN1": doc.get("DOMAIN 1", ""),
                    "DOMAIN2": doc.get("DOMAIN 2", ""),
                    "DOMAIN3": doc.get("DOMAIN 3", ""),
                    "UniversityEMAILID": doc.get("University EMAIL ID", ""),
                    "IMAGE": doc.get("IMAGE", ""),
                    "EMPID": doc.get("EMP ID", ""),
                }
            )

        return jsonify(
            {
                "page": page,
                "limit": limit,
                "total": total_count,
                "totalPages": (total_count + limit - 1) // limit,
                "guides": result,
            }
        )

    except Exception as e:
        print("🔥 /guide_list error:", e)
        return jsonify({"error": "Something went wrong"}), 500


# @app.route("/guide_list", methods=["GET"])
# def get_guide_list():
#     try:
#         page = int(request.args.get("page", 1))
#         limit = int(request.args.get("limit", 10))
#         search = request.args.get("search", "").strip()

#         query = {}
#         if search:
#             query["NAME"] = {"$regex": search, "$options": "i"}

#         total_docs = db.guides.count_documents(query)
#         total_pages = (total_docs + limit - 1) // limit

#         cursor = db.guides.find(query).skip((page - 1) * limit).limit(limit)

#         guides = []
#         for doc in cursor:
#             doc["_id"] = str(doc["_id"])
#             guides.append(doc)

#         return jsonify({"guides": guides, "totalPages": total_pages})

#     except Exception as e:
#         print("🔥 /guide_list error:", e)
#         return jsonify({"error": "Internal server error"}), 500


@app.route("/create_collection/<string:mailId>", methods=["POST"])
def create_collection_single(mailId):
    data = request.json  # Assuming the request data is in JSON format

    # Get the collection name and data from the request JSON
    # collection_name = data.get('collection_name')
    collection_data = data.get("registered_data")
    print(f"{collection_data=}")

    # Create the collection
    collection = db["registeredStudentsData"]
    print(f"{collection=}")

    status = {
        "documentation": False,
        "ppt": False,
        "guideApproval": False,
        "researchPaper": {
            "approval": False,
            "communicated": False,
            "accepted": False,
            "payment": False,
        },
    }

    documents = {"researchPaper": None, "documentation": None, "ppt": None}

    comments = []

    collection_data["status"] = status
    collection_data["documentation"] = documents
    collection_data["comments"] = comments
    collection_data["editProjectDetails"] = False
    collection_data["marks"] = 0

    generated_team_id = f"CSE-{str(datetime.now().year % 100 + 1)}-{str(int(collection_data['regNo']) % 10000).rjust(4, '0')}"
    collection_data["teamId"] = generated_team_id

    users_collection = db["users"]
    registered_users = db["registeredUsers"]
    filter = {"email": mailId}
    update = {"$set": {"teamId": generated_team_id}}
    res1 = users_collection.update_one(filter, update)
    res2 = registered_users.update_one(filter, update)
    print(f"{res1=}")
    print(f"{res2=}")
    usersCollection = db["users"]

    # collection_data["image"] = usersCollection.find_one(filter)["image"]

    # Insert data into the collection
    inserted_data = collection.insert_one(collection_data)
    print(f"{inserted_data=}")

    collection = db["facultylist"]
    document = collection.find_one(
        {"University EMAIL ID": collection_data["selectedGuideMailId"]}
    )
    print(f"{document=}")
    updated_data = {"allStudents": [], "allTeams": []}

    if document:
        if "allStudents" in document:
            document["allStudents"].append(mailId)
        else:
            document["allStudents"] = [mailId]

        if "allTeams" in document:
            document["allTeams"].append(generated_team_id)
        else:
            document["allTeams"] = [generated_team_id]

    updated_data["allStudents"] = document["allStudents"]
    updated_data["allTeams"] = document["allTeams"]

    # print(filter_data, updated_data)

    # Update the data in the collection

    result = collection.update_one(
        {"University EMAIL ID": collection_data["selectedGuideMailId"]},
        {"$set": updated_data},
    )

    print(f"{result=}")
    # Send Mail To Student
    password = collection_data["password"]
    print(generated_team_id, password)

    try:
        msg = Message(
            "Project Submission Confirmation",  # Email subject
            sender="guideselection.cse@sathyabama.ac.in",  # Replace with your email address
            recipients=[mailId],
        )  # Replace with the recipient's email address
        msg.html = f"""
        <html>
        <body>
            <p>Dear {collection_data["name"]},</p>
            <p>We are writing to inform you that we have received your project submission successfully. Thank you for your effort and contribution.</p>
            <b>Project Details:</b><br/>
            <ul>
            <li>Project Id - {generated_team_id}</li>
            <li>Project Name - {collection_data["projectTitle"]}</li>
            <li>Project Domain - {collection_data["projectDomain"]}</li>
            <li>Project Description - {collection_data["projectDesc"]}</li>
            <li>Guide Name - {collection_data["selectedGuide"]}</li>
            </ul><br/>

            <ul>
            <b>Login Credentials:</b><br/>
            <li>Project Id - {generated_team_id}</li>
            <li>Password - {password}</li>
            </ul><br/>
            <p>Your guide will review your project thoroughly and get back to you with feedback.
            </p><br/><br/><br/>
            <p>Best Regards,</p>
            <p>School of Computing,</p>
            <p>Sathyabama Institute of Science & Technology</p>
        </body>
        </html>
        """

        mail.send(msg)
        print("mail sent")
        return jsonify({"Is_Email_sent": "true"})
    except Exception as e:
        print(f"{e=}")
        return jsonify(
            {
                "Is_Email_sent": "false",
                "message": "Collection created and data inserted successfully!",
                "inserted_id": str(inserted_data.inserted_id),
            }
        )


@app.route("/create_collection/<string:mailId1>/<string:mailId2>", methods=["POST"])
def create_collection_duo(mailId1, mailId2):
    data = request.json  # Assuming the request data is in JSON format

    # Get the collection name and data from the request JSON
    collection_data = data.get("data")

    status = {
        "documentation": False,
        "ppt": False,
        "guideApproval": False,
        "researchPaper": {
            "approval": False,
            "communicated": False,
            "accepted": False,
            "payment": False,
        },
    }

    documents = {"researchPaper": None, "documentation": None, "ppt": None}

    comments = []

    collection_data["status"] = status
    collection_data["documentation"] = documents
    collection_data["comments"] = comments
    collection_data["editProjectDetails"] = False
    collection_data["marks"] = 0
    collection_data["p2marks"] = 0

    # Create the collection
    collection = db["registeredStudentsData"]

    generated_team_id = f"CSE-{str(datetime.now().year % 100 + 1)}-{str(int(collection_data['regNo']) % 10000).rjust(4, '0')}"
    collection_data["teamId"] = generated_team_id

    users_collection = db["users"]
    registered_users = db["registeredUsers"]
    filter1 = {"email": mailId1}
    filter2 = {"email": mailId2}
    update = {"$set": {"teamId": generated_team_id}}
    users_collection.update_one(filter1, update)
    registered_users.update_one(filter1, update)
    users_collection.update_one(filter2, update)
    registered_users.update_one(filter2, update)

    usersCollection = db["users"]
    # collection_data["image"] = usersCollection.find_one(filter1)["image"]
    # collection_data["p2image"] = usersCollection.find_one(filter2)["image"]

    # Insert data into the collection
    inserted_data = collection.insert_one(collection_data)

    collection = db["facultylist"]
    document = collection.find_one(
        {"University EMAIL ID": collection_data["selectedGuideMailId"]}
    )
    updated_data = {"allStudents": []}
    if document:
        if "allStudents" in document:
            document["allStudents"].append(mailId1)
            document["allStudents"].append(mailId2)
        else:
            document["allStudents"] = [mailId1, mailId2]

        if "allTeams" in document:
            document["allTeams"].append(generated_team_id)
        else:
            document["allTeams"] = [generated_team_id]

    updated_data["allStudents"] = document["allStudents"]
    updated_data["allTeams"] = document["allTeams"]

    # print(filter_data, updated_data)

    # Update the data in the collection

    result = collection.update_one(
        {"University EMAIL ID": collection_data["selectedGuideMailId"]},
        {"$set": updated_data},
    )

    # Send Mail To Student
    password = collection_data["password"]
    print(generated_team_id, password)

    print(mailId1, mailId2)

    try:
        msg = Message(
            f"Project Submission Confirmation",  # Email subject
            sender="guideselection.cse@sathyabama.ac.in",  # Replace with your email address
            recipients=[mailId1, mailId2],
        )  # Replace with the recipient's email address
        msg.html = f"""
        <html>
        <body>
            <p>Dear {collection_data["name"]} and {collection_data["p2name"]},</p>
            <p>We are writing to inform you that we have received your project submission successfully. Thank you for your effort and contribution.</p>
            <b>Project Details:</b><br/>
            <ul>
            <li>Project Id - {generated_team_id}</li>
            <li>Project Name - {collection_data["projectTitle"]}</li>
            <li>Project Domain - {collection_data["projectDomain"]}</li>
            <li>Project Description - {collection_data["projectDesc"]}</li>
            <li>Guide Name - {collection_data["selectedGuide"]}</li>
            </ul><br/>

            <ul>
            <b>Login Credentials:</b><br/>
            <li>Project Id - {generated_team_id}</li>
            <li>Password - {password}</li>
            </ul><br/>
            <p>Your guide will review your project thoroughly and get back to you with feedback.
            </p><br/><br/><br/>
            <p>Best Regards,</p>
            <p>School of Computing,</p>
            <p>Sathyabama Institute of Science & Technology</p>
        </body>
        </html>
        """

        mail.send(msg)
        return jsonify({"Is_Email_sent": "true"})
    except Exception as e:
        print(e)
        return jsonify(
            {
                "Is_Email_sent": "false",
                "message": "Collection created and data inserted successfully!",
                "inserted_id": str(inserted_data.inserted_id),
            }
        )


@app.route("/update_data", methods=["PUT"])
def updateLoginData():
    data = request.json
    collection_name = data.get("collection_name")
    filter_data = data.get("filter_data")
    updated_data = data.get("updated_data")

    # 🔒 Convert to MongoDB Int32
    # if "password" in updated_data:
    #     try:
    #         updated_data["password"] = Int32(int(updated_data["password"]))
    #     except Exception:
    #         return jsonify({"error": "Password must be an integer"}), 400

    if "regNo" in updated_data:
        try:
            updated_data["regNo"] = int(updated_data["regNo"])
        except Exception:
            return jsonify({"error": "regNo must be an integer"}), 400

    # Update the data in the collection
    collection = db[collection_name]
    result = collection.update_one(filter_data, {"$set": updated_data})

    if result.modified_count > 0:
        return jsonify({"message": "Data updated successfully!"})
    else:
        return jsonify({"message": "No matching data found for update."}), 404


# @app.route('/update_vacancies_data', methods=['PUT'])
def update_vacancies_data(collection_name, filter_data, updated_data):
    data = request.json  # Assuming the request data is in JSON format
    collection = db[collection_name]

    result = collection.update_one(filter_data, {"$set": updated_data})

    if result.modified_count > 0:
        return jsonify({"message": "Data updated successfully!"})
    else:
        return jsonify({"message": "No matching data found for update."}), 404


# Function to acquire a lock
def acquire_lock(guide_mail_id):
    lock_collection = db["lock_collection"]
    try:
        lock_collection.insert_one({"mailId": guide_mail_id})
        return True
    except DuplicateKeyError:
        return False


# Function to release a lock
def release_lock(guide_mail_id):
    lock_collection = db["lock_collection"]
    lock_collection.delete_one({"mailId": guide_mail_id})


# print(acquire_lock("dean.computing@sathyabama.ac.in"))


@app.route("/add_registered_data", methods=["PUT"])
def add_registered_data():
    data = request.json
    email = data.get("email")
    users_collection = db.registeredUsers
    guideMailId = data.get("guideMailId")

    collection = db.facultylist
    filter = {"University EMAIL ID": guideMailId}
    result = collection.find_one(filter)
    # print(result)
    if result:
        # Check if registration lock is set for the guide
        while not acquire_lock(guideMailId):
            time.sleep(1)  # Wait for a short period
            # Check again after waiting
        # print(email, "--Acquired Lock")

        result = collection.find_one(filter)
        if result["TOTAL BATCHES"] > 0:
            try:
                # Start a client session
                with client.start_session() as session:
                    # Start a transaction

                    with session.start_transaction():
                        # Perform the critical operation
                        new_user = data
                        users_collection.insert_one(new_user, session=session)
                        if data.get("update_vacancies_data"):
                            update_vacancies_data(
                                "facultylist",
                                {"University EMAIL ID": guideMailId},
                                {"TOTAL BATCHES": result["TOTAL BATCHES"] - 1},
                            )
                    # Commit the transaction
                    # session.commit_transaction()
                    # Release the lock when done

                    # print(email, "--Realesed Lock")

                return jsonify({"message": "User registered successfully"}), 201
            except DuplicateKeyError as e:
                # session.abort_transaction()
                return jsonify({"error": "Email already registered"})
            except InvalidOperation as e:
                return jsonify({"error": str(e)}), 400
            except Exception as e:
                return jsonify(
                    {"error": "An error occurred during registration", "exception": e}
                )
            finally:
                # Release the lock when done
                release_lock(guideMailId)
                # print(email, "--Realesed Lock")
        else:
            release_lock(guideMailId)
            return jsonify({"message": "No Vacancies"})


@app.route("/rollback_registered_data", methods=["POST"])
def rollback_registered_data():
    data = request.json
    collection = db.registeredUsers
    delete_result = collection.delete_many({"email": data.get("email")})

    # collection = db.facultylist
    # filter = {'University EMAIL ID':data.get("guideMailId")}
    # result = collection.find_one(filter)
    # update_vacancies_data("facultylist", { "University EMAIL ID": data.get("guideMailId") }, {"TOTAL BATCHES": result['TOTAL BATCHES']+1 })

    return jsonify({"deleted": "true"})


@app.route("/check_vacancies/<string:mail>", methods=["GET"])
def check_vacancies(mail):
    collection = db.facultylist
    filter = {"University EMAIL ID": mail}
    result = collection.find_one(filter)
    # print(result)
    return jsonify({"vacancies": result["TOTAL BATCHES"]})


@app.route("/check_second_mail/<string:mailid>", methods=["GET"])
def check_second_Person_mail(mailid):
    collection = db.users
    filter = {"email": mailid}
    result = collection.find_one(filter)
    print(result)

    if result:
        if result["firstTime"]:
            otp = random.randint(100000, 999999)
            print(f"\n\n{otp=}\n\n")

            try:
                msg = Message(
                    "One-Time Password (OTP) for Registration",  # Email subject
                    sender="guideselection.cse@sathyabama.ac.in",  # Replace with your email address
                    recipients=[mailid],
                )  # Replace with the recipient's email address
                # msg.body = 'This is a test email sent from Flask-Mail'  # Email body
                msg.html = f"""
                            <html>
                            <body>
                                <p>Dear {result["Full Name"]},</p>
                                <p>Your One-Time Password (OTP) for registration is:</p>
                                <div style="display: flex; justify-content: center;">
                                    <h2 style="color: #007bff; font-size: 24px; font-weight: bold;">{otp}</h2>
                                </div>
                                <p>Please use this OTP to complete your registration process.</p>
                                <p>If you did not request this OTP or have any questions, please contact our support team.</p>
                                <p>Thank you for choosing to register with us.</p>
                                <br/><br/><br/>
                                <p>Best Regards,</p>
                                <p>School of Computing,</p>
                                <p>Sathyabama Institute of Science & Technology</p>
                            </body>
                            </html>
                            """

                mail.send(msg)

                return jsonify(
                    {
                        "email": result["email"],
                        "firstTime": result["firstTime"],
                        "name": result["Full Name"],
                        "regNo": result["regNo"],
                        "phoneNo": result["Mobile Number"],
                        "section": result["section"],
                        "otp": otp,
                    }
                )
            except Exception as e:
                print(e)
                return jsonify(
                    {"email": result["email"], "firstTime": result["firstTime"]}
                )
        else:
            return jsonify({"email": result["email"], "firstTime": result["firstTime"]})
    else:
        return jsonify({"data": "mail not found"})


@app.route("/update_second_user_credentials", methods=["POST"])
def update_second_user_credentials():
    data = request.json
    collection_name = data.get("collection_name")
    filter = data.get("filter_data")
    collection = db[collection_name]
    result = collection.delete_many(filter)

    return jsonify({"deleted": "true"})


@app.route("/studentLogin/getStudentData/<string:mailid>", methods=["POST"])
def getStudentdata(mailid):
    registeredStudentsData = db["registeredStudentsData"]
    filter = {"teamId": mailid}
    print(filter)
    studentCompleteData = registeredStudentsData.find(filter)
    # print(studentCompleteData[0])

    # Initialize an empty list to store the results
    studentData = []
    projectDetails = []
    projectDetails2 = []
    projectStatus = []
    documentation = []
    comments = []
    comments2 = []
    studentImage2 = ""
    studentImage1 = ""

    try:
        filterguide = {
            "University EMAIL ID": studentCompleteData[0]["selectedGuideMailId"]
        }
        collection = db["facultylist"]
        res = collection.find_one(filterguide)
        # print(res)
        ps = res.get("problemStatements", [])
    except:
        ps = []

    # Iterate over the cursor to extract data
    for student in studentCompleteData:
        # Do something with each document in the cursor
        if student["team"]:
            studentData.append(
                {
                    # "student_id": str(student["_id"]),
                    "name": student["name"],
                    "team": student["team"],
                    "regNo": student["regNo"],
                    "phoneNo": student["phoneNo"],
                    "p2name": student["p2name"],
                    "p2regNo": student["p2regNo"],
                    "p2phoneNo": student["p2phoneNo"],
                    "p2mailId": student["p2mailId"],
                    "teamId": student["teamId"],
                    "editProjectDetails": student["editProjectDetails"],
                    "editProjectDetails2": student.get("p2editProjectDetails", False),
                    "section": student["section"],
                    "p2section": student["p2section"],
                    "selectedGuide": student["selectedGuide"],
                    "selectedGuideMailId": student["selectedGuideMailId"],
                }
            )

            projectDetails2.append(
                {
                    "projectTitle": student.get("p2projectTitle", ""),
                    "projectDesc": student.get("p2projectDesc", ""),
                    "projectDomain": student.get("p2projectDomain", ""),
                }
            )

            comments2.append(student.get("p2comments", []))
            # studentImage1 = student["image"]
            # studentImage2 = student["p2image"]
        else:
            comments2.append(student.get("p2comments", []))
            projectDetails2.append(
                {
                    "projectTitle": student.get("p2projectTitle", ""),
                    "projectDesc": student.get("p2projectDesc", ""),
                    "projectDomain": student.get("p2projectDomain", ""),
                }
            )
            studentData.append(
                {
                    # "student_id": str(student["_id"]),
                    "name": student["name"],
                    "team": student["team"],
                    "regNo": student["regNo"],
                    "phoneNo": student["phoneNo"],
                    "teamId": student["teamId"],
                    "section": student["section"],
                    "editProjectDetails": student["editProjectDetails"],
                    "editProjectDetails2": student.get("p2editProjectDetails", False),
                    "selectedGuide": student["selectedGuide"],
                    "selectedGuideMailId": student["selectedGuideMailId"],
                }
            )
            # studentImage1 = student["image"]

        projectDetails.append(
            {
                "projectTitle": student["projectTitle"],
                "projectDesc": student["projectDesc"],
                "projectDomain": student["projectDomain"],
            }
        )

        projectStatus.append(
            {
                "documentation": student["status"]["documentation"],
                "ppt": student["status"]["ppt"],
                "guideApproval": student["status"]["guideApproval"],
                "researchPaper": {
                    "approval": student["status"]["researchPaper"]["approval"],
                    "communicated": student["status"]["researchPaper"]["communicated"],
                    "accepted": student["status"]["researchPaper"]["accepted"],
                    "payment": student["status"]["researchPaper"]["payment"],
                },
            }
        )

        documentation.append(
            {
                "researchPaper": student["documentation"]["researchPaper"],
                "documentation": student["documentation"]["documentation"],
                "ppt": student["documentation"]["ppt"],
            }
        )

        comments.append(student["comments"])

    print(studentData)
    guideFilter = {"University EMAIL ID": studentData[0]["selectedGuideMailId"]}
    result = db["facultylist"].find(guideFilter)
    guideImage = ""
    # for r in result:
    # guideImage = r["IMAGE"]

    return jsonify(
        {
            "studentData": studentData,
            "projectDetails": projectDetails,
            "projectDetails2": projectDetails2,
            "projectStatus": projectStatus,
            "documentation": documentation,
            "guideImage": guideImage,
            "comments": comments[0],
            "comments2": comments2[0],
            # "studentImage1": studentImage1,
            # "studentImage2": studentImage2,
            "problemStatements": ps,
        }
    )


@app.route("/studentLogin/updateProjectDetails/<string:mailid>", methods=["POST"])
def updateProjectDetails(mailid):
    data = request.json
    updatedData = data.get("updatedData")
    student = data.get("student")
    teamId = data.get("teamId")
    registeredStudentsData = db["registeredStudentsData"]
    filter = {"teamId": teamId}
    if student == "p1":
        try:
            print(filter)
            updatedResult = registeredStudentsData.update_one(
                filter, {"$set": updatedData}
            )
            updatedResult = registeredStudentsData.update_one(
                filter, {"$set": {"editProjectDetails": False}}
            )
        except Exception as e:
            print(e)
        if updatedResult.modified_count >= 1:
            return jsonify({"message": "Success"})
        else:
            return jsonify({"message": "Fail"})

    if student == "p2":
        try:
            filter2 = {"teamId": teamId}
            updatedResult2 = registeredStudentsData.update_one(
                filter2, {"$set": updatedData}
            )
            updatedResult2 = registeredStudentsData.update_one(
                filter2, {"$set": {"p2editProjectDetails": False}}
            )
        except Exception as e:
            print(e)
        if updatedResult2.modified_count >= 1:
            return jsonify({"message": "Success"})
        else:
            return jsonify({"message": "Fail"})


@app.route("/staffLogin/check/<string:mailId>/<string:password1>", methods=["POST"])
def checkStaffLogin(mailId, password1):
    data = request.json
    password = data.get("passcode")
    facultycredentials = db["facultycredentials"]
    filter = {"mailId": mailId}
    result = facultycredentials.find_one(filter)
    print(result)
    if result:
        token = generate_token(mailId)
        if str(password) == str(result["password"]):
            return jsonify(
                {
                    "is_account_available": "true",
                    "Is_Password_Correct": "true",
                    "token": token,
                }
            )
        else:
            return jsonify(
                {"is_account_available": "true", "Is_Password_Correct": "false"}
            )
    else:
        return jsonify(
            {"is_account_available": "false", "Is_Password_Correct": "false"}
        )


@app.route("/staffLogin/getStudentsData/<string:mailid>", methods=["POST"])
def getStudentsdata(mailid):
    facultylist = db["facultylist"]
    filter = {"University EMAIL ID": mailid}
    allStudentMailIds = []
    guide = facultylist.find(filter)
    guideImg = ""
    for g in guide:
        print(g["allStudents"])
        allStudentMailIds = g["allStudents"]
        # guideImg = g["IMAGE"]

    allStudentsData = []
    registeredStudentsData = db["registeredStudentsData"]

    for studentMail in allStudentMailIds:
        filter = {"mailId": studentMail}
        studentData = registeredStudentsData.find(filter)
        for student in studentData:
            if student["team"]:
                allStudentsData.append(
                    {
                        "team": student["team"],
                        "projectId": student["teamId"],
                        # "studentOneImg": student["image"],
                        # "studentTwoImg": student["p2image"],
                        "regNoOne": student["regNo"],
                        "studentOne": student["name"],
                        "regNoTwo": student["p2regNo"],
                        "studentTwo": student["p2name"],
                        "section": student["section"],
                        "p2section": student["p2section"],
                        "projectTitle": student["projectTitle"],
                        "projectDomain": student["projectDomain"],
                    }
                )
            else:
                allStudentsData.append(
                    {
                        "team": student["team"],
                        "projectId": student["teamId"],
                        # "studentOneImg": student["image"],
                        "regNoOne": student["regNo"],
                        "studentOne": student["name"],
                        "section": student["section"],
                        "projectTitle": student["projectTitle"],
                        "projectDomain": student["projectDomain"],
                    }
                )

    return jsonify(
        {
            "message": "fetched successfully",
            "allStudentsData": allStudentsData,
            # "guideImg": guideImg,
        }
    )


@app.route(
    "/staffLogin/getProfileData/profile_details/<string:teamid>", methods=["POST"]
)
def getTeamdetails(teamid):
    registeredStudentsData = db["registeredStudentsData"]
    filter = {"teamId": teamid}
    team_data = registeredStudentsData.find_one(filter)
    if not team_data:
        return jsonify({"error": "Team not found"}), 404

    studentdetailsone = []
    studentdetailstwo = []

    projectdetails = []
    guidedetails = []

    projectdetails.append(
        {
            "title": team_data["projectTitle"],
            "desc": team_data["projectDesc"],
            "domain": team_data["projectDomain"],
            "projectApproval": team_data["editProjectDetails"],
        }
    )

    guidedetails.append(
        {
            "projectId": team_data["teamId"],
            "guideName": team_data["selectedGuide"],
            "guideMaidId": team_data["selectedGuideMailId"],
        }
    )

    if team_data["team"]:
        studentdetailsone.append(
            {
                # "imgOne": team_data["image"],
                "fullNameOne": team_data["name"],
                "team": team_data["team"],
                "regNoOne": team_data["regNo"],
                "secOne": team_data["section"],
                "emailOne": team_data["mailId"],
                "mobileNoOne": team_data["phoneNo"],
            }
        )
        studentdetailstwo.append(
            {
                "team": team_data["team"],
                "fullNameTwo": team_data["p2name"],
                "regNoTwo": team_data["p2regNo"],
                "mobileNoTwo": team_data["p2phoneNo"],
                "emailTwo": team_data["p2mailId"],
                "secTwo": team_data["p2section"],
                # "imgTwo": team_data["p2image"],
            }
        )
        return jsonify(
            {
                "studentDetailsOne": studentdetailsone[0],
                "studentDetailsTwo": studentdetailstwo[0],
                "projectdetails": projectdetails[0],
                "guidedetails": guidedetails[0],
                "type": team_data.get("projectType", ""),
            }
        )

    else:
        studentdetailsone.append(
            {
                "fullNameOne": team_data["name"],
                "team": team_data["team"],
                "regNoOne": team_data["regNo"],
                "emailOne": team_data["mailId"],
                "mobileNoOne": team_data["phoneNo"],
                "secOne": team_data["section"],
                # "imgOne": team_data["image"],
            }
        )

        return jsonify(
            {
                "studentDetailsOne": studentdetailsone[0],
                "projectdetails": projectdetails[0],
                "guidedetails": guidedetails[0],
                "type": team_data.get("projectType", ""),
            }
        )


@app.route(
    "/staffLogin/getProfileData/profile_details2/<string:teamid>", methods=["POST"]
)
def getTeamdetails2(teamid):
    registeredStudentsData = db["registeredStudentsData"]
    filter = {"teamId": teamid}
    team_data = registeredStudentsData.find_one(filter)
    if not team_data:
        return jsonify({"error": "Team not found"}), 404

    studentdetailsone = []
    studentdetailstwo = []

    projectdetails = []
    guidedetails = []

    projectdetails.append(
        {
            "title": team_data.get("p2projectTitle", ""),
            "desc": team_data.get("p2projectDesc", ""),
            "domain": team_data.get("p2projectDomain", ""),
            "projectApproval": team_data.get("p2editProjectDetails", ""),
        }
    )

    guidedetails.append(
        {
            "projectId": team_data["teamId"],
            "guideName": team_data["selectedGuide"],
            "guideMaidId": team_data["selectedGuideMailId"],
        }
    )

    if team_data["team"]:
        studentdetailsone.append(
            {
                # "imgOne": team_data["image"],
                "fullNameOne": team_data["name"],
                "team": team_data["team"],
                "regNoOne": team_data["regNo"],
                "secOne": team_data["section"],
                "emailOne": team_data["mailId"],
                "mobileNoOne": team_data["phoneNo"],
            }
        )
        studentdetailstwo.append(
            {
                "team": team_data["team"],
                "fullNameTwo": team_data["p2name"],
                "regNoTwo": team_data["p2regNo"],
                "mobileNoTwo": team_data["p2phoneNo"],
                "emailTwo": team_data["p2mailId"],
                "secTwo": team_data["p2section"],
                # "imgTwo": team_data["p2image"],
            }
        )
        return jsonify(
            {
                "studentDetailsOne": studentdetailsone[0],
                "studentDetailsTwo": studentdetailstwo[0],
                "projectdetails": projectdetails[0],
                "guidedetails": guidedetails[0],
                "type": team_data.get("p2projectType", ""),
            }
        )

    else:
        studentdetailsone.append(
            {
                "fullNameOne": team_data["name"],
                "team": team_data["team"],
                "regNoOne": team_data["regNo"],
                "emailOne": team_data["mailId"],
                "mobileNoOne": team_data["phoneNo"],
                "secOne": team_data["section"],
                # "imgOne": team_data["image"],
            }
        )

        return jsonify(
            {
                "studentDetailsOne": studentdetailsone[0],
                "projectdetails": projectdetails[0],
                "guidedetails": guidedetails[0],
                "type": team_data.get("p2projectType", ""),
            }
        )


@app.route("/staffLogin/getProfileData/<string:teamid>", methods=["POST"])
def get_profile_data(teamid):
    # print(request.json)
    registeredStudentsData = db["registeredStudentsData"]
    filter = {"teamId": teamid}
    profileCompleteData = registeredStudentsData.find(filter)

    # Initialize an empty list to store the results
    projectDetails = []
    projectMarks = []
    links = []
    documentation = []
    ppt = []
    researchPaper = []
    guideApproval = []
    isChecked = []
    comments = []
    comments2 = []

    for team in profileCompleteData:
        if team["team"]:
            projectDetails.append(
                {
                    # "studentOneImg": team["image"],
                    "studentOneName": team["name"],
                    "team": team["team"],
                    "studentOneRegNo": team["regNo"],
                    "studentOneSection": team["section"],
                    # "studentTwoImg": team["p2image"],
                    "studentTwoName": team["p2name"],
                    "studentTwoRegNo": team["p2regNo"],
                    "studentTwoSection": team["p2section"],
                    "projectId": team["teamId"],
                    "projectTitle": team["projectTitle"],
                    "projectDomain": team["projectDomain"],
                }
            )
        else:
            projectDetails.append(
                {
                    # "studentOneImg": team["image"],
                    "studentOneName": team["name"],
                    "team": team["team"],
                    "studentOneRegNo": team["regNo"],
                    "studentOneSection": team["section"],
                    "projectId": team["teamId"],
                    "projectTitle": team["projectTitle"],
                    "projectDomain": team["projectDomain"],
                }
            )
        if team["team"]:
            projectMarks.append(
                {"studentOneMarks": team["marks"], "studentTwoMarks": team["p2marks"]}
            )
        else:
            projectMarks.append({"studentOneMarks": team["marks"]})

        isChecked.append(
            {
                "researchPaper": {
                    "communicated": team["status"]["researchPaper"]["communicated"],
                    "accepted": team["status"]["researchPaper"]["accepted"],
                    "paymentDone": team["status"]["researchPaper"]["payment"],
                }
            }
        )

        documentation.append({"documentation": team["status"]["documentation"]})
        ppt.append({"ppt": team["status"]["ppt"]})
        researchPaper.append(
            {"researchPaper": {"approval": team["status"]["researchPaper"]["approval"]}}
        )
        guideApproval.append({"guideApproval": team["editProjectDetails"]})

        links.append(
            {
                "researchPaper": team["documentation"]["researchPaper"],
                "documentation": team["documentation"]["documentation"],
                "ppt": team["documentation"]["ppt"],
            }
        )

        comments.append(team["comments"])
        comments2.append(team.get("p2comments", []))

    return jsonify(
        {
            "projectDetails": projectDetails[0],
            "projectMarks": projectMarks[0],
            "links": links[0],
            "isChecked": isChecked[0],
            "documentation": documentation[0],
            "researchPaper": researchPaper[0],
            "ppt": ppt[0],
            "guideApproval": guideApproval[0],
            "comments": comments[0],
            "comments2": comments2[0],
        }
    )


@app.route("/staffLogin/updateProjectDetails/<string:teamid>", methods=["POST"])
def updateProjectDetailsStatus(teamid):
    updatedData = request.json
    registeredStudentsData = db["registeredStudentsData"]
    filter = {"teamId": teamid}

    approval_status = updatedData.get("approvalStatus", "")

    # updatedResult = registeredStudentsData.update_one(filter, {"$set": updatedData})

    if approval_status == "approved":
        updatedResult = registeredStudentsData.update_one(
            filter, {"$set": {"editProjectDetails": False}}
        )
    elif approval_status == "declined":
        updatedResult = registeredStudentsData.update_one(
            filter, {"$set": {"editProjectDetails": True}}
        )
    else:
        pass

    if updatedResult.modified_count == 1:
        return jsonify({"message": "Success"})
    else:
        return jsonify({"message": "Fail"})


@app.route("/staffLogin/updateProjectDetails2/<string:teamid>", methods=["POST"])
def updateProjectDetailsStatus2(teamid):
    updatedData = request.json
    registeredStudentsData = db["registeredStudentsData"]
    filter = {"teamId": teamid}

    approval_status = updatedData.get("approvalStatus", "")

    # updatedResult = registeredStudentsData.update_one(filter, {"$set": updatedData})

    if approval_status == "approved":
        updatedResult = registeredStudentsData.update_one(
            filter, {"$set": {"p2editProjectDetails": False}}
        )
    elif approval_status == "declined":
        updatedResult = registeredStudentsData.update_one(
            filter, {"$set": {"p2editProjectDetails": True}}
        )
    else:
        pass

    if updatedResult.modified_count == 1:
        return jsonify({"message": "Success"})
    else:
        return jsonify({"message": "Fail"})


@app.route(
    "/staffLogin/profiledetails/updatestatusDetails/<string:teamid>", methods=["POST"]
)
def updatestatusDetails(teamid):
    today_date = datetime.now().strftime("%d-%m-%Y")
    data = request.json
    print(data)
    status = {
        "documentation": data["editedDocumentationApproval"],
        "ppt": data["editedPptApproval"],
        "guideApproval": data["editedGuideApproval"],
        "researchPaper": {
            "approval": data["editedResearchApproval"],
            "communicated": data["editedCommunicationApproval"],
            "accepted": data["editedAcceptedApproval"],
            "payment": data["editedPaymentApproval"],
        },
    }

    if status["documentation"] == False:
        try:
            print("execute")
            col = db["registeredStudentsData"]
            doc = col.find_one({"teamId": teamid})
            details = doc["documentation"]
            details["documentation"] = ""
            col.update_one({"teamId": teamid}, {"$set": {"documentation": details}})
        except:
            pass

    if status["ppt"] == False:
        try:
            print("execute")
            col = db["registeredStudentsData"]
            doc = col.find_one({"teamId": teamid})
            details = doc["documentation"]
            details["ppt"] = ""
            col.update_one({"teamId": teamid}, {"$set": {"documentation": details}})
        except:
            pass

    if status["researchPaper"]["approval"] == False:
        try:
            print("execute")
            col = db["registeredStudentsData"]
            doc = col.find_one({"teamId": teamid})
            details = doc["documentation"]
            details["researchPaper"] = ""
            col.update_one({"teamId": teamid}, {"$set": {"documentation": details}})
        except:
            pass

    registeredStudentsData = db["registeredStudentsData"]
    filter = {"teamId": teamid}
    comment = {
        today_date: data.get("editedComments", ""),
    }

    comment2 = {
        today_date: data.get("editedComments2", ""),
    }

    try:
        if comment[today_date] == "":
            pass
        else:
            registeredStudentsData.update_one(
                filter,
                {"$push": {"comments": comment}},
            )

        if comment2[today_date] == "":
            pass
        else:
            registeredStudentsData.update_one(
                filter,
                {"$push": {"p2comments": comment2}},
            )

        # Update status and push comment to the 'comments' array
        registeredStudentsData.update_one(filter, {"$set": {"status": status}})

        # Update marks based on the condition
        filter = {"teamId": teamid}

        size = registeredStudentsData.find_one(filter)
        if size and size.get("team"):
            marks = {
                "marks": data.get("editedStudentOneMarks"),
                "p2marks": data.get("editedStudentTwoMarks"),
            }
        else:
            marks = {"marks": data.get("editedStudentOneMarks")}

        # Update the document with the new marks
        registeredStudentsData.update_one(filter, {"$set": marks})

        return jsonify({"message": "Success"})
    except:
        return jsonify({"message": "Fail"})


# Google Drive API credentials
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
SERVICE_ACCOUNT_FILE = "Credentials.json"

# Specify the folder ID where you want to upload the file
FOLDER_ID = "1u0t5YKNrHOIDFISlShezgIINcSJDkF9S"


UPLOAD_FOLDER = "uploads"  # Define the directory name for uploads

# Ensure the upload directory exists
upload_dir = os.path.join(app.root_path, UPLOAD_FOLDER)
os.makedirs(upload_dir, exist_ok=True)

# @app.route("/upload", methods=["POST"])
# def upload():
#     data = request.files.get("ppt")


@app.route("/studentLogin/uploadppt/<string:teamid>", methods=["PUT"])
def upload_ppt_file(teamid):
    try:
        data = request.form
        teamId = data.get("teamId")
        file = request.files.get("ppt")

        # Ensure file and teamId are present
        if not (file and teamId):
            return jsonify({"message": "Missing data parameters"}), 400

        # Save the file to the upload directory
        file_path_to_upload = os.path.join(
            upload_dir, f"{teamId}_ppt_{secure_filename(file.filename)}"
        )
        file.save(file_path_to_upload)

        # Upload file to Google Drive (Assuming the method driveAPI.upload_file_to_drive() is correctly implemented)
        file_name = os.path.basename(file_path_to_upload)
        file_id = driveAPI.upload_file_to_drive(
            file_path_to_upload, file_name, FOLDER_ID, SCOPES, SERVICE_ACCOUNT_FILE
        )

        # Get the file link
        ppt_file_link = f"https://drive.google.com/file/d/{file_id}"

        # Update the database with the file link
        filter = {"teamId": teamid}
        collection = db["registeredStudentsData"]
        doc = collection.find_one(filter)
        if doc:
            doc["documentation"]["ppt"] = ppt_file_link
            result = collection.update_one(
                filter, {"$set": {"documentation": doc["documentation"]}}
            )
            os.remove(file_path_to_upload)  # Cleanup: Delete the temporary file
            if result.modified_count:
                return jsonify({"message": "Success"}), 200
            else:
                return jsonify({"message": "Database update failed"}), 500
        else:
            os.remove(file_path_to_upload)  # Cleanup: Delete the temporary file
            return jsonify({"message": "Team ID not found"}), 404

    except Exception as e:
        return jsonify({"message": str(e)}), 500


@app.route("/studentLogin/uploaddoc/<string:teamid>", methods=["PUT"])
def upload_doc_file(teamid):
    try:
        data = request.form
        teamId = data.get("teamId")
        file = request.files.get("documentation")

        # Ensure file and teamId are present
        if not (file and teamId):
            return jsonify({"message": "Missing data parameters"}), 400

        # Save the file to the upload directory
        file_path_to_upload = os.path.join(
            upload_dir, f"{teamId}_documentation_{secure_filename(file.filename)}"
        )
        file.save(file_path_to_upload)

        # Upload file to Google Drive (Assuming the method driveAPI.upload_file_to_drive() is correctly implemented)
        file_name = os.path.basename(file_path_to_upload)
        file_id = driveAPI.upload_file_to_drive(
            file_path_to_upload, file_name, FOLDER_ID, SCOPES, SERVICE_ACCOUNT_FILE
        )

        # Get the file link
        documentation_file_link = f"https://drive.google.com/file/d/{file_id}"

        # Update the database with the file link
        filter = {"teamId": teamid}
        collection = db["registeredStudentsData"]
        doc = collection.find_one(filter)
        if doc:
            doc["documentation"]["documentation"] = documentation_file_link
            result = collection.update_one(
                filter, {"$set": {"documentation": doc["documentation"]}}
            )
            os.remove(file_path_to_upload)  # Cleanup: Delete the temporary file
            if result.modified_count:
                return jsonify({"message": "Success"}), 200
            else:
                return jsonify(
                    {"message": "Fail", "error": "Database update failed"}
                ), 500
        else:
            os.remove(file_path_to_upload)  # Cleanup: Delete the temporary file
            return jsonify({"message": "Fail", "error": "Team ID not found"}), 404

    except Exception as e:
        return jsonify({"message": "Fail", "error": str(e)}), 500


@app.route("/studentLogin/uploadrspaper/<string:teamid>", methods=["PUT"])
def upload_researchPaper_file(teamid):
    try:
        # Retrieve data from the request
        data = request.form
        teamId = data.get("teamId")
        file = request.files.get("researchPaper")

        # Ensure file and teamId are present
        if not (file and teamId):
            return jsonify({"message": "Missing data parameters"}), 400

        # Save the file to the upload directory
        file_path_to_upload = os.path.join(
            upload_dir, f"{teamId}_researchPaper_{secure_filename(file.filename)}"
        )
        file.save(file_path_to_upload)

        # Upload file to Google Drive (Assuming the method driveAPI.upload_file_to_drive() is correctly implemented)
        file_name = os.path.basename(file_path_to_upload)
        file_id = driveAPI.upload_file_to_drive(
            file_path_to_upload, file_name, FOLDER_ID, SCOPES, SERVICE_ACCOUNT_FILE
        )

        # Get the file link
        researchPaper_file_link = f"https://drive.google.com/file/d/{file_id}"

        # Update the database with the file link
        filter = {"teamId": teamid}
        collection = db["registeredStudentsData"]
        doc = collection.find_one(filter)
        if doc:
            doc["documentation"]["researchPaper"] = researchPaper_file_link
            result = collection.update_one(
                filter, {"$set": {"documentation": doc["documentation"]}}
            )
            os.remove(file_path_to_upload)  # Cleanup: Delete the temporary file
            if result.modified_count:
                return jsonify({"message": "Success"}), 200
            else:
                return jsonify(
                    {"message": "Fail", "error": "Database update failed"}
                ), 500
        else:
            os.remove(file_path_to_upload)  # Cleanup: Delete the temporary file
            return jsonify({"message": "Fail", "error": "Team ID not found"}), 404

    except Exception as e:
        return jsonify({"message": "Fail", "error": str(e)}), 500


@app.route("/staffCredential/<string:mailId>", methods=["POST"])
def staffchangepassword(mailId):
    updatecredentials = request.json
    print(updatecredentials)
    staffcredentials = db["facultycredentials"]
    filter = {"mailId": mailId}
    updatedResult = staffcredentials.update_one(filter, {"$set": updatecredentials})

    if updatedResult.modified_count == 1:
        return jsonify({"message": "Success"})
    else:
        return jsonify({"message": "Fail"})


@app.route(
    "/staffLogin/staffDashboard/fetchProblemStatements/<string:mailid>",
    methods={"POST"},
)
def fetchProblemStatements(mailid):
    filter = {"University EMAIL ID": mailid}
    collection = db["facultylist"]
    res = collection.find_one(filter)
    # print(res)
    ps = res.get("problemStatements", [])
    return jsonify({"message": "Success", "problemStatements": ps})


@app.route(
    "/staffLogin/staffDashboard/addProblemStatements/<string:mailid>", methods={"POST"}
)
def addProblemStatements(mailid):
    data = request.json
    # data = {"problemStatement":"something"}
    filter = {"University EMAIL ID": mailid}
    collection = db["facultylist"]
    res = collection.update_one(
        filter, {"$push": {"problemStatements": data["problemStatement"]}}
    )
    if res.modified_count == 1:
        return jsonify({"message": "Success"})
    else:
        return jsonify({"message": "Fail"})


# @app.route("/test", methods=["POST"])
# def test1():
#     filter1 = {"email" : "geddadavenkatapradeep@gmail.com"}
#     c = db["users"]
#     print(c.find_one(filter1)["image"])
#     return jsonify({'message': 'Fail'})


@app.route("/studentlogin/dashboard/change_password/<string:teamId>", methods=["POST"])
def studentchangepassword(teamId):
    updatecredentials = request.json
    registeredUsers = db["registeredUsers"]
    users = db["users"]
    registeredStudentsData = db["registeredStudentsData"]

    filter_registeredUsers = {"teamId": teamId}
    filter_users = {"teamId": teamId}
    filter_registeredStudentsData = {"teamId": teamId}

    updatedResult = registeredUsers.update_many(
        filter_registeredUsers, {"$set": updatecredentials}
    )
    updatedResult = users.update_many(filter_users, {"$set": updatecredentials})
    updatedResult = registeredStudentsData.update_one(
        filter_registeredStudentsData, {"$set": updatecredentials}
    )

    if updatedResult.modified_count >= 1:
        return jsonify({"message": "Success"})
    else:
        return jsonify({"message": "Fail"})


@app.route("/staffLogin/staffDashboard/selectStudent/<string:mailid>", methods=["POST"])
def selectStudentDirectlyByStaff(mailid):
    data = request.json
    print(data)

    if data.get("team"):
        try:
            generated_team_id = f"CSE-{str(datetime.now().year % 100 + 1)}-{str(int(data['regNo']) % 10000).rjust(4, '0')}"

            users_collection = db["users"]
            user = users_collection.find_one({"regNo": data["regNo"]})
            user2 = users_collection.find_one({"regNo": data["p2regNo"]})
            # print(f"{user=}")
            # print(f"{user2=}")

            # print(user)
        except Exception as e:
            print({"error": "register no not found"})
            return jsonify({"message": "Fail", "error": "register no not found"})

        if user:
            try:
                reggisterdusers_collection = db["registeredUsers"]
                reggisterdusers_collection.insert_one(
                    {
                        "email": user.get("email", ""),
                        "password": data.get("password", ""),
                        "guideMailId": mailid,
                        "update_vacancies_data": "",
                        "teamId": generated_team_id,
                    }
                )
                reggisterdusers_collection.insert_one(
                    {
                        "email": user2.get("email", ""),
                        "password": data.get("password", ""),
                        "guideMailId": mailid,
                        "update_vacancies_data": "",
                        "teamId": generated_team_id,
                    }
                )
            except Exception as e:
                print({"message": "Fail", "error": "student is already selected"})
                return jsonify(
                    {"message": "Fail", "error": "student is already selected"}
                )
            try:
                users_collection.update_one(
                    {"regNo": data["regNo"]},
                    {
                        "$set": {
                            "firstTime": False,
                            "teamId": generated_team_id,
                            "password": data.get("password", ""),
                        }
                    },
                )
                users_collection.update_one(
                    {"regNo": data["p2regNo"]},
                    {
                        "$set": {
                            "firstTime": False,
                            "teamId": generated_team_id,
                            "password": data.get("password", ""),
                        }
                    },
                )

                collection_data = {}

                # collection_data["image"] = user["image"]
                # collection_data["p2image"] = user2["image"]

                collection_data["teamId"] = generated_team_id
                status = {
                    "documentation": False,
                    "ppt": False,
                    "guideApproval": False,
                    "researchPaper": {
                        "approval": False,
                        "communicated": False,
                        "accepted": False,
                        "payment": False,
                    },
                }

                documents = {"researchPaper": None, "documentation": None, "ppt": None}

                comments = []

                collection_data["status"] = status
                collection_data["documentation"] = documents
                collection_data["comments"] = comments
                collection_data["editProjectDetails"] = True
                collection_data["marks"] = 0
                collection_data["p2marks"] = 0

                collection_data["password"] = data.get("password", "")
                collection_data["team"] = True
                collection_data["name"] = user["Full Name"]
                collection_data["regNo"] = user["regNo"]
                collection_data["phoneNo"] = user.get("Mobile Number", "")
                collection_data["mailId"] = user.get("email")
                collection_data["section"] = user.get("section", "")

                collection_data["p2name"] = user2["Full Name"]
                collection_data["p2regNo"] = user2["regNo"]
                collection_data["p2phoneNo"] = user2.get("Mobile Number", "")
                collection_data["p2mailId"] = user2.get("email")
                collection_data["p2section"] = user2.get("section", "")

                collection_data["projectTitle"] = ""
                collection_data["projectDesc"] = ""
                collection_data["projectDomain"] = ""
                # collection_data["selectedGuide"] = ""
                collection_data["selectedGuideMailId"] = mailid

                # registeredStudents_collection = db["registeredStudentsData"]
                # registeredStudents_collection.insert_one(collection_data)

                faculty_collection = db["facultylist"]
                document = faculty_collection.find_one(
                    {"University EMAIL ID": collection_data["selectedGuideMailId"]}
                )
                updated_data = {"allStudents": [], "allTeams": []}
                collection_data["selectedGuide"] = document["NAME OF THE FACULTY"]

                registeredStudents_collection = db["registeredStudentsData"]
                registeredStudents_collection.insert_one(collection_data)

                if document:
                    if "allStudents" in document:
                        document["allStudents"].append(user.get("email"))
                        document["allStudents"].append(user2.get("email"))

                    else:
                        document["allStudents"] = [
                            user.get("email"),
                            user2.get("email"),
                        ]

                    if "allTeams" in document:
                        document["allTeams"].append(generated_team_id)
                    else:
                        document["allTeams"] = [generated_team_id]

                updated_data["allStudents"] = document["allStudents"]
                updated_data["allTeams"] = document["allTeams"]

                # print(filter_data, updated_data)

                # Update the data in the collection

                # result = faculty_collection.update_one({ "University EMAIL ID": collection_data["selectedGuideMailId"] }, {'$set': updated_data})
                vacancies = document["TOTAL BATCHES"]
                maxTeams = document["MAX TEAMS"]

                result = faculty_collection.update_one(
                    {"University EMAIL ID": collection_data["selectedGuideMailId"]},
                    {"$set": updated_data},
                )
                result = faculty_collection.update_one(
                    {"University EMAIL ID": collection_data["selectedGuideMailId"]},
                    {
                        "$set": {
                            "MAX TEAMS": maxTeams - 1,
                            "TOTAL BATCHES": vacancies - 1,
                        }
                    },
                )

                # try:
                #     msg = Message(f'Guide Selection Confirmation',  # Email subject
                #                 sender='guideselection.cse@sathyabama.ac.in',  # Replace with your email address
                #                 recipients=[user.get("email"), user2.get("email")])  # Replace with the recipient's email address
                #     msg.html = f"""
                #     <html>
                #     <body>
                #         <p>Dear {collection_data['name']} and {collection_data['p2name']},</p>
                #         <p>We are delighted to announce that a new guide has been assigned to oversee your project. As a result, we kindly request you to log in to the student dashboard at your earliest convenience. Once logged in, please review the problem statements provided by your newly assigned guide.</p>
                #         <b>Guide Details:</b><br/>
                #         <ul>
                #         <li>Guide Name - {collection_data["selectedGuide"]}</li>
                #         </ul><br/>

                #         <ul>
                #         <b>Login Credentials:</b><br/>
                #         <li>Project Id - {generated_team_id}</li>
                #         <li>Password - {data.get("password")}</li>
                #         </ul><br/>
                #         <p>We understand that your previous guide has resigned, and we apologize for any confusion that may have arisen from this transition. Rest assured, your new guide is fully committed to supporting you throughout the remainder of your project.</p>
                #         <br/>
                #         <p>Thank you for your attention to this matter. Should you have any questions or require further assistance, please do not hesitate to contact us.</p>
                #         <p>Your guide will review your submission and provide further guidance and feedback.</p><br/><br/><br/>
                #         <p>Best Regards,</p>
                #         <p>School of Computing,</p>
                #         <p>Sathyabama Institute of Science & Technology</p>
                #     </body>
                #     </html>
                #     """

                #     mail.send(msg)
                #     return jsonify({"Is_Email_sent":"true", "message":"Success", "status":"Collection created and data inserted successfully!"})
                # except Exception as e:
                #     print(e)
                #     return jsonify({"Is_Email_sent":"false","message": "Collection created and data inserted successfully!"})

                return jsonify(
                    {
                        "Is_Email_sent": "false",
                        "message": "Success",
                        "status": "Collection created and data inserted successfully!",
                    }
                )

            except Exception as e:
                print(e)
                return jsonify({"message": "Fail", "error": "failed to select student"})
        else:
            print({"message": "Fail", "error": "register no not found"})
            return jsonify({"message": "Fail", "error": "register no not found"})
    else:
        try:
            generated_team_id = f"CSE-{str(datetime.now().year % 100 + 1)}-{str(int(data['regNo']) % 10000).rjust(4, '0')}"

            users_collection = db["users"]
            user = users_collection.find_one({"regNo": data["regNo"]})
            print(user)
        except Exception as e:
            return jsonify({"message": "Fail", "error": "register no not found"})

        if user:
            try:
                reggisterdusers_collection = db["registeredUsers"]
                reggisterdusers_collection.insert_one(
                    {
                        "email": user.get("email", ""),
                        "password": data.get("password", ""),
                        "guideMailId": mailid,
                        "update_vacancies_data": "",
                        "teamId": generated_team_id,
                    }
                )
            except Exception as e:
                return jsonify(
                    {"message": "Fail", "error": "student is already selected"}
                )
            try:
                users_collection.update_one(
                    {"regNo": data["regNo"]},
                    {
                        "$set": {
                            "firstTime": False,
                            "teamId": generated_team_id,
                            "password": data.get("password", ""),
                        }
                    },
                )

                collection_data = {}

                # collection_data["image"] = user["image"]
                collection_data["teamId"] = generated_team_id
                status = {
                    "documentation": False,
                    "ppt": False,
                    "guideApproval": False,
                    "researchPaper": {
                        "approval": False,
                        "communicated": False,
                        "accepted": False,
                        "payment": False,
                    },
                }

                documents = {"researchPaper": None, "documentation": None, "ppt": None}

                comments = []

                collection_data["status"] = status
                collection_data["documentation"] = documents
                collection_data["comments"] = comments
                collection_data["editProjectDetails"] = True
                collection_data["marks"] = 0
                collection_data["password"] = data.get("password", "")
                collection_data["team"] = False
                collection_data["name"] = user["Full Name"]
                collection_data["regNo"] = user["regNo"]
                collection_data["phoneNo"] = user.get("Mobile Number", "")
                collection_data["mailId"] = user.get("email")
                collection_data["section"] = user.get("section", "")
                collection_data["projectTitle"] = ""
                collection_data["projectDesc"] = ""
                collection_data["projectDomain"] = ""
                # collection_data["selectedGuide"] = data.get("selectedGuide", "")
                collection_data["selectedGuideMailId"] = mailid

                # registeredStudents_collection = db["registeredStudentsData"]
                # registeredStudents_collection.insert_one(collection_data)

                faculty_collection = db["facultylist"]
                document = faculty_collection.find_one(
                    {"University EMAIL ID": collection_data["selectedGuideMailId"]}
                )
                updated_data = {"allStudents": [], "allTeams": []}

                collection_data["selectedGuide"] = document["NAME OF THE FACULTY"]

                registeredStudents_collection = db["registeredStudentsData"]
                registeredStudents_collection.insert_one(collection_data)

                if document:
                    if "allStudents" in document:
                        document["allStudents"].append(user.get("email"))
                    else:
                        document["allStudents"] = [user.get("email")]

                    if "allTeams" in document:
                        document["allTeams"].append(generated_team_id)
                    else:
                        document["allTeams"] = [generated_team_id]

                updated_data["allStudents"] = document["allStudents"]
                updated_data["allTeams"] = document["allTeams"]

                # print(filter_data, updated_data)

                # Update the data in the collection
                vacancies = document["TOTAL BATCHES"]
                maxTeams = document["MAX TEAMS"]

                result = faculty_collection.update_one(
                    {"University EMAIL ID": collection_data["selectedGuideMailId"]},
                    {"$set": updated_data},
                )
                result = faculty_collection.update_one(
                    {"University EMAIL ID": collection_data["selectedGuideMailId"]},
                    {
                        "$set": {
                            "MAX TEAMS": maxTeams - 1,
                            "TOTAL BATCHES": vacancies - 1,
                        }
                    },
                )

                # try:
                #     msg = Message(f'Guide Selection Confirmation',  # Email subject
                #                 sender='guideselection.cse@sathyabama.ac.in',  # Replace with your email address
                #                 recipients=[user.get("email")])  # Replace with the recipient's email address
                #     msg.html = f"""
                #     <html>
                #     <body>
                #         <p>Dear {collection_data['name']},</p>
                #         <p>We are delighted to announce that a new guide has been assigned to oversee your project. As a result, we kindly request you to log in to the student dashboard at your earliest convenience. Once logged in, please review the problem statements provided by your newly assigned guide.</p>
                #         <b>Guide Details:</b><br/>
                #         <ul>
                #         <li>Guide Name - {collection_data["selectedGuide"]}</li>
                #         </ul><br/>

                #         <ul>
                #         <b>Login Credentials:</b><br/>
                #         <li>Project Id - {generated_team_id}</li>
                #         <li>Password - {data.get("password")}</li>
                #         </ul><br/>
                #         <p>We understand that your previous guide has resigned, and we apologize for any confusion that may have arisen from this transition. Rest assured, your new guide is fully committed to supporting you throughout the remainder of your project.</p>
                #         <br/>
                #         <p>Thank you for your attention to this matter. Should you have any questions or require further assistance, please do not hesitate to contact us.</p>
                #         <p>Your guide will review your submission and provide further guidance and feedback.</p><br/><br/><br/>
                #         <p>Best Regards,</p>
                #         <p>School of Computing,</p>
                #         <p>Sathyabama Institute of Science & Technology</p>
                #     </body>
                #     </html>
                #     """

                #     mail.send(msg)
                #     return jsonify({"Is_Email_sent":"true", "message":"Success", "status":"Collection created and data inserted successfully!"})
                # except Exception as e:
                #     print(e)
                #     return jsonify({"Is_Email_sent":"false","message": "Collection created and data inserted successfully!"})
                return jsonify(
                    {
                        "Is_Email_sent": "false",
                        "message": "Success",
                        "status": "Collection created and data inserted successfully!",
                    }
                )

            except Exception as e:
                return jsonify({"message": "Fail", "error": "failed to select student"})
        else:
            return jsonify({"message": "Fail", "error": "register no not found"})


@app.route("/staffLogin/staffDashboard/fetchMaxTeams/<string:mailid>", methods=["POST"])
def fetchmaxteam(mailid):
    facultylist = db["facultylist"]
    filter = {"University EMAIL ID": mailid}
    findmaxteam = facultylist.find_one(filter)

    return jsonify({"maxTeams": findmaxteam["MAX TEAMS"]})


@app.route("/admin_login/check", methods=["POST"])
def checkAdminLogin():
    data = request.json
    mailId = data.get("mailId")
    password = data.get("passcode")
    AdminCredentials = db["AdminCredentials"]
    filter = {"mailId": mailId}
    result = AdminCredentials.find_one(filter)
    print(result)
    if result:
        token = generate_token(mailId)
        if str(password) == result["password"]:
            return jsonify(
                {
                    "is_account_available": "true",
                    "Is_Password_Correct": "true",
                    "token": token,
                }
            )
        else:
            return jsonify(
                {"is_account_available": "true", "Is_Password_Correct": "false"}
            )
    else:
        return jsonify(
            {"is_account_available": "false", "Is_Password_Correct": "false"}
        )


teams_collection = db["registeredStudentsData"]  # Collection where team data is stored
users_collection = db["users"]  # Users collection
registered_users_collection = db["registeredUsers"]  # Registered users collection
faculty_list_collection = db["facultylist"]  # Faculty list collection
individual_registered_collection = db["individulregdata"]


@app.route("/admin/deleteTeam", methods=["POST"])
def deleteTeam():
    data = request.json
    teamId = data["teamId"]

    try:
        # Step 1: Remove the document from the "teams" collection and store the `guidemailid`
        team = teams_collection.find_one_and_delete({"teamId": teamId})
        if not team:
            return jsonify({"error": f"Team with ID {teamId} not found"}), 404

        guidemailid = team["selectedGuideMailId"]
        studentmails = [team["mailId"]]
        if team["team"]:
            studentmails.append(team["p2mailId"])

        print("step1 success")

        # Step 2: Update the "users" collection
        users_collection.update_many(
            {"teamId": teamId}, {"$unset": {"teamId": ""}, "$set": {"firstTime": True}}
        )

        print("step2 success")

        # Step 3: Remove records from the "registeredusers" collection
        registered_users_collection.delete_many({"teamId": teamId})

        print("step3 success")

        # Step 4: Update the "facultylist" collection
        print(guidemailid)
        faculty_list_collection.update_one(
            {"University EMAIL ID": guidemailid},
            {
                "$inc": {"TOTAL BATCHES": 1, "MAX TEAMS": 1},
                "$pull": {
                    "allTeams": str(teamId),
                    "allStudents": {"$in": studentmails},
                },
            },
        )

        print("step4 success")

        # Step 5: Remove records from the "registeredusers" collection
        # individual_registered_collection.delete_many({"Team ID": teamId})

        print("step5 success")

        return jsonify(
            {
                "message": f"Successfully removed team {teamId} and updated related records"
            }
        ), 200
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route("/admin/update_faculty_vacancies", methods=["POST"])
def update_vacancies():
    data = request.get_json()
    faculty_email = data["faculty"]
    new_vacancies = int(data["vacancies"])

    faculty_list_collection = db["facultylist"]

    # Find the faculty
    faculty_doc = faculty_list_collection.find_one(
        {"University EMAIL ID": faculty_email}
    )
    if not faculty_doc:
        return jsonify({"message": "Faculty not found."}), 404

    current_vacancies = faculty_doc.get("TOTAL BATCHES", 0)

    updated_vacancies = current_vacancies + new_vacancies

    # Update in DB
    faculty_list_collection.update_one(
        {"University EMAIL ID": faculty_email},
        {"$set": {"TOTAL BATCHES": updated_vacancies}},
    )

    return jsonify({"message": f"Vacancies updated to {updated_vacancies}."}), 200


# @app.route("/admin/get_faculty_details", methods=["GET"])
# def get_faculty_details():
#     collection = db.facultylist
#     try:
#         page = int(request.args.get("page", 1))
#         limit = int(request.args.get("limit", 10))
#         search = request.args.get("search", "").strip()
#         skip = (page - 1) * limit

#         query = {}
#         if search:
#             query["NAME OF THE FACULTY"] = {"$regex": search, "$options": "i"}

#         total_count = collection.count_documents(query)
#         data = collection.find(query).skip(skip).limit(limit)

#         result = []
#         for i, document in enumerate(data, start=skip + 1):
#             result.append(
#                 {
#                     "id": i,
#                     "NAME": document["NAME OF THE FACULTY"],
#                     "VACANCIES": document["TOTAL BATCHES"],
#                     "MaxTeams": document["MAX TEAMS"],
#                     "DESIGNATION": document["DESIGNATION"],
#                     "DOMAIN1": document["DOMAIN 1"],
#                     "DOMAIN2": document["DOMAIN 2"],
#                     "DOMAIN3": document["DOMAIN 3"],
#                     "UniversityEMAILID": document["University EMAIL ID"],
#                     "IMAGE": document["IMAGE"],
#                     "EMPID": document["EMP ID"],
#                 }
#             )

#         return jsonify(
#             {
#                 "total": total_count,
#                 "page": page,
#                 "limit": limit,
#                 "totalPages": (total_count + limit - 1) // limit,
#                 "guides": result,
#             }
#         )

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


@app.route("/admin/get_faculty_details", methods=["GET"])
def get_faculty_details():
    collection = db.facultylist
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 10))
        search = request.args.get("search", "").strip()
        skip = (page - 1) * limit

        query = {}
        if search:
            query["NAME OF THE FACULTY"] = {"$regex": search, "$options": "i"}

        total_count = collection.count_documents(query)
        data = collection.find(query).skip(skip).limit(limit)

        result = []
        for i, doc in enumerate(data, start=skip + 1):
            result.append(
                {
                    "id": i,
                    "NAME": doc.get("NAME OF THE FACULTY", ""),
                    "VACANCIES": doc.get("TOTAL BATCHES", 0),
                    "MaxTeams": doc.get("MAX TEAMS", 0),
                    "DESIGNATION": doc.get("DESIGNATION", ""),
                    "DOMAIN1": doc.get("DOMAIN 1", ""),
                    "DOMAIN2": doc.get("DOMAIN 2", ""),
                    "DOMAIN3": doc.get("DOMAIN 3", ""),
                    "UniversityEMAILID": doc.get("University EMAIL ID", ""),
                    "IMAGE": doc.get("IMAGE", ""),
                    "EMPID": doc.get("EMP ID", ""),
                }
            )

        return jsonify(
            {
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": (total_count + limit - 1) // limit,
                "guides": result,
            }
        )

    except Exception as e:
        print("🔥 /admin/get_faculty_details ERROR:", e)
        return jsonify({"error": "Something went wrong"}), 500


@app.route("/admin/add_faculty", methods=["POST"])
def add_faculty():
    facultylist_collection = db["facultylist"]
    facultycredentials_collection = db["facultycredentials"]

    try:
        # Get data from request
        data = request.get_json()

        # Validate required fields
        required_fields = [
            "name",
            "email",
            "empId",
            "designation",
            "totalBatches",
            "maxTeams",
            "password",
        ]
        for field in required_fields:
            if not data.get(field):
                return jsonify({"message": f"{field} is required"}), 400

        # Validate domain1 is provided (at least one domain required)
        if not data.get("fieldOfInterest") or not data.get("fieldOfInterest").strip():
            return jsonify(
                {"message": "At least one field of interest domain is required"}
            ), 400

        # Check if faculty already exists
        existing_faculty = facultylist_collection.find_one(
            {
                "$or": [
                    {"University EMAIL ID": data["email"]},
                    {"EMP ID": int(data["empId"])},
                ]
            }
        )

        if existing_faculty:
            return jsonify(
                {"message": "Faculty with this email or Employee ID already exists"}
            ), 400

        # Get the next serial number
        last_faculty = facultylist_collection.find_one(sort=[("SL.NO", -1)])
        next_sl_no = (last_faculty["SL"]["NO"] + 1) if last_faculty else 1

        # Parse domains from fieldOfInterest
        domains = [
            domain.strip()
            for domain in data["fieldOfInterest"].split(";")
            if domain.strip()
        ]

        # Ensure we have at least one domain and pad with empty strings if needed
        while len(domains) < 3:
            domains.append("")

        # Create faculty document structure
        faculty_document = {
            "SL": {"NO": next_sl_no},
            "NAME OF THE FACULTY": data["name"],
            "TOTAL BATCHES": int(data["totalBatches"]),
            "MAX TEAMS": int(data["maxTeams"]),
            "EMP ID": int(data["empId"]),
            "NAME_Capital letters as per Guide List": data["name"].upper(),
            "DESIGNATION": data["designation"].upper(),
            "DOMAIN 1": domains[0] if len(domains) > 0 else "",
            "DOMAIN 2": domains[1] if len(domains) > 1 else "",
            "DOMAIN 3": domains[2] if len(domains) > 2 else "",
            "University EMAIL ID": data["email"],
            "IMAGE": data["imageDriveLink"],
        }

        # Create credentials document (same as faculty document for your structure)
        # credentials_document = faculty_document.copy()

        # Insert into both collections
        facultylist_result = facultylist_collection.insert_one(faculty_document)
        # facultycredentials_result = facultycredentials_collection.insert_one(
        #     credentials_document
        # )

        # Optionally, you might want to store hashed password separately
        # If you need a separate password collection, uncomment below:
        credentials_document = {
            # "faculty_id": facultylist_result.inserted_id,
            "mailId": data["email"],
            "name": data["name"],
            "password": data["password"],
            # "created_at": datetime.utcnow()
        }
        facultycredentials_collection.insert_one(credentials_document)

        return jsonify(
            {
                "message": f"Faculty {data['name']} added successfully!",
                "faculty_id": str(facultylist_result.inserted_id),
                "sl_no": next_sl_no,
            }
        ), 201

    except ValueError as ve:
        return jsonify({"message": f"Invalid data format: {str(ve)}"}), 400
    except Exception as e:
        return jsonify({"message": f"Error adding faculty: {str(e)}"}), 500


if __name__ == "__main__":
    app.debug = True
    app.run()
