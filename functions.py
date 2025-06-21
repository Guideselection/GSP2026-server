from flask import Flask, render_template, request, jsonify
import csv
from pymongo import MongoClient
from bson.objectid import ObjectId
from flask_cors import CORS
from flask_mail import Mail, Message
from pymongo.errors import InvalidOperation, DuplicateKeyError
import random
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaFileUpload
import os
from werkzeug.utils import secure_filename
import random
import string
from google.oauth2 import service_account


SCOPES = ["https://www.googleapis.com/auth/drive.file"]


client = MongoClient(str(os.getenv("MONGO_URI")))
# print(os.getenv("MONGO_URI"))

# db = client.cse_gsp_21_25
db = client.cse_gsp_22_26

# from googleapiclient.discovery import build


def authenticate():
    creds = service_account.Credentials.from_service_account_file(
        "Credentials.json",  # Update with your service account file path
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return creds


def get_entire_row(file_path, row_number):
    try:
        # Open the CSV file
        with open(file_path, "r", newline="") as csvfile:
            # Create a CSV reader object
            csv_reader = csv.reader(csvfile)

            # Skip to the specified row
            for _ in range(row_number - 1):
                next(csv_reader)

            # Read the specified row
            row_data = next(csv_reader)

            # Assuming the first row contains headers
            with open(file_path, "r", newline="") as csvfile:
                csv_reader = csv.reader(csvfile)
                headers = next(csv_reader)

            # Create a dictionary with headers as keys and row data as values
            row_dict = dict(zip(headers, row_data))

            # print(row_dict)
            return row_dict

    except Exception as e:
        print(f"An error occurred: {e}")


def get_google_drive_link(service, file_name):
    # Search for the file by name
    # folder_name = "All Students Images"
    folder_name = "2025 CSE All Students Images"
    # folder_name = "2026 CSE All Students Images"
    folder_query = (
        f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
    )
    folder_results = service.files().list(q=folder_query, fields="files(id)").execute()
    folders = folder_results.get("files", [])

    if not folders:
        print(f"No folder found with the name '{folder_name}' in Google Drive.")
        return None

    folder_id = folders[0]["id"]
    query = f"name contains '{file_name}' and '{folder_id}' in parents"
    results = service.files().list(q=query, fields="files(id,webViewLink)").execute()
    files = results.get("files", [])

    if not files:
        print(
            f"No file found with the name starting with '{file_name}' in the folder '{folder_name}'."
        )
        return None

    file_id = files[0]["id"]
    web_view_link = files[0]["webViewLink"]

    return web_view_link


creds = authenticate()
service = build("drive", "v3", credentials=creds)
# print(get_google_drive_link(service, 41111354))


# db = client.SIST_Courses


# faild = []
# def insertSectionToStudentDocument(regNo, section):
#     try:
#         collection = db["users"]
#         res = collection.update_one({"regNo":regNo}, {"$set":{"section":section}})
#         if res:
#             pass
#         else:
#             faild.append(regNo)
#     except:
#         faild.append(regNo)

# Example usage

failed = []


def insertImagesToStudentDocument(regNo, image):
    try:
        collection = db["users"]
        res = collection.update_one({"regNo": regNo}, {"$set": {"image": image}})
        if res:
            pass
        else:
            failed.append(regNo)
    except:
        failed.append(regNo)


# file_path = 'updatedstudentdetails.csv'
# for i in range(10,1484):
#     row_number = i  # Change this to the desired row number

#     stData = get_entire_row(file_path, row_number)
#     print(stData["regNo"])
#     regNo = stData["regNo"]
#     image = get_google_drive_link(service, regNo)
#     insertImagesToStudentDocument(int(regNo), image)

# # print(faild)
# print(failed)


# def import_csv_to_mongodb(csv_file, db_name, collection_name):
#     try:
#         collection = db_name[collection_name]

#         # Open CSV file and read data
#         with open(csv_file, "r", newline="", encoding="utf-8") as file:
#             csv_reader = csv.DictReader(file)  # Reads rows as dictionaries
#             data = list(csv_reader)  # Convert to list of dictionaries

#             if data:
#                 # Convert numeric values properly (MongoDB stores numbers as strings otherwise)
#                 for row in data:
#                     for key in row:
#                         # Try to convert numeric values
#                         if row[key].isdigit():
#                             row[key] = int(row[key])

#                 collection.insert_many(data)  # Insert all rows into MongoDB
#                 print(
#                     f"Imported {len(data)} records into '{collection_name}' collection!"
#                 )
#             else:
#                 print("CSV file is empty.")

#     except Exception as e:
#         print(f"Error: {e}")


# # Ensure the database name is a string
# import_csv_to_mongodb("SIST-PC-DB-2026-CSE-DB.csv", db, "users")
# import_csv_to_mongodb("../GSP Upgraded Data/SIST-PC-DB-2026-CSE-DB.csv", db, "users")


def get_all_rows_as_dict(file_path):
    try:
        # Initialize an empty list to store each row as a dictionary
        all_rows = []

        # Open the CSV file once with DictReader to access headers easily
        with open(file_path, "r", newline="") as csvfile:
            csv_reader = csv.DictReader(csvfile)  # Read the file into a dictionary

            # Iterate through each row and append it to the list
            for row in csv_reader:
                all_rows.append(row)  # Add the row as a dictionary to the list

        # Return the list of rows (each as a dictionary)
        return all_rows

    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def insert_rows_to_mongodb(rows_data, db_name, collection_name):
    try:
        collection = db_name[collection_name]

        # Insert the rows into MongoDB
        if rows_data:
            collection.insert_many(rows_data)
            print(
                f"Successfully inserted {len(rows_data)} documents into '{collection_name}' collection."
            )
        else:
            print("No data to insert.")

    except Exception as e:
        print(f"Error: {e}")


# Get the data from CSV
# rows_data = get_all_rows_as_dict("SIST-PC-DB-2026-CSE-DB.csv")
rows_data = get_all_rows_as_dict("../GSP Upgraded Data/SIST-PC-DB-2026-CSE-DB.csv")

# Insert the data into MongoDB
insert_rows_to_mongodb(rows_data, db, "users")
