# Stdlibs
import os
import pika
import json
import logging
import requests

# Custom code
from models.p360_case import P360Case
from models.p360_contact import P360Contact


class P360Client:

    def __init__(self, log=None, api_base_uri=None, api_key=None, http_timeout=30):
        
        if log:
            self.log = log
            self.log.info("The Public 360 client is using the provided log handler")
        else:
            log_name = os.environ.get("LOG_NAME", "DEFAULT_LOG")
            log_level = os.environ.get("LOG_LEVEL", "INFO")
            self.log = logging.getLogger(log_name)
            self.log.setLevel(log_level)
            self.log.info("P360 client: Created new log \"" + self.log.name + "\" with log level " + logging.getLevelName(self.log.level))
            
        self.log.info("Initializing the P360 client with base URI " + api_base_uri + " and timeout " + str(http_timeout))
        self.log.debug("Base URI: " + api_base_uri)
        self.log.debug("API key: " + api_key)
        self.api_base_uri = api_base_uri
        self.api_key = api_key

        self.http_timeout = http_timeout

    def get_contact_person_by_email(self, user_email):
        self.log.info("Searching for a P360 user with email \"" + user_email + "\"...")

        url = self.api_base_uri + "/ContactService/GetContactPersons?authkey=" + self.api_key

        post_data = {"parameter": {
            "Email": user_email 
        }}

        self.log.info("Running HTTP POST to " + url + ", with this body content: " + str(post_data))
        try:
            response = requests.post(url, json=post_data, timeout=self.http_timeout)
        except Exception as e:
            self.log.error("ERROR: " + str(e))
            raise Exception("Failed to POST data to " + url + ". Error message: " + str(e))

        response_object = self.validate_response(response, url)

        contacts = response_object["ContactPersons"]
        if len(contacts) > 1:
            raise Exception("The P360 returned more than one contact persons. Expected 0 or 1.")

        elif len(contacts) == 1:
            contact_person = contacts[0]
            recno = contact_person["Recno"]
            email = contact_person["Email"]
            return P360Contact(email=email, recno=recno)
        else:
            return None

    def validate_response(self, response, url):
        status_code = response.status_code
        self.log.debug("API response status code: " + str(status_code))
        if not status_code == 200:
            raise Exception("The request to " + url + " returned status code " + str(status_code))
        response_object = response.json()
        if not response_object["Successful"]:
            raise Exception("The request to " + url + " returned status code " + str(
                status_code) + ". Response from the API was this: " + str(response_object))
        self.log.debug("API response: " + str(response_object))
        return response_object

    def get_case_by_pnr_and_access_group(self, pnr, access_group_filter):
        self.log.info("Searching for a P360 case based on pnr \"" + pnr + "\" and access group \"" + access_group_filter + "\"...")

        url = self.api_base_uri + "/CaseService/GetCases?authkey=" + self.api_key
        post_data = {
            "parameter": {
                "ArchiveCode": pnr
            }
        }

        self.log.info("Running HTTP POST to " + url + ", with this body content: " + str(post_data))
        try:
            response = requests.post(url, json=post_data, timeout=self.http_timeout)
        except Exception as e:
            raise Exception("Failed to POST data to " + url + ". Error message: " + str(e))

        response_object = self.validate_response(response, url)

        cases = response_object["Cases"]

        existing_case = None

        # The person may have a case registered with multiple units. Loop through the cases and fetch the one with
        # the correct access group.
        for case in cases:
            this_access_group = case["AccessGroup"]
            if this_access_group == access_group_filter:
                existing_case = case

        if existing_case is None:
            raise Exception("Could not find any cases matching prn " + pnr + " and access group " + access_group_filter)

        recno = existing_case["Recno"]
        case_number = existing_case["CaseNumber"]
        access_group_filter = existing_case["AccessGroup"]

        try:
            responsible_person_email = existing_case["ResponsiblePerson"]["Email"]
            responsible_person_recno = existing_case["ResponsiblePerson"]["Recno"]
        except:
            raise RuntimeError("Could not find responsible person email in existing case " + str(case_number))

        return P360Case(responsible_person_email=responsible_person_email,
                        responsible_person_recno=responsible_person_recno,
                        access_group=access_group_filter,
                        case_number=case_number,
                        case_recno=recno)

    def get_case_by_title(self, case_title):
        self.log.info("Searching for a P360 case with title \"" + case_title + "\"...")

        url = self.api_base_uri + "/CaseService/GetCases?authkey=" + self.api_key
        post_data = {"parameter": {
            "Title": case_title
        }}

        self.log.info("Running HTTP POST to " + url + ", with this body content: " + str(post_data))
        try:
            response = requests.post(url, json=post_data, timeout=self.http_timeout)
        except Exception as e:
            raise Exception("Failed to POST data to " + url + ". Error message: " + str(e))

        response_object = self.validate_response(response, url)

        cases = response_object["Cases"]
        if len(cases) > 1:
            raise Exception("The P360 returned more than one case. Expected 0 or 1 case.")

        elif len(cases) == 1:
            existing_case = cases[0]
            recno = existing_case["Recno"]
            case_number = existing_case["CaseNumber"]
            return P360Case(case_number=case_number, case_recno=recno)
        else:
            return None

    def create_case(self, case_title, responsible_person_recno, access_group, pnr):
        self.log.info("Creating new P360 case \"" + case_title + "\"...")

        url = self.api_base_uri + "/CaseService/CreateCase?authkey=" + self.api_key

        post_data = {"parameter": {
            "Title": case_title,
            "ResponsiblePersonRecno": responsible_person_recno,
            "AccessGroup": access_group,
            "SubArchive": "100001",
            "ArchiveCodes": [
                {
                    "ArchiveCode": "221",
                    "ArchiveType": "Felles arkivnøkkel for statsforvaltning",
                    "Sort": 1
                },
                {
                    "ArchiveCode": pnr,
                    "ArchiveType": "Fødselsnr/student",
                    "Sort": 2,
                    "IsManualText": True
                }
            ],
        }}

        self.log.info("Running HTTP POST to " + url + ", with this body content: " + str(post_data))
        try:
            response = requests.post(url, json=post_data, timeout=self.http_timeout)
        except Exception as e:
            raise Exception("Failed to POST data to " + url + ". Error message: " + str(e))

        response_object = self.validate_response(response, url)

        recno = response_object["Recno"]
        case_number = response_object["CaseNumber"]
        self.log.debug("Got this data from P360 CreateCase: " + str(response_object))

        return P360Case(case_number=case_number, case_recno=recno)

    def get_document_folder(self, folder_name, document_number):
        self.log.info("Looking for document folder " + folder_name + " in P360...")

        url = self.api_base_uri + "/DocumentService/GetDocuments?authkey=" + self.api_key

        post_data = {"parameter": {
            "CaseNumber": document_number,
            "Title": folder_name
        }}

        self.log.info("Running HTTP POST to " + url + " with this body: " + str(post_data))
        try:
            response = requests.post(url, json=post_data, timeout=self.http_timeout)
        except Exception as e:
            raise Exception("Failed to POST data to " + url + ". Error message: " + str(e))

        response_object = self.validate_response(response, url)

        documents = response_object["Documents"]
        if len(documents) > 1:
            raise Exception("The P360 returned more than one documents. Expected 0 or 1.")

        elif len(documents) == 1:
            existing_document = documents[0]
            recno = existing_document["Recno"]
            document_number = existing_document["DocumentNumber"]
            return document_number

        else:
            return None

    # POP Har lagt til access_code og paragraph
    def create_document_folder(self, folder_name, category, status, case_number, responsible_recno, access_code,
                               paragraph, access_group):
        self.log.info("Creating new P360 documents folder \"" + folder_name + "\"")

        url = self.api_base_uri + "/DocumentService/CreateDocument?authkey=" + self.api_key

        if category == 113:         # POP For lønsmelding er det viktig å få lagt inn Lønn som mottaker
            post_data = {"parameter": {
                "Title": folder_name,
                "Category": category,
                "Status": status,
                "CaseNumber": case_number,
                "ResponsiblePersonRecno": responsible_recno,
                "Contacts": [
                    {
                        "Recno": 315998,  # POP ØK - Økonomi - Lønn (I Unitest 201736), se 7.6.1 Data Contract: DocumentContactParameter
                        "Role": 6
                    }
                ],
                "Access code": access_code,  # POP Ikke Kamelskfift???
                "Paragraph": paragraph,  # POP
                "AccessGroup": access_group
            }}
        else:                   # POP Vi må ta høyde for at arbeidstakeren ikke har kontakt i P360 -> legger ikke inn mottaker i versjon 2.0
            post_data = {"parameter": {
                "Title": folder_name,
                "Category": category,
                "Status": status,
                "CaseNumber": case_number,
                "ResponsiblePersonRecno": responsible_recno,
                "Access code": access_code,  # POP Ikke Kamelskfift???
                "Paragraph": paragraph,  # POP
                "AccessGroup": access_group
            }}

        self.log.info("Running HTTP POST to " + url + ", with this body content: " + str(post_data))
        try:
            response = requests.post(url, json=post_data, timeout=self.http_timeout)
        except Exception as e:
            raise Exception("Failed to POST data to " + url + ". Error message: " + str(e))

        response_object = self.validate_response(response, url)

        recno = response_object["Recno"]
        document_number = response_object["DocumentNumber"]
        self.log.debug("Got this data from P360 CreateDocument: " + str(response_object))

        return recno, document_number

    def upload_file(self, document_number, file_object):
        self.log.info("Uploading document to P360...")

        url = self.api_base_uri + "/DocumentService/UpdateDocument?authkey=" + self.api_key

        post_data = {"parameter": {
            "DocumentNumber": document_number,
            "Files": [
                file_object
            ]
        }}

        self.log.info("Running HTTP POST to " + url + " using this document number: " + document_number)
        try:
            response = requests.post(url, json=post_data, timeout=self.http_timeout)
        except Exception as e:
            raise Exception("Failed to POST data to " + url + ". Error message: " + str(e))

        response_object = self.validate_response(response, url)

        recno = response_object["Recno"]
        document_number = response_object["DocumentNumber"]
        self.log.debug("Got this data from P360 UpdateDocument: " + str(response_object))

        return recno, document_number

