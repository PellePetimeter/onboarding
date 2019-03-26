# Stdlibs
import logging
import os
import datetime

# Custom code
from mq_client import MqClient
from config.server import ServerConfig as Config
from p360_client import P360Client
from docxgenerator import DocxGenerator
import utils


class Server:

    ldap_client = None
    mq_client = None
    p360_client = None
    document_creator = None
    config = None

    def __init__(self, mq_client=None, log=None):
        logging.info("Initializing the server...")
        
        if log:
            self.log = log
            self.log.info("The server is using the provided log handler")
        else:
            log_name = os.environ.get("LOG_NAME", "DEFAULT_LOG")
            log_level = os.environ.get("LOG_LEVEL", "INFO")
            self.log = logging.getLogger(log_name)
            self.log.setLevel(log_level)
            self.log.info("The server created a new log \"" + self.log.name + "\" with log level " +
                          logging.getLevelName(self.log.level))

        self.config = Config(log=self.log)

        self.p360_client = P360Client(log=self.log,
                                      api_base_uri=self.config.get_p360_api_base_uri(),
                                      api_key=self.config.get_p360_api_key())

        self.document_creator = DocxGenerator(log=self.log)

        if mq_client:
            self.mq_client = mq_client
        else:
            mq_notification_vhost = self.config.get_notification_vhost()
            mq_notification_username = self.config.get_mq_username()
            mq_notification_password = self.config.get_mq_password()
            mq_notification_exchange = self.config.get_notification_exchange_name()
            self.mq_client = MqClient(log=self.log, mq_host=self.config.get_mq_host(),
                                      mq_port=self.config.get_mq_port(),
                                      listen_exchange_name=self.config.get_mq_listen_exchange_name(),
                                      listen_queue_name=self.config.get_mq_listen_queue_name(),
                                      onboarding_callback=self.handle_new_onboarding,
                                      lonnsmelding_callback=self.handle_new_lonnsmelding,
                                      username=self.config.get_mq_username(),
                                      password=self.config.get_mq_password(),
                                      vhost=self.config.get_mq_vhost(),
                                      logs_exchange_name=self.config.get_logs_exchange_name(),
                                      retry_count=self.config.get_mq_retry_count(),
                                      retry_sleep_time=self.config.get_mq_retry_sleep_time(),
                                      notification_vhost=mq_notification_vhost,
                                      notification_username=mq_notification_username,
                                      notification_password=mq_notification_password,
                                      notification_exchange=mq_notification_exchange)

    def run(self):
        self.log.info("Preparing to consuming messages from queue.")
        
        queue_name = self.config.get_mq_listen_queue_name()

        mq_vhost = self.config.get_mq_vhost()
        mq_username = self.config.get_mq_username()
        mq_password = self.config.get_mq_password()
        mq_exchange = self.config.get_mq_listen_exchange_name()

        mq_channel = self.mq_client.establish_mq_channel(mq_username, mq_password, mq_vhost)

        self.mq_client.bind_to_queue(mq_channel, mq_exchange, queue_name)
        self.mq_client.start_consuming(mq_channel, mq_exchange, queue_name)

    def get_new_onboarding_callback_function(self):
        return self.handle_new_onboarding

    def get_new_lonnsmelding_callback_function(self):          #POP????
        return self.handle_new_lonnsmelding

    def handle_new_lonnsmelding(self, mq_message):

        responsible_person_email = None

        person_pnr = mq_message["FødselsOgPersonnummer"]
        person_name = mq_message["Navn"]
        access_group = mq_message["Enhet"] + " Personalmapper"


        incoming_document_path = "/resources/Lonnsmelding.docx"

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        document_result_path = "/result/generated_lonnsmelding_" + person_pnr + "-" + today +".docx"
        document_title = "Melding til Lønn"

        try:
            self.generate_docx_file(mq_message, incoming_document_path, document_result_path)

            try:
                p360_case = self.p360_client.get_case_by_pnr_and_access_group(pnr=person_pnr, access_group_filter=access_group)
            except Exception as e:
                raise RuntimeError("Something went wrong when querying P360 for existing cases. Error message: " + str(e))

            if p360_case is not None:
                self.log.info("Found an existing case with recno " + str(p360_case.get_recno()))
            else:
                raise RuntimeError("Couldn't find any existing cases on the new employee with prn " + str(person_pnr))

            responsible_person_email = p360_case.get_responsible_person_email()
            responsible_person_recno = p360_case.get_responsible_person_recno()
            access_group = p360_case.get_access_group()
            case_number = p360_case.get_case_number()

            documents_folder_number = self.get_p360_document_folder(document_title, p360_case.get_case_number())
            if documents_folder_number is None:
                self.log.info("No existing documents folder found. Will now create a new one called \"" + document_title + "\"")

                case_document_category = 113  # Internal memo with follow-up
                case_document_status = 1  # "Reserved"
                access_code = 18  # POP UO/Untatt offentlighet
                paragraph = "Offl § 26 femte ledd"  # POP "only code value is permitted"

                documents_folder_number = self.create_p360_documents_folder(access_code,    # POP
                                                                            paragraph,      # POP
                                                                            access_group,
                                                                            case_document_category,
                                                                            case_document_status,
                                                                            document_title,
                                                                            case_number,
                                                                            responsible_person_recno)
            else:
                self.log.info("Existing document folder with number " + str(documents_folder_number) +
                              " found. No need to create a new one")

            assert documents_folder_number is not None

            document_file_object = self.generate_documents_file_object(document_result_path, "Melding til Lønn for " + person_name)
            self.upload_file_to_p360(document_file_object, documents_folder_number)

            assert responsible_person_email is not None

            self.emit_mq_notification(case_number=p360_case.get_case_number(), case_recno=p360_case.get_recno(),
                                      person_name=person_name,
                                      responsible_user_email=responsible_person_email,
                                      event_name="p360lonnsmeldingCreated")

        except Exception as e:

            outgoing_mq_message = {
                "event": "error",
                "data": {
                    "message": "Noe gikk galt ved opprettelse av melding til Lønn for " + person_name + "."
                }
            }

            # If we've know who the responsible person is, send a notification to that person. Else, we'll leave out the
            # "email_recipient" part of the message, making the notification service use the default
            # error event mail recipients
            if responsible_person_email is not None:
                outgoing_mq_message["data"]["email_recipient"] = responsible_person_email

            self.log.error("Something went wrong. Error message: " + str(e))
            self.mq_client.emit_notification_message(outgoing_mq_message)
            return False

        return True

    def handle_new_onboarding(self, mq_message):

        self.log.info("Processing this incoming message: " + str(mq_message))
        person_name = mq_message["Navn"]
        person_pnr = mq_message["FødselsOgPersonnummer"]
        responsible_user_email = mq_message["DinEpostadresse"]
        unit = mq_message["Enhet"]
        access_group = unit + " Personalmapper"
        new_case_name = "Personalmappe offentlig - " + person_name + " - " + unit
        case_arbeidsavtale_document_title = "Arbeidsavtale"
        case_hta_document_title = "Hovedtariffavtale"
        case_welcome_letter_document_title = "Velkomstbrev"

        case_document_category = 111  # "Outbound document"
        case_document_status = 1  # "Reserved"
        access_code = 18  # POP UO/Untatt offentlighet
        paragraph = "Offl § 26 femte ledd"  # POP "only code value is permitted"

        today = datetime.datetime.now().strftime("%Y-%m-%d")

        terms_of_employment_document_result_path = "/result/generated_arbeidsavtale_" + \
                                                   person_pnr + "_" + today + ".docx"

        if mq_message["ArbeidsavtaleLanguage"] == "Engelsk":
            terms_of_employment_incoming_document_path = "/resources/Arbeidsavtale_engelsk.docx"
        else:
            terms_of_employment_incoming_document_path = "/resources/Arbeidsavtale_norsk.docx"

        collective_bargaining_incoming_document_path = "/resources/Hovedtariffavtale.docx"
        welcome_letter_incoming_document_path = "/resources/Velkomstbrev.docx"

        collective_bargaining_document_result_path = "/result/generated_hovedtariffavtale_" + person_pnr + "_" + today + ".docx"
        welcome_letter_document_result_path = "/result/generated_welcome_letter_" + person_pnr + "_" + today + ".docx"

        try:
            self.generate_docx_file(mq_message, terms_of_employment_incoming_document_path, terms_of_employment_document_result_path)
            self.log.info(
                "Successfully created the document " + terms_of_employment_document_result_path + ".")

            responsible_contact = self.get_p360_contact_person_by_email(responsible_user_email)
            responsible_recno = responsible_contact.get_recno()

            p360_case = self.get_p360_case_by_title(new_case_name)
            if p360_case is None:
                self.log.info("No existing case found. Will now create a new case with title \"" + new_case_name + "\"")
                p360_case = self.create_p360_case(access_group, new_case_name, person_pnr, responsible_recno)
            else:
                self.log.info("Existing case with case number " + str(p360_case.get_case_number()) + " found. No need to create a new one")

            case_number = p360_case.get_case_number()
            case_recno = p360_case.get_recno()

            # POP Velkomstbrev lagt først
            welcome_letter_documents_folder_number = self.get_p360_document_folder(case_welcome_letter_document_title,
                                                                                   case_number)

            if welcome_letter_documents_folder_number is None:
                self.log.info(
                    "No existing documents folder found. Will now create a new one called \"" + case_welcome_letter_document_title + "\"")

                welcome_letter_documents_folder_number = self.create_p360_documents_folder(access_group,
                                                                                           case_document_category,
                                                                                           case_document_status,
                                                                                           case_welcome_letter_document_title,
                                                                                           case_number,
                                                                                           responsible_recno)
            else:
                self.log.info("Existing document folder with number " + str(
                    welcome_letter_documents_folder_number) + " found. No need to create a new one")

            arbeidsavtale_documents_folder_number = self.get_p360_document_folder(case_arbeidsavtale_document_title, case_number)

            if arbeidsavtale_documents_folder_number is None:
                self.log.info("No existing documents folder found. Will now create a new one called \"" + case_arbeidsavtale_document_title + "\"")

                arbeidsavtale_documents_folder_number = self.create_p360_documents_folder(access_code, paragraph, access_group, case_document_category,
                                                                            case_document_status, case_arbeidsavtale_document_title,
                                                                            case_number, responsible_recno)
            else:
                self.log.info("Existing document folder with number " + str(arbeidsavtale_documents_folder_number) + " found. No need to create a new one")

            hta_documents_folder_number = self.get_p360_document_folder(case_hta_document_title, case_number)

            if hta_documents_folder_number is None:
                self.log.info(
                    "No existing documents folder found. Will now create a new one called \"" + case_hta_document_title + "\"")

                hta_documents_folder_number = self.create_p360_documents_folder(access_group,
                                                                                case_document_category,
                                                                                case_document_status,
                                                                                case_hta_document_title,
                                                                                case_number,
                                                                                responsible_recno)
            else:
                self.log.info("Existing document folder with number " + str(
                    hta_documents_folder_number) + " found. No need to create a new one")



            # We'll generate more files, but first let's add some data
            enriched_mq_message = mq_message.copy()
            enriched_mq_message["p360_case_number"] = case_number
            enriched_mq_message["date"] = utils.get_current_date_as_string()

            try:
                self.generate_docx_file(enriched_mq_message, collective_bargaining_incoming_document_path,
                                        collective_bargaining_document_result_path)
            except Exception as e:
                self.log.error("Something went wrong when creating a .docx-file based on this MQ message: " + str(
                    mq_message) + ". Error message: " + str(e))
                raise

            try:
                self.generate_docx_file(enriched_mq_message, welcome_letter_incoming_document_path,
                                        welcome_letter_document_result_path)
            except Exception as e:
                self.log.error("Something went wrong when creating a .docx-file based on this MQ message: " + str(
                    mq_message) + ". Error message: " + str(e))
                raise

            # Convert the generated docx-files to JSON objects, in which the docx-files content
            # are represented in ascii format.
            terms_of_employment_document_file_object = self.generate_documents_file_object(terms_of_employment_document_result_path, "Arbeidsavtale for " + person_name)
            hta_document_file_object = self.generate_documents_file_object(collective_bargaining_document_result_path, "Hovedtariffavtale for " + person_name)
            welcome_letter_document_file_object = self.generate_documents_file_object(welcome_letter_document_result_path, "Velkomstbrev for " + person_name)

            assert welcome_letter_document_file_object is not None
            assert welcome_letter_document_result_path is not None

            self.upload_file_to_p360(terms_of_employment_document_file_object, arbeidsavtale_documents_folder_number)
            self.upload_file_to_p360(hta_document_file_object, hta_documents_folder_number)
            self.upload_file_to_p360(welcome_letter_document_file_object, welcome_letter_documents_folder_number)

            self.emit_mq_notification(case_number=case_number, case_recno=case_recno, person_name=person_name,
                                      responsible_user_email=responsible_user_email, event_name="p360caseCreated")

        except Exception as e:

            assert responsible_user_email is not None
            assert person_name is not None

            self.log.error("Something went wrong. Error message: \"" + str(e) + "\", exception type " + str(type(e)) +
                           " . Will emit an MQ message.")
            outgoing_mq_message = {
                "event": "error",
                "data": {
                    "email_recipient": responsible_user_email,
                    "message": "Noe gikk galt ved opprettelse av ny personalmappe for " + person_name + "."
                }
            }
            self.mq_client.emit_notification_message(outgoing_mq_message)
            return False

        return True

    def emit_mq_notification(self, case_number, case_recno, person_name, responsible_user_email, event_name):

        web_link = self.config.get_p360_web_base_uri() + "?recno=" + str(
            case_recno) + "&module=Case&subtype=2"

        data = {
            "event": event_name,
            "data": {
                "executive_officer": responsible_user_email,  # "Saksbehandler"
                "p360_case_subject": person_name,  # Our new employee
                "p360_recno": case_recno,
                "p360_case_number": case_number,
                "web_link": web_link
            }
        }

        self.mq_client.emit_notification_message(data)

    def create_p360_case(self, access_group, new_case_name, person_pnr, responsible_recno):
        try:
            p360_case = self.p360_client.create_case(case_title=new_case_name,
                                                     responsible_person_recno=responsible_recno,
                                                     access_group=access_group, pnr=person_pnr)

        except Exception as e:
            self.log.error("Something went wrong when creating the new P360 case. Error message: " + str(e))
            raise

        if p360_case is not None:
            self.log.info("Successfully created a case with recno " + str(p360_case.get_recno()) +
                          " and case number " + p360_case.get_case_number())
        return p360_case

    def get_p360_contact_person_by_email(self, responsible_user_email):
        try:
            responsible_recno = self.p360_client.get_contact_person_by_email(user_email=responsible_user_email)
        except Exception as e:
            raise Exception("Something went wrong when fetching responsible recno from username " +
                            responsible_user_email + ". Error message: " + str(e))

        if responsible_recno is None:
            raise RuntimeError("Got an empty response when looking up user " + responsible_user_email + " in P360")
        else:
            self.log.info(
                "Found a person with email " + responsible_user_email + " and recno " + str(responsible_recno))

        return responsible_recno

    def get_p360_case_by_title(self, new_case_name):
        try:
            p360_case = self.p360_client.get_case_by_title(case_title=new_case_name)
        except Exception as e:
            self.log.error("The P360 client returned this error message when looking up case by title: " + str(e) +
                           ". Exception type is " + str(type(e)))
            raise  # Re-raise the current exception

        if p360_case is not None:
            self.log.info("Found an existing case with recno " + str(p360_case.get_recno()))

        return p360_case

    def generate_documents_file_object(self, document_path, title):
        self.log.debug("Converting contents of " + document_path + " to ASCII text")
        file_contents_as_ascii_text = utils.convert_file_contents_to_ASCII_text(document_path)
        document_file_object = {"title": title,
                                "format": "docx",
                                "data": file_contents_as_ascii_text}
        return document_file_object

    def create_p360_documents_folder(self, access_group, case_document_category, case_document_status,   ## POP lagt til access_code og paragraph
                                     case_document_title, case_number, responsible_recno):
        try:
            document_folder_recno, documents_folder_number = self.p360_client.create_document_folder(
                folder_name=case_document_title,
                category=case_document_category,
                status=case_document_status,
                case_number=case_number,
                responsible_recno=responsible_recno,
                access_group=access_group)
            self.log.info(
                "Successfully create a new documents folder \"" + case_document_title + "\" with recno " + str(
                    document_folder_recno))
        except Exception as e:
            self.log.error("Something went wrong when creating the new P360 document folder. Error message: " + str(e))
            raise
        return documents_folder_number

    def upload_file_to_p360(self, document_file_object, documents_folder_number):
        document_title = document_file_object["title"]
        self.log.info("Will now upload the generated docx-file \"" + document_title + "\" to document number " + documents_folder_number)
        try:
            self.p360_client.upload_file(document_number=documents_folder_number, file_object=document_file_object)

        except Exception as e:
            self.log.error("Something went wrong when uploading the document " + str(documents_folder_number) +
                           " to Public 360. Error message: " + str(e))
            raise  # Re-raise current exception

    def generate_docx_file(self, mq_message, incoming_document_path, generated_document_path):
        try:
            self.document_creator.create_docx_file(mq_message,
                                                   incoming_document_path,
                                                   generated_document_path)
        except Exception as e:
            self.log.error("Something went wrong when creating a .docx-file based on this MQ message: " + str(
                mq_message) + ". Error message: " + str(e))
            raise

    def get_p360_document_folder(self, case_document_title, case_number):
        self.log.info("Looking to see if there's already a documents folder named " +
                      case_document_title + " registered in P360.")
        try:
            documents_folder_number = self.p360_client.get_document_folder(
                folder_name=case_document_title, document_number=case_number)
            self.log.info("Found a documents folder with number " + str(documents_folder_number))
        except Exception as e:
            self.log.error(
                "Something went wrong when querying P360 for existing document folders. Error message: " + str(e))
            raise

        return documents_folder_number



