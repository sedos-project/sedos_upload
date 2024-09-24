import json
import logging
import os
import pathlib
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# Set up basic configuration for the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Define a common formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Create and configure file handler
file_handler = logging.FileHandler("upload.log")
file_handler.setFormatter(formatter)

# Create and configure console handler

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


OEP_USER = os.environ.get("OEP_USER")
OEP_TOKEN = os.environ.get("OEP_TOKEN")
DATABUS_USER = os.environ.get("DATABUS_USER")
DATABUS_API_KEY = os.environ.get("DATABUS_API_KEY")
DATABUS_GROUP = os.environ.get("DATABUS_GROUP")

OEP_API = "https://openenergyplatform.org/api/v0"
OEDATAMODEL_API_URL = "https://modex.rl-institut.de"


def load_oep_credentials():
    global OEP_USER, OEP_TOKEN
    if OEP_USER is None:
        OEP_USER = input("OEP Username: ")
    if OEP_TOKEN is None:
        OEP_TOKEN = input("OEP Token: ")


def load_databus_credentials():
    global DATABUS_USER, DATABUS_API_KEY, DATABUS_GROUP
    if DATABUS_USER is None:
        DATABUS_USER = input("Databus Username: ")
    if DATABUS_API_KEY is None:
        DATABUS_API_KEY = input("Databus API Key: ")
    if DATABUS_GROUP is None:
        DATABUS_GROUP = input("Databus Group: ")


def table_exists(table_name: str) -> bool:
    response = requests.get(f"{OEP_API}/schema/model_draft/tables/{table_name}")
    if response.status_code == 200:
        return True
    return False


def create_table(table_name: str, metadata_filename: pathlib.Path):
    files = {"metadata_file": open(metadata_filename, "rb")}
    response = requests.post(
        f"{OEDATAMODEL_API_URL}/create_table/",
        files=files,
        data={"user": OEP_USER, "token": OEP_TOKEN},
    )
    if response.status_code == 200:
        logger.info(f"Table {table_name} successfully created.")
    else:
        logger.error(
            f"Table {table_name} could not be created. Reason: {response.text}"
        )


def create_tables_from_folder(folder: pathlib.Path):
    for metadata_filename in folder.iterdir():
        if not metadata_filename.suffix == ".json":
            continue

        with open(metadata_filename, "r") as f:
            metadata = json.load(f)

        table_name = metadata["resources"][0]["name"].split(".")[1]
        if table_exists(table_name):
            logger.info(f"Table {table_name} already exists. Skipping.")
            continue

        create_table(table_name, metadata_filename)


def version_exists(table_name: str, version: str, version_column: str) -> bool:
    data = {
        "query": {
            "fields": [version_column],
            "from": {"type": "table", "table": table_name, "schema": "model_draft"},
        }
    }
    response = requests.post(f"{OEP_API}/advanced/search", json=data)
    data = response.json()

    # If table is empty, no data is delivered, thus check if rowcount is zero
    if data["content"]["rowcount"] == 0:
        return False

    rows = data["data"]
    versions = {row[0] for row in rows}
    return version in versions


def upload_data(table_name: str, data_file: pathlib.Path):
    files = {"csv_file": open(data_file, "rb")}
    response = requests.post(
        f"{OEDATAMODEL_API_URL}/upload/",
        files=files,
        data={
            "token": OEP_TOKEN,
            "schema": "model_draft",
            "table": table_name,
            "adapt_pks": "on",
        },
    )
    if response.status_code == 200:
        logger.info(f"Dataset uploaded successfully for table {table_name}.")
    else:
        logger.error(
            f"Dataset upload failed for table {table_name}. Reason: {response.text}"
        )


def upload_files_from_folder(folder: pathlib.Path, version_column: str = "version", artifact_names=None):
    artifact_names = artifact_names if artifact_names else {}
    for data_filename in folder.iterdir():
        if not data_filename.suffix == ".csv":
            continue

        with open(data_filename, "r") as f:
            data = pd.read_csv(f, delimiter=";")

        table_name = data_filename.name.split(".")[0]
        version = data.iloc[0][version_column]

        if version_exists(table_name, version, version_column):
            logger.info(
                f"Version {version_column}={version} already exists in table {table_name}. Skipping."
            )
            continue

        upload_data(table_name, data_filename)
        register_data_on_databus(table_name, version, artifact_names.get(table_name), version_column)


def register_data_on_databus(table_name: str, version: str, artifact_name: Optional[str] = None, version_column: str = "version"):
    response = requests.post(
        f"{OEDATAMODEL_API_URL}/databus/",
        data={
            "account": DATABUS_USER,
            "api_key": DATABUS_API_KEY,
            "group": DATABUS_GROUP,
            "schema": "model_draft",
            "table": table_name,
            "version": version,
            "artifact_name": artifact_name,
            "version_column": version_column,
        },
    )
    if response.status_code == 200:
        logger.info(
            f"Version {version} for table {table_name} successfully registered on databus."
        )
    else:
        logger.error(
            f"Registration of version {version} for table {table_name} on databus failed. Reason: {response.text}"
        )


def check_nomenclature_table(check_table: str, upload_folder: pathlib.Path):
    logger.info(
        f"Check if column naming matches nomenclature for table(s): {check_table}"
    )
    nomenclature_static = load_static_nomenclature()

    # find all csv files in upload_folder
    csvs = return_csv_table_names(upload_folder)

    # if specific table name is given only check this table
    if check_table != "all":
        if check_table not in csvs:
            logger.error(
                f"Table: {check_table} is not a csv file in your upload folder path: {upload_folder}\n"
            )
            return
        csvs = [check_table]

    logging_explained = False
    for table in csvs:
        tables_headers = set(
            pd.read_csv(
                filepath_or_buffer=os.path.join(upload_folder, table + ".csv"),
                sep=";",
                nrows=0,
            ).columns
        )
        difference_set = tables_headers - nomenclature_static
        if len(difference_set) > 0:
            if not logging_explained:
                logger.error(
                    f"The following column headers are not conform with the nomenclature."
                    f"Please, check the dynamic parameter conventions or if your columns are simply wrong:"
                )
                logging_explained = True
            logger.error(f"Table: {table} -> Headers: {difference_set}")


def load_static_nomenclature():
    nomenclature = pd.read_excel(
        os.path.join(nomenclature_path, "SEDOS_Modellstruktur.xlsx"),
        sheet_name="Parameter_Set",
        usecols=["SEDOS_name_long", "static_parameter"],
    )
    nomenclature_static = set(
        nomenclature.loc[nomenclature["static_parameter"] == 1, "SEDOS_name_long"]
    )

    return nomenclature_static


def delete_tables(delete_table_folder: pathlib.Path):
    tables_to_delete = return_csv_table_names(delete_table_folder)
    TOKEN_DICT = {"Authorization": "Token %s" % OEP_TOKEN}

    for table in tables_to_delete:
        response = requests.delete(
            f"{OEP_API}/schema/model_draft/tables/{table}/",
            headers=TOKEN_DICT
        )

        # raise Exception if request fails
        if response.ok:
            logger.info(
                f"Table {table} successfully deleted."
            )
        else:
            logger.error(f"Could not delete table '{table}'. Reason: {response.text}.")


def return_csv_table_names(path: pathlib.Path) -> list:
    files = os.listdir(path)
    return [csv.removesuffix(".csv") for csv in files if csv.endswith(".csv")]


def get_input(prompt, default):
    """Prompt for input and use a default value if none is provided."""
    user_input = input(f"{prompt} (default: '{default}'): ")
    return user_input.strip() or default


if __name__ == "__main__":
    # delete tables
    delete = get_input("Delete table(s) on OEP", "no")

    if delete != "no":
        delete_table_folder = get_input("Path to folder for csv tables to be deleted on OEP", "delete/")
        delete_table_folder = pathlib.Path(delete_table_folder)
        load_oep_credentials()
        delete_tables(delete_table_folder)

    # user input
    upload_folder = get_input("Table folder", "data/")
    check_table = get_input(
        "Enter specific table name to check or hit enter for 'all'", "all"
    )
    nomenclature_path = get_input("Path to 'SEDOS_Modellstruktur.xlsx'", "data/")

    # nomenclature check
    upload_folder = pathlib.Path(upload_folder)
    nomenclature_path = pathlib.Path(nomenclature_path)
    check_nomenclature_table(check_table, upload_folder)

    # upload
    if delete == "no":
        load_oep_credentials()
    load_databus_credentials()
    create_tables_from_folder(upload_folder)
    upload_files_from_folder(upload_folder)
