import json
import os
import pathlib
import requests
import logging
import pandas as pd

# Set up basic configuration for the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Define a common formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create and configure file handler
file_handler = logging.FileHandler('upload.log')
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


def load_credentials():
    global OEP_USER, OEP_TOKEN, DATABUS_USER, DATABUS_API_KEY, DATABUS_GROUP
    if OEP_USER is None:
        OEP_USER = input("OEP Username: ")
    if OEP_TOKEN is None:
        OEP_TOKEN = input("OEP Token: ")
    if DATABUS_USER is None:
        DATABUS_USER = input("Database Username: ")
    if DATABUS_API_KEY is None:
        DATABUS_API_KEY = input("Database API Key: ")
    if DATABUS_GROUP is None:
        DATABUS_GROUP = input("Database Group: ")


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
        logger.info(f"Table {table_name} created successfully.")
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


def version_exists(table_name: str, version: str) -> bool:
    data = {
        "query": {
            "fields": ["version"],
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


def upload_files_form_folder(folder: pathlib.Path):
    for data_filename in folder.iterdir():
        if not data_filename.suffix == ".csv":
            continue

        with open(data_filename, "r") as f:
            data = pd.read_csv(f, delimiter=";")

        table_name = data_filename.name.split(".")[0]
        version = data.iloc[0]["version"]

        if version_exists(table_name, version):
            logger.info(
                f"Version {version} already exists in table {table_name}. Skipping."
            )
            continue

        upload_data(table_name, data_filename)
        register_data_on_databus(table_name, version)


def register_data_on_databus(table_name: str, version: str):
    response = requests.post(
        f"{OEDATAMODEL_API_URL}/databus/",
        data={
            "account": DATABUS_USER,
            "api_key": DATABUS_API_KEY,
            "group": DATABUS_GROUP,
            "schema": "model_draft",
            "table": table_name,
            "version": version,
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


def check_nomenclature_table(check_table: str, upload_folder: str):
    logger.info(
        f"Check if column naming matches nomenclature for table(s): {check_table}"
    )
    nomenclature_static = load_static_nomenclature()

    # find all csv files in upload_folder
    files = os.listdir(upload_folder)
    csvs = [csv.rstrip(".csv") for csv in files if csv.endswith(".csv")]

    # if specific table name is given only check this table
    if check_table in csvs and check_table != "all":
        csvs = [check_table]

    # if specific table name is not in csvs and not all - notify
    if check_table not in csvs and check_table != "all":
        logger.info(
            f"Table: {check_table} is not a csv file in your upload folder path: {upload_folder}\n"
        )

    logger.info(
        f"The following column headers are not conform with the nomenclature."
        f"Please, check the dynamic parameter conventions or if your columns are simply wrong:"
    )

    for table in csvs:
        tables_headers = set(
            pd.read_csv(
                filepath_or_buffer=os.path.join(upload_folder, table + ".csv"),
                sep=";",
                nrows=0,
            ).columns
        )
        difference_set = tables_headers - nomenclature_static
        logger.info(f"Table: {table} -> Header: {difference_set}")



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


def get_input(prompt, default):
    """Prompt for input and use a default value if none is provided."""
    user_input = input(f"{prompt} (default: '{default}'): ")
    return user_input.strip() or default


if __name__ == "__main__":
    # user inputs
    check_table = get_input("Enter specific table name to check or hit enter for 'all'", "all")
    nomenclature_path = get_input("Path to 'SEDOS_Modellstruktur.xlsx'", "data/")
    upload_folder = get_input("Input folder", "data/")

    # nomenclature check
    upload_folder = pathlib.Path(upload_folder)
    nomenclature_path = pathlib.Path(nomenclature_path)
    check_nomenclature_table(check_table, upload_folder)

    # upload
    load_credentials()
    create_tables_from_folder(upload_folder)
    upload_files_form_folder(upload_folder)
