import json
import os
import pathlib
import requests
import logging

logging.basicConfig(filename='upload.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OEP_USER = os.environ.get('OEP_USER')
OEP_TOKEN = os.environ.get('OEP_TOKEN')

OEP_API = "https://openenergyplatform.org/api/v0/schema/model_draft/tables"
OEDATAMODEL_API_URL = "https://modex.rl-institut.de"


def load_credentials():
    global OEP_USER, OEP_TOKEN
    if OEP_USER is None:
        OEP_USER = input("OEP Username: ")
    if OEP_TOKEN is None:
        OEP_TOKEN = input("OEP Token: ")


def table_exists(table_name: str) -> bool:
    response = requests.get(f"{OEP_API}/{table_name}")
    if response.status_code == 200:
        return True
    return False


def create_table(table_name: str, metadata_filename: pathlib.Path):
    files = {"metadata_file": open(metadata_filename, "rb")}
    response = requests.post(f"{OEDATAMODEL_API_URL}/create_table/", files=files, data={"user": OEP_USER, "token": OEP_TOKEN})
    if response.status_code == 200:
        logging.info(f"Table {table_name} created successfully.")
    else:
        logging.error(f"Table {table_name} could not be created. Reason: {response.text}")


def create_tables_from_folder(folder: pathlib.Path | str):
    if isinstance(folder, str):
        folder = pathlib.Path(folder)
    for metadata_filename in folder.iterdir():
        if not metadata_filename.suffix == ".json":
            continue

        with open(metadata_filename, "r") as f:
            metadata = json.load(f)

        table_name = metadata["resources"][0]["name"].split(".")[1]
        if table_exists(table_name):
            logging.info(f"Table {table_name} already exists. Skipping.")
            continue

        create_table(table_name, metadata_filename)


if __name__ == "__main__":
    load_credentials()
    create_tables_from_folder("data")
