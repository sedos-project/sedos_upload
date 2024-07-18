"""This module offers functions for upload of SEDOS scenario results"""

import pathlib
from main import (
    get_input,
    load_oep_credentials,
    load_databus_credentials,
    upload_files_from_folder,
)

if __name__ == "__main__":
    # user input
    upload_folder = get_input("Table folder", "data/")
    upload_folder = pathlib.Path(upload_folder)

    # upload
    load_oep_credentials()
    load_databus_credentials()
    upload_files_from_folder(upload_folder, version_column="scenario")
