
# SEDOS Upload Script

This script allows for creating table on OEP from metadata (JSON), uploading data from CSV and registration on databus.
All you need to do is point script to a folder which contains metadata and/or data and your credentials for OEP and databus.
Possible errors are logged in `upload.log`.


## Installation

Steps to set up script: 
- create python environment
- activate environment
- install packages: `pip install -r requirements.txt`

## Usage

There are two scripts in the repository:

1. `main.py`
With this script you can create tables from metadata and fill tables with correspondiong data from CSVs.
To do so, you have to run `python main.py` and enter all required credential (see how to set credentials permanently in next section).
Afterwards, you have to point to a directory holding your metadata and/or data files.
Metadata must be given as JSON, data file has to be named after OPE table and given as CSV.
From there, the script should do the rest...
2. `results.py`
This script is for uploading result data to OEP and register scenario data on databus 
(similar to `main.py`, but with fixed table to upload data to).
Run `python results.py` to update results data to table (`sedos_results.csv` must be placed in upload folder)

## Credentials

Instead of entering credentials at every run, you can add a `.env` file to the folder containing credentials in the following way:
```text
DATABUS_API_KEY=api_key
DATABUS_GROUP=example_group
DATABUS_USER=example_user
OEP_TOKEN=token
OEP_USER=username containing spaces
```
As this file is added to `.gitignore` your secrets are not getting commited!

## Issues

Please report any issue in this repository.