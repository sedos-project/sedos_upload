
# SEDOS Upload Script

This script allows for creating table on OEP from metadata (JSON), uploading data from CSV and registration on databus.
All you need to do is point script to a folder which contains metadata and/or data and your credentials for OEP and databus.
Possible errors are logged in `upload.log`.


## Installation

Steps to set up script: 
- create python environment
- activate environment
- install packages: `pip install -r requirements.txt`
- run script: `python main.py`

## Usage

First you have to enter all required credential (see how to set credentials permanently in next section).
Afterwards, you have to point to a directory holding your metadata and/or data files.
Metadata must be given as JSON, data file has to be named after OPE table and given as CSV.
From there, the script should do the rest...

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