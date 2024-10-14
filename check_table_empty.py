import requests
import os

OEP_API = "https://openenergyplatform.org/api/v0"

def open_tables(table_name: str, version_column: str = "version"):
    data = {
            "query": {
                "fields": [version_column],
                "from": {"type": "table", "table": table_name, "schema": "model_draft"},
                    }
            }
    response = requests.post(f"{OEP_API}/advanced/search", json=data)
    data = response.json()
    return data

def check_tables(data, filename: str):
    # If table is empty, no data is delivered, thus check if rowcount is zero
    if data["content"]["rowcount"] == 0:
        print(f"Table {filename} is empty.")
    
    
if __name__ == "__main__":
    directory = 'O:/ESY/06_Projekte-ST/BMWi SEDOS/05_Inhalte/AP8/techno-economic parameter/OEP'
    for filename in os.listdir(directory):
        filename = filename.split(".")[0]
        print ("Checking table: " + filename + '.')
        data = open_tables(table_name=filename)
        check_tables(data, filename)