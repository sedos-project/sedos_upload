from data_adapter import databus
from data_adapter.preprocessing import Adapter
from data_adapter.structure import Structure
from main import get_input


# Create a collection with your sector data on the databus and copy its URL here, e.g. https://databus.openenergyplatform.org/sedos-project/collections/sedos-transport-collection
url = get_input("Insert url for your sector collection on the databus", "https://databus.openenergyplatform.org/sedos-project/collections/sedos-project") # insert your collection url

# Before starting the script define your excel with all processes (process and helper sheets) of your sector data to be tested
structure_name = get_input("Insert name of your structure", "SEDOS_Modellstruktur_all") # Insert your structure, e.g.: SEDOS_Modellstruktur_test_tra

# Default names of the sheets
process_sheet = "Process_Set"
helper_sheet = "Helper_Set"
collection_name = url.split('/')[-1]

# Download entire collection
databus.download_collection(url)
# Define structure class
structure = Structure(
    structure_name,
    process_sheet=process_sheet,
    helper_sheet=helper_sheet,
)

# Define adapter class
adapter = Adapter(
    collection_name,
    structure=structure,
)

## Download process from structure
downloaded_processes = {}
failed_processes = []
for process_name in structure.processes.keys():
    try:
        print(f"Checking process: {process_name}")
        downloaded_processes[process_name] = adapter.get_process(process_name)
        print(f"Process '{process_name}' downloaded successfully.")
    except Exception as e:
        failed_processes.append(process_name)
        print(f"Failed to download process '{process_name}': {e}")
print(f"Failed processes: {failed_processes}")

