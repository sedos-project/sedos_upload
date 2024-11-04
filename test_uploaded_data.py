from data_adapter import databus
from data_adapter.preprocessing import Adapter
from data_adapter.structure import Structure
import os


url = "https://databus.openenergyplatform.org/sedos-project/collections/sedos-project"
structure_name = "SEDOS_Modellstruktur_test_tra"
process_sheet = "Process_Set"
helper_sheet = "Helper_Set"
collection_name = url.split('/')[-1]

databus.download_collection(url)
structure = Structure(
    structure_name,
    process_sheet=process_sheet,
    helper_sheet=helper_sheet,
)

adapter = Adapter(
    collection_name,
    structure=structure,
)

processes = {}

for process_name in structure.processes.keys():
    try:
        print(f"Checking process: {process_name}")
        processes[process_name] = adapter.get_process(process_name)
        print(f"Process '{process_name}' downloaded successfully.")
    except Exception as e:
        print(f"Failed to download process '{process_name}': {e}")
