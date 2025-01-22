#Jun 20th, 2025
#For reasons still unknown, the update_sampled_contracts.py behaves erractically as the CWD becomes the sysproduction folder instead of the root pysystemtrade folder
#Individual components worked fine and tested in this script

from syscore.fileutils import resolve_path_and_filename_for_package
from sysproduction.data.contracts import dataContracts
from sysproduction.data.prices import diagPrices, get_valid_instrument_code_from_user
from sysdata.data_blob import dataBlob
from sysproduction.update_sampled_contracts import updateSampledContracts, update_sampled_contracts


#This passed 
ROLLS_DATAPATH = "data.futures.csvconfig"
ROLLS_CONFIG_FILE = "rollconfig.csv"
test = resolve_path_and_filename_for_package(ROLLS_DATAPATH,ROLLS_CONFIG_FILE)
print(test)

#This passed 
instrument_code = 'SP500'
data = dataBlob(log_name="Update-Sampled_Contracts")
diag_contracts = dataContracts(data)
roll_parameters = diag_contracts.get_roll_parameters(instrument_code)


#This passed 
update_contracts_object = updateSampledContracts(data)
update_contracts_object.update_sampled_contracts(
            instrument_code=instrument_code
        )

#This passed 
ALL_INSTRUMENTS = "ALL"
instrument_code = get_valid_instrument_code_from_user(
            allow_all=True, all_code=ALL_INSTRUMENTS
        )

update_sampled_contracts()
