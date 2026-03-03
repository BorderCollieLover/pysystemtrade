#specifically for NIFTY-IN contracts 
from sysobjects.contracts import futuresContract
from mttestscripts.clean_pst_contract_prices import manually_map_pst_contract_to_ib, enhance_pst_data_with_ib_data_for_contract

NIFTY_IN_CONTRACT_2_IB_conId_mapping = {
    '20220700': 559234492,
    '20220800': 564672722,
    '20220900': 571021472,
    '20221000': 576501245,
    '20221100': 582044431,
    '20221200': 589040590,
    '20230100': 594418732,
    '20230200': 600195197,
    '20230300': 605924117,
    '20230400': 610689624,
    '20230500': 616415995,
    '20230600': 623233892,
    '20230700': 628565706,
    '20230800': 633693700,
}

for contract_dt, conId in NIFTY_IN_CONTRACT_2_IB_conId_mapping.items():
    contract = futuresContract('NIFTY-IN', contract_dt)
    print(f"Processing contract: {contract}, conId: {conId}")
    map_result = manually_map_pst_contract_to_ib(contract, conId)
    print(f"Mapping result: {map_result}")
    enhance_pst_data_with_ib_data_for_contract(contract)
    #break
