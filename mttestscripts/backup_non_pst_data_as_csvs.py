from mttestscripts.files_tool import back_up_parquet_into_csv, backup_one_folder
from sysdata.config.production_config import get_production_config, Config
import os
from mttestscripts.clean_pst_contract_prices import dedup_all_pst_contract_prices
from mttestscripts.ohlc_parquet_cleanup_tools import dedup_all_ib_parquets


#Back up all data parquets into csv files for more data protection
def backup_all_data_parquet_into_csv_files(backup_root):
    #back up parquet data
    config = Config()
    config = get_production_config()
    print(backup_root)
    #ignore spot fx, because it involves setting the index name, leave it to PST's own backup process 
    #also ignore other PST data which is back up in PST's own process 
    source_folders = ['ib', 'futures_contract_prices'] 
    for folder in source_folders: 
        
        source_files_path = os.path.join(config.get_element("parquet_store"), folder)
        target_files_path = os.path.join(backup_root, folder)
        #print(target_files_path)
        back_up_parquet_into_csv(source_folder=source_files_path, backup_folder=target_files_path)

    #back up barchart 
    parquet_home = config.get_element("parquet_store")
    data_home = os.path.abspath(os.path.join(parquet_home, '..'))
    barchart_data_home = os.path.join(data_home, 'barchart')
    backup_path = os.path.join(backup_root, 'barchart')
    if os.path.exists(barchart_data_home):
        backup_one_folder(barchart_data_home,backup_path)

    #back up MRCI
    mrci_home = os.path.join (os.path.abspath(os.path.join(data_home, '..')), 'MRCIData')
    backup_path = os.path.join(backup_root, 'MRCIData')
    if os.path.exists(mrci_home):
        backup_one_folder(mrci_home,backup_path, extension='.html')



if __name__ == "__main__":
    #ensure contract prices are deduped before backup -- hardly necessary but just in case
    #dedup_all_ib_parquets()
    #dedup_all_pst_contract_prices()

    config = Config()
    config = get_production_config()
    back_up_root= config.get_element("parquet_store") 

    back_up_root = os.path.join(back_up_root,  '../DATABackup')
    backup_all_data_parquet_into_csv_files(back_up_root)

    destination_path = '/mnt/sdb1/DATABackup/'
    os.system("rsync -av %s %s" % (back_up_root, destination_path))

    #back_up_root = '/mnt/sdb1/DATA'
    #backup_all_data_parquet_into_csv_files(back_up_root)
    #back_up_root = os.path.join(os.path.expanduser('~'), 'DATA')
    #backup_all_data_parquet_into_csv_files(back_up_root)
    
