from random import randint
import multiprocessing
from time import sleep

mtIBDataUpdater_instance = None
def set_global_dataupdater_instance():
    global mtIBDataUpdater_instance 
    if not mtIBDataUpdater_instance: 
        mtIBDataUpdater_instance = randint(100,110)
        print(mtIBDataUpdater_instance)


def update_ohlcv(i):
    mtIBDataUpdater_instance = i 
    print(mtIBDataUpdater_instance)
    sleep(5)

def update_all_ohlcv_contracts(contracts):
    with multiprocessing.get_context("spawn").Pool(initializer=set_global_dataupdater_instance, processes=4) as pool:
        pool.map(update_ohlcv, contracts)
    pool.join()
    #global mtIBDataUpdater_instance
    print(mtIBDataUpdater_instance)


if __name__ == "__main__": 
    multiprocessing.set_start_method("spawn")
    #contracts = get_all_ib_contracts()
    effective_contracts = range(20)
    update_all_ohlcv_contracts(effective_contracts)
    print(mtIBDataUpdater_instance)