from time import sleep
from sysproduction.data.generic_production_data import productionDataLayerGeneric
from sysbrokers.broker_factory import get_broker_class_list
from sysproduction.data.control_process import diagControlProcess
from syscore.constants import arg_not_supplied
from sysdata.data_blob import dataBlob
from sysbrokers.IB.ib_connection import connectionIB
from syscore.objects import get_class_name
from syscore.text import camel_case_split
from sysdata.config.production_config import get_production_config, Config
from sysdata.mongodb.mongo_connection import mongoDb
from syslogging.logger import *
from sysdata.mongodb.mongo_IB_client_id import mongoIbBrokerClientIdData
from sysdata.data_blob import identifying_name
from sysbrokers.IB.client.ib_price_client import  TIMEOUT_SECONDS_ON_HISTORICAL_DATA
from ib_insync import Contract, Future, IB, util
import time
from datetime import timezone
VERY_FEW_DATA_POINTS = 5

#A stripped down version of the PST dataBroker class to test the running sequence 
#data : dataBlob is the main data processing class 
#it binds different task specific data processing classes, e.g. ib_****, mongo_**** through the add_class_list function
#_add_required_classes_to_data is defined in productionDataLayerGeneric, and commonly inherited by various data processing classes
#  which subsequently modifies its behavior in inheritance
#Because the __init__ function of productionDataLayerGeneric calls _add_required_classes_to_data
#Derived datalayer objects binds the functionality properly during initialization

#The call sequence from dataBlob.add_class_list: add_class_list --> add_class_object -->_get_resolved_instance_of_class -->  _add_ib_class
#In _add_ib_class, it will create an instance of an IB connection (if not already connected)
#It will then create an instance of the particular class to add as a property to data via: 
# resolved_instance = class_object(self.ib_conn, self, log=log)
#where class_object is the class name resolved through the process, all these classes have the same three parameters for initialization

#When adding IB related instances and methods to a dataBlob object, the initializaton function has as an input an ib_conn, an IB connection
# when this property function is called for the first time, and when the connection is not established, it will establish an IB connection for the dataBlob object 

#In get_broker_class_list it first resolves the actual name of a get_class_list function, which is specified in the configuration yaml file
#Specifically, the result is get_ib_class_list function specified in the configuration yaml file,
# this function which returns a list of IB related class names, these classes are defined in various python files in sysbrokers.ib...
#The class names are listed below: 
"""[<class 'sysbrokers.IB.ib_Fx_prices_data.ibFxPricesData'>, 
        <class 'sysbrokers.IB.ib_futures_contract_price_data.ibFuturesContractPriceData'>, 
        <class 'sysbrokers.IB.ib_futures_contracts_data.ibFuturesContractData'>, 
        <class 'sysbrokers.IB.ib_contract_position_data.ibContractPositionData'>, 
        <class 'sysbrokers.IB.ib_orders.ibExecutionStackData'>, 
        <class 'sysbrokers.IB.ib_static_data.ibStaticData'>, 
        <class 'sysbrokers.IB.ib_capital_data.ibCapitalData'>, 
        <class 'sysbrokers.IB.ib_instruments_data.ibFuturesInstrumentData'>, 
        <class 'sysbrokers.IB.ib_fx_handling.ibFxHandlingData'>, 
        <class 'sysbrokers.IB.ib_broker_commissions.ibFuturesContractCommissionData'>]"""
#The corresponding elements in the data object as : 
"""db_ib_broker_client_id,
        broker_fx_prices,
        broker_futures_contract_price,  <-- this is the one called by dataBroker through .data, which returns a brokerFuturesContractPriceData, and call its get_prices_at_frequency_for_contract_object() function. 
        broker_futures_contract,
        broker_contract_position,
        broker_execution_stack,
        broker_static,
        broker_capital,
        broker_futures_instrument,
        broker_fx_handling,
        broker_futures_contract_commission"""
class mydataBroker(productionDataLayerGeneric):
    def __init__(self, data: dataBlob = arg_not_supplied):
        super().__init__(data)
        self._diag_controls = diagControlProcess()

    def _add_required_classes_to_data(self, data) -> dataBlob:
        # Add a list of broker specific classes that will be aliased as self.data.broker_fx_prices,
        # self.data.broker_futures_contract_price ... and so on
        broker_class_list = get_broker_class_list(data)
        print(broker_class_list)
        data.add_class_list(broker_class_list)
        #this is where the data (dataBlob) object is added with various data source interfaces 
        return data

###my own class
##This class retrieves all historical data of a contract from IB
##
class mtIBData(object):
    def __init__(
            self,
            ib_conn: connectionIB = arg_not_supplied,
            keep_original_prefix: bool = False
            ):
        self._ib_conn = ib_conn
        self._keep_original_prefix = keep_original_prefix
        self._attr_list = []
        self.ib_conn
        #self.price_client = ibPriceClient(self.ib_conn)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self._ib_conn is not arg_not_supplied:
            self.ib_conn.close_connection()
            self.db_ib_broker_client_id.release_clientid(self.ib_conn.client_id())
        # No need to explicitly close Mongo connections; handled by Python garbage collection

   
    def retrieve_historical_data_for_contract_with_frequency(self, contract, startDateTime='', endDateTime='', useRTH: bool=False,barSizeSetting: str='1 hour'):
        if time.tzname[time.daylight]=='UTC':
            ...
        else:
            self._raise_and_log_error('The local time zone is not UTC, please set the time zone to UTC')
        
        #sample code from ib_sync recipe
        """ dt = ''
        barsList = []
        while True:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=dt,
                durationStr='10 D',
                barSizeSetting='1 min',
                whatToShow='MIDPOINT',
                useRTH=True,
                formatDate=1)
            if not bars:
                break
        barsList.append(bars)
        dt = bars[0].date
        print(dt)

        # save to CSV file
        allBars = [b for bars in reversed(barsList) for b in bars]
        df = util.df(allBars)
        df.to_csv(contract.symbol + '.csv', index=False) """

        #actually I need endDateTime for expired contracts ........... duh.......
        
        #decide on duration str based on barSzieSetting 
        #this is the necessary as with lower time frame, a short time windows is imperative 
        
        #regarding startDateTime and endDateTime
        #endDateTime is necessary when downloading historical data for expired contracts for hourly and minute data, where historical data is patched up over small intervals (month or week of data per batch)
        #In this scenario, if endDateTime is not supplied, the code will try to download data from the present time. Upon retrieving no trading data, it might mistakenly deduce that the intrument has not historical data available. 
        #This wasn't a problem for daily data where I can use a relatively long duration to try to get whatever historical that's available.
        #This wasn't an issue for instruments without expiry, e.g. stocks, when it was always possible to start with present data. 
        #ib_insync's reqHistoricalData takes and endDateTime parameter, mimicking the IB API interface 
        #In IB API interface, the endDateTime should be a string of either '' (empty) or “YYYYMMDD HH:mm:ss TMZ”, https://ibkrcampus.com/ibkr-api-page/twsapi-doc/#historical-bars
        #In ib_insync, the endDateTime parameter can be (Union[datetime, date, str, None]) https://ib-insync.readthedocs.io/api.html#ib_insync.objects.BarDataList.reqId
        #In its code, the input is essentially converted to a string of either '' (empty) or “YYYYMMDD HH:mm:ss TMZ”  in https://github.com/erdewit/ib_insync/blob/master/ib_insync/util.py 
        #The code snippet is below: 
        """ def formatIBDatetime(t: Union[dt.date, dt.datetime, str, None]) -> str:
            #Format date or datetime to string that IB uses.
            if not t:
                s = ''
            elif isinstance(t, dt.datetime):
            # convert to UTC timezone
                t = t.astimezone(tz=dt.timezone.utc)
                s = t.strftime('%Y%m%d %H:%M:%S UTC')
            elif isinstance(t, dt.date):
                t = dt.datetime(
                t.year, t.month, t.day, 23, 59, 59).astimezone(tz=dt.timezone.utc)
                s = t.strftime('%Y%m%d %H:%M:%S UTC')
            else:
                s = t
            return s """
        #The next question to address is the format of the startDateTime and endDateTime when using them
        #The reqHistoricalData method returns, for datetime information, a datatime.datetime object
        #This datatime.datetime object is passed directly to the endDateTime when downloading all historical data in a repeative manner (although it's still converted to a properly formatted string later)
        #To faciliate the check against downloaded data when startDateTime is used, we convert the input to datetime.datetime 

        dt = endDateTime # the endDateTime parameter is necessary because we'll try to download data for recently expired contracts
        barslist = []
        whatToShow = 'Trades'
        useStartDateTime = (startDateTime!='')  #Whether a startDateTime is specified
        durationStr = self._get_duration_from_barSize(barSizeSetting) # determine duration according to the barSizeSetting. 
        formatDate = 1 if barSizeSetting=='1 day' else 2  #when download daily data use local time zone dates 

        if useStartDateTime: #process the startDateTime string if it is given, return a datetime.datetime or datetime.date object
            #technically data should start AFTER startdt as in updating, the input would be the date or datetime of the last available data 
            startdt = self._process_startDateTime(startDateTime, barSizeSetting)
        
        #startDateTime = datetime.datetime(2024, 1, 27, 12, 0, tzinfo=datetime.timezone.utc)
        self.ib.reqMarketDataType(3)
        while True:
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=dt,
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow=whatToShow,
                useRTH=useRTH,  #However, need to set useRTH to True for barSizeSetting = '1 day' otherwise the code doesn't work as intended 
                formatDate=formatDate, #2: returns UTC time, 1: returns local time zone. Use 1 only for daily data
                timeout=TIMEOUT_SECONDS_ON_HISTORICAL_DATA,
            )
            if not bars: 
                break
            bar_length = len(bars)
            #print(bars)
            #April 25, 2025
            ##The code below is to remove data points with future timestamps
            if formatDate==2:
                bars = [bar for bar in bars if bar.date<datetime.datetime.now(timezone.utc)]
            else:
                bars = [bar for bar in bars if bar.date<datetime.datetime.now().date()]
            new_bar_length = len(bars)
            if new_bar_length < bar_length:
                print(
                "Ignoring %d prices with future timestamps"
                % (bar_length - new_bar_length)
            )
            if not bars:
                break; 

            
            #Occasionally the code will end up in an infinite loop in the following scenario: 
            #Say the existing data is from August 1, 2024 to now, the code is attempting to retrieve a block of historical bars that ends before August 1
            #The code would end returning data (as an example) of August 1 to August 2, 2024. 
            #It will pass the non-empty-ness test above, 
            #The duplicated data will be added to the buffer (and eventually discarded)
            #Again the existing data is from August 1, 2024 and the code will again attempt to download data prior to August 1st. 
            #It then enters into an infinite loop. 
            #The if statement below tries to prevent the program from hanging in this situation. 
            #It's not the most elegant solution but historical data is a messy business by nature. 
            if dt!='':
                tmp_dt = bars[len(bars)-1].date
                if isinstance(tmp_dt, datetime.date):
                    tmp_dt = datetime.datetime(tmp_dt.year, tmp_dt.month, tmp_dt.day).astimezone(tz=datetime.timezone.utc)
                if isinstance(dt, datetime.date):
                    dt = datetime.datetime(dt.year, dt.month, dt.day).astimezone(tz=datetime.timezone.utc)
                if tmp_dt > dt:
                    #print(dt)
                    #print(bars[len(bars)-1])
                    break

            #When updating previously downloaded data, startDateTime should be the datetime of the last downloaded data point
            #startDate
            #April 30, 2025
            #Min Tang
            #To further address the issue of partial data retrieval before close of the day/hour, 
            #Maybe use >= instead of > in the if statement below
            #Later the combine_first method will be used to combine the new data with the existing data, which will remove the duplicates and use the data downloaded later to replace the existing data 
            #Which will be 'more' correct with regard to the time of data retrieval
            if useStartDateTime:
                #print(startdt)
                #print(type(startdt))
                if (barSizeSetting == '1 day'):
                    new_bars = [bar for bar in bars if datetime.datetime(bar.date.year, bar.date.month, bar.date.day)>=startdt]
                else:
                    new_bars = [bar for bar in bars if bar.date>=startdt]
                if len(new_bars) < 1: 
                    break
                else: 
                    bars = new_bars
            #print(bars)
            barslist.append(bars)
            dt = bars[0].date
            print(dt)
            if len(bars)<=VERY_FEW_DATA_POINTS:# there are occasions where the script hangs and keeps retrieving the same few data points 
                break

        allBars =  [b for bars in reversed(barslist) for b in bars]  
        #print(allBars[-1]) 
        df = util.df(allBars)
        return(df)

    #when updating previously downloaded time series, the datetime of the last data point is passed to the downloading method as the starting point for new data
    #for daily data the input should be a datetime.date object
    #for hourly/minute data, the input should be a datetime.datetime object
    #this method is called when a startDateTime is given 
    def _process_startDateTime(self, startDateTime, barSizeSetting) -> datetime.date|datetime.datetime|None :
        startdt = None
        if barSizeSetting=='1 day':
            #For daily data, the date field returns a datetime.date object 
            if isinstance(startDateTime, datetime.date):
                startdt = startDateTime
            elif isinstance(startDateTime, str):
                try:
                    formatStr = '%Y-%m-%d'
                    startdt = datetime.datetime.strptime(startDateTime,formatStr).date()
                except Exception as e: 
                    print(e)
                    msg = (
                            "Error '%s'.  Cannot convert startDateTime '%s' to a datetime.date object. \
                            An example of the correct format is 2023-09-27"
                            % (str(e), startDateTime)
                            )
                    self._raise_and_log_error(msg)
        else:  
            #For sub-daily data, the date field returns a datetime.datetime object
            #The input should either be a datetime.datetime object, or a string matching a specific format with timezone information
            #For sub-daily data, all datetime.datetime objects will re-base to UTC for consistency 
            if isinstance(startDateTime, datetime.datetime):
                startdt = startDateTime.astimezone(tz=datetime.timezone.utc)
            elif isinstance(startDateTime, str):
                try:
                    formatStr = '%Y-%m-%d %H:%M:%S%z'
                    startdt = datetime.datetime.strptime(startDateTime,formatStr)
                    startdt = startdt.astimezone(tz=datetime.timezone.utc)
                except Exception as e: 
                    print(e)
                    msg = (
                            "Error '%s'.  Cannot convert startDateTime '%s' to a datetime.datetime object. \
                            An example of the correct format is 2023-09-27 20:00:00+00:00"
                            % (str(e), startDateTime)
                            )
                    self._raise_and_log_error(msg)
        return(startdt)

    @staticmethod
    def _get_duration_from_barSize(barSizeSetting: str) -> str:
        duration_lookup = dict(
            [
                ("1 day", "2 Y"),
                ("1 hour", "6 M"),
                ("15 mins", "1 M"),
                ("5 mins", "2 W"),
                ("1 min", "3 D"), # I don't go into 1 min or lower timeframes so not sure whether the duration is a good choice here 
                ("10 secs", "14400 S"),
                ("1 secs", "1800 S"),
            ]
        )
        try:
            assert barSizeSetting in duration_lookup.keys()
        except:
            raise Exception(
                "Barsize %s not recognised should be one of %s"
                % (str(barSizeSetting), str(duration_lookup.keys()))
            )

        ib_duration = duration_lookup[barSizeSetting]
        return ib_duration

    @property
    def ib_conn(self) -> connectionIB:
        ib_conn = getattr(self, "_ib_conn", arg_not_supplied)
        if ib_conn is arg_not_supplied:
            ib_conn = self._get_new_ib_connection()
            self._ib_conn = ib_conn
        return ib_conn
    
    @property
    def ib(self) -> IB:
        return self.ib_conn.ib

    def _get_new_ib_connection(self) -> connectionIB:
        # Try this 5 times...
        attempts = 0
        failed_ids = []
        client_id = self._get_next_client_id_for_ib()
        while True:
            try:
                ib_conn = connectionIB(client_id, log_name=self.log_name)
                for id in failed_ids:
                    self.db_ib_broker_client_id.release_clientid(id)
                return ib_conn
            except Exception as e:
                failed_ids.append(client_id)
                client_id = self._get_next_client_id_for_ib()
                attempts += 1
                if attempts > 5:
                    for id in failed_ids:
                        self.db_ib_broker_client_id.release_clientid(id)
                    raise e

    def _get_next_client_id_for_ib(self) -> int:
        ## default to tracking ID through mongo change if required
        self.add_class_object(mongoIbBrokerClientIdData)
        client_id = self.db_ib_broker_client_id.return_valid_client_id()

        return int(client_id)
    
    def add_class_list(self, class_list: list, use_prefix: str = arg_not_supplied):
        for class_object in class_list:
            self.add_class_object(class_object, use_prefix=use_prefix)

    def add_class_object(self, class_object, use_prefix: str = arg_not_supplied):
        class_name = get_class_name(class_object)
        new_name = self._get_new_name(class_name, use_prefix=use_prefix)
        if not self._already_existing_class_name(new_name):
            resolved_instance = self._get_resolved_instance_of_class(class_object)
            self._add_new_class_with_new_name(
                resolved_instance=resolved_instance, attr_name=new_name
            )

    @property
    def mongo_db(self) -> mongoDb:
        mongo_db = getattr(self, "_mongo_db", arg_not_supplied)
        if mongo_db is arg_not_supplied:
            mongo_db = self._get_new_mongo_db()
            self._mongo_db = mongo_db

        return mongo_db

    def _get_new_mongo_db(self) -> mongoDb:
        mongo_db = mongoDb()

        return mongo_db

    @property
    def config(self) -> Config:
        config = getattr(self, "_config", None)
        if config is None:
            config = self._config = get_production_config()

        return config

    def _raise_and_log_error(self, error_msg: str):
        self.log.critical(error_msg)
        raise Exception(error_msg)

    @property
    def log(self):
        log = getattr(self, "_log", arg_not_supplied)
        if log is arg_not_supplied:
            log = get_logger(self.log_name)
            self._log = log

        return log

    @property
    def log_name(self) -> str:
        log_name = getattr(self, "_log_name", "")
        return log_name
    
    def _get_new_name(self, class_name: str, use_prefix: str = arg_not_supplied) -> str:
        split_up_name = camel_case_split(class_name)
        attr_name = identifying_name(
            split_up_name,
            keep_original_prefix=self._keep_original_prefix,
            use_prefix=use_prefix,
        )

        return attr_name
    
    def _already_existing_class_name(self, attr_name: str):
        existing_attr = getattr(self, attr_name, None)
        if existing_attr is None:
            return False
        else:
            return True
        
    def _get_resolved_instance_of_class(self, class_object):
        class_adding_method = self._get_class_adding_method(class_object)
        resolved_instance = class_adding_method(class_object)

        return resolved_instance

    def _get_class_adding_method(self, class_object):
        prefix = self._get_class_prefix(class_object)
        class_dict = dict(
            ib=self._add_ib_class,
            csv=self._add_csv_class,
            arctic=self._add_arctic_class,
            mongo=self._add_mongo_class,
            parquet=self._add_parquet_class,
        )

        method_to_add_with = class_dict.get(prefix, None)
        if method_to_add_with is None:
            error_msg = "Don't know how to handle object named %s" % get_class_name(
                class_object
            )
            self._raise_and_log_error(error_msg)

        return method_to_add_with

    def _get_class_prefix(self, class_object) -> str:
        class_name = get_class_name(class_object)
        split_up_name = camel_case_split(class_name)
        prefix = split_up_name[0]

        return prefix
    
    def _add_ib_class(self, class_object):
        log = self._get_specific_logger(class_object)
        try:
            resolved_instance = class_object(self.ib_conn, self, log=log)
        except Exception as e:
            class_name = get_class_name(class_object)
            msg = (
                "Error %s couldn't evaluate %s(self.ib_conn, self) This might be because (a) IB gateway not running, or (b) import is missing\
                         or (c) arguments don't follow pattern"
                % (str(e), class_name)
            )
            self._raise_and_log_error(msg)

        return resolved_instance
    
    def _add_csv_class(self, class_object):
        datapath = self._get_csv_paths_for_class(class_object)
        log = self._get_specific_logger(class_object)

        try:
            resolved_instance = class_object(datapath=datapath, log=log)
        except Exception as e:
            class_name = get_class_name(class_object)
            msg = (
                "Error %s couldn't evaluate %s(datapath = datapath) \
                        This might be because import is missing\
                         or arguments don't follow pattern"
                % (str(e), class_name)
            )
            self._raise_and_log_error(msg)

        return resolved_instance
    
    def _add_arctic_class(self, class_object):
        log = self._get_specific_logger(class_object)
        try:
            resolved_instance = class_object(mongo_db=self.mongo_db, log=log)
        except Exception as e:
            class_name = get_class_name(class_object)
            msg = (
                "Error %s couldn't evaluate %s(mongo_db=self.mongo_db) \
                        This might be because import is missing\
                         or arguments don't follow pattern"
                % (str(e), class_name)
            )
            self._raise_and_log_error(msg)

        return resolved_instance

    def _add_parquet_class(self, class_object):
        log = self._get_specific_logger(class_object)
        try:
            resolved_instance = class_object(
                parquet_access=self.parquet_access, log=log
            )
        except Exception as e:
            class_name = get_class_name(class_object)
            msg = (
                "Error '%s' couldn't evaluate %s(parquet_access = self.parquet_access) \
                        This might be because import is missing\
                         or arguments don't follow pattern or parquet_store is undefined"
                % (str(e), class_name)
            )
            self._raise_and_log_error(msg)

        return resolved_instance
    
    def _add_mongo_class(self, class_object):
        log = self._get_specific_logger(class_object)
        try:
            resolved_instance = class_object(mongo_db=self.mongo_db, log=log)
        except Exception as e:
            class_name = get_class_name(class_object)
            msg = (
                "Error '%s' couldn't evaluate %s(mongo_db=self.mongo_db) \
                        This might be because import is missing\
                         or arguments don't follow pattern"
                % (str(e), class_name)
            )
            self._raise_and_log_error(msg)

        return resolved_instance
    
    def _get_specific_logger(self, class_object):
        class_name = get_class_name(class_object)
        log = get_logger(self.log.name, {COMPONENT_LOG_LABEL: class_name})

        return log
    
    def _add_new_class_with_new_name(self, resolved_instance, attr_name: str):
        already_exists = self._already_existing_class_name(attr_name)
        if already_exists:
            ## not uncommon don't log or would be a sea of spam
            pass
        else:
            setattr(self, attr_name, resolved_instance)
            self._add_attr_to_list(attr_name)

    def _already_existing_class_name(self, attr_name: str):
        existing_attr = getattr(self, attr_name, None)
        if existing_attr is None:
            return False
        else:
            return True

    def _add_attr_to_list(self, new_attr: str):
        self._attr_list.append(new_attr)

    def update_log(self, new_log):
        self._log = new_log
    



if __name__ == "__main__": 
    
    #Test the PST dataBroker class
    """ data_broker = dataBroker()
    instrument_code ='GOLD'
    arbitrary_contract_date = '20240828'
    contract_object = futuresContract(instrument_code, arbitrary_contract_date)
    print(contract_object)
    prices = data_broker.get_prices_at_frequency_for_contract_object(
        contract_object, HOURLY_FREQ
    )
    print("Prices for %s:" % str(contract_object))
    print(prices) """
    #End -- Test the PST dataBroker class


    #Notes on the call sequence to retrieve data, from dataBroker.get_prices_at_frequency_for_contract_object
    #this databroker call in turn calls the function below, which ultimately returns the data.broker_futures_contract_price element of a dataBlob object
    """ def get_prices_at_frequency_for_contract_object(
        self, contract_object: futuresContract, frequency: Frequency
    ) -> futuresContractPrices:
        return self.broker_futures_contract_price_data.get_prices_at_frequency_for_contract_object(
            contract_object, frequency, return_empty=False
        ) """
    
    """ @property
    def broker_futures_contract_price_data(self) -> brokerFuturesContractPriceData:
        return self.data.broker_futures_contract_price """

    

    #For historical price retrieval, it is done through the broker_futures_contract_price of the data object
    #which is itself a sysbrokers.IB.ib_futures_contract_price_data.ibFuturesContractPriceData object 
    # it calls the get_prices_at_frequency_for_contract_object method of the object, 
    # which ultimately calls self.ib_client.broker_get_historical_futures_data_for_contract() to retrieve the data 

    #ib_client is an instance of ibPriceClient, which in turn is a derived class from ibContractClient
    #the ibPriceClient.broker_get_historical_futures_data_for_contract() method calls its _get_generic_data_for_contract() method
    # which calls the _get_barsize_and_duration_from_frequency() mthod to decide the barsize and duration parameters, based on the data frequency 
    #it calls _ib_get_historical_data_of_duration_and_barSize() to retrieve data 
    # and calls _raw_ib_data_to_df() to massage the data (column names, datetime index etc ) before returning the data 

    #_ib_get_historical_data_of_duration_and_barSize is where the actual reqHistoricalData call occurs 
    # 
    """ self.ib.reqMarketDataType(3)
    bars = self.ib.reqHistoricalData(
            ibcontract,
            endDateTime="",
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow=whatToShow,
            useRTH=True,
            formatDate=2,
            timeout=TIMEOUT_SECONDS_ON_HISTORICAL_DATA,
        )
    df = util.df(bars) """
    # In this method, the object calls its element IB, which in turn calls the reqMarketData and reqHistoricalData methods 
    #The IB element is explicitly defined either in ibPriceClient or its parent class, ibContractsClient.  
    #It is defined in ibContractsClient's super class, ibClient with the definition below: 
    """ @property
    def ib_connection(self) -> connectionIB:
        return self._ib_connnection

    @property
    def ib(self) -> IB:
        return self.ib_connection.ib """
    #Note that ibClient's initialization requires a non-empty IBconnction: 
    """ def __init__(self, ibconnection: connectionIB, log=get_logger("ibClient")):
        # means our first call won't be throttled for pacing
        self.last_historic_price_calltime = (
            datetime.datetime.now()
            - datetime.timedelta(seconds=PACING_INTERVAL_SECONDS)
        )

        # Add error handler
        ibconnection.ib.errorEvent += self.error_handler

        self._ib_connnection = ibconnection
        self._log = log
        self._cache = Cache(self)"""
    #So the ib element is indeed an element of class connectionIB(object), which is an IB class object defined in ib_insync 


    #to do: 
    #maybe setup a copy of my own ibPriceData class
    # to call: need to configure a contract information
    # The following are the same contract, and initialized using ib_sync's Future and Contract classes  
    Future('MES','20241220','CME')
    Contract(conId=654503314, exchange='CME')
    test=mtIBData()
    df = test.retrieve_historical_data_for_contract_with_frequency(contract=Future('ES','20251219','CME'))
    df = test.retrieve_historical_data_for_contract_with_frequency(contract=Future('ES','20251219','CME'), barSizeSetting='1 day')

    #df = test.retrieve_historical_data_for_contract_with_frequency(contract=Future('ES','20220617','CME'))
    #df = test.retrieve_historical_data_for_contract_with_frequency(contract=Future('ES','20241220','CME'), startDateTime='2024-03-04 14:00:00+00:00')
    #df = test.retrieve_historical_data_for_contract_with_frequency(contract=Future('ES','20241220','CME'), startDateTime='2024-03-04 14:00:00')
    #print(test._get_duration_from_barSize('1 hour'))
    df.to_csv('foo.csv',index=False)
    sleep(5)