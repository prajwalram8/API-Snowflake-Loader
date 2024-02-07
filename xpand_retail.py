import os
import datetime as dt
from utils.logger import setup_logging
from utils.utils import ProjectDirectory
from api.api_handler import APIHandler
from data_processor.data_processor import DataProcessor, LocalStageOrchestrator
from state_manager.state_manager import StateManager
from credentials.credential_manager import CredentialManager
from db.snowflake_loader import DataLoader

# Initializing helper classes and functions
logger = setup_logging(__name__)

class XpandRetail():
    def __init__(self):

        # Initializing API Attributes
        self.name = os.path.basename(__file__).replace('.py','')
        self.base_url = 'http://dlapi.xpandretail.com:18085'
        self.login_path = 'api/v1/user/login'
        self.login_headers = {
            'Content-Type': 'application/json'
        }
        
        # Initializing Necessary Helper Objects
        self.credentials = CredentialManager()
        self.api_handler = APIHandler(base_url=self.base_url)
        self.data_processor = DataProcessor()
        self.state = StateManager(name=self.name)
        self.project_dir = ProjectDirectory(name=self.name)
        self.dataloader = DataLoader()
        self.local_stage_orchestrator = LocalStageOrchestrator(
            staging_location=self.project_dir.get_directories('snowflake_stage')
            )

        # Initialize auth token
        self.auth_code = self.get_auth_code()
        self.call_header = {
            'authorization': self.auth_code
            }
        
        # Initializing class attributes
        self.timestamp_run = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        self.startDate = dt.datetime.strptime(self.state.get_last_state(),"%Y-%m-%d").date()
        self.endDate = dt.datetime.now().date()


    def get_auth_code(self):

        login_credentials=self.credentials.get_credentials(
            appkey='xpandretail', 
            username='xpandretail', 
            password='xpandretail'
            )
        
        response_json = self.api_handler.make_request(
            endpoint=self.login_path,
            method='POST',
            data=login_credentials,
            headers=self.login_headers
        )
        logger.info("Auth Token retrived successfully")
        return response_json['atoken']

    def get_store_info(self, endpoint=None, method=None):
        # Store Info
        store_info = self.api_handler.make_request(
            endpoint=endpoint,
            method=method,
            headers=self.call_header
            )
        return store_info

    def get_store_entrance_info(self, store_id, endpoint:str, method:str):
        # Store Entrance Info
        store_entrance_info = self.api_handler.make_request(
            endpoint=endpoint,
            method=method,
            headers=self.call_header,
            params={
                'plaza_unid': store_id
                }
                )
        return store_entrance_info
    
    def get_store_count(self, store_id:str, startTime:str, endTime:str, endpoint:str, method:str):
        store_counts = self.api_handler.make_request(
            endpoint=endpoint,
            method=method,
            headers=self.call_header,
            params={
                'plaza_unid': store_id,
                'startTime': startTime,
                'endTime': endTime
            }
        )
        return store_counts

    def get_store_cust_segments(self,store_id:str, startTime:str, endTime:str, endpoint:str, method:str):
        store_counts = self.api_handler.make_request(
            endpoint=endpoint,
            method=method,
            headers=self.call_header,
            params={
                'plazaUnid': store_id,
                'startTime': startTime,
                'endTime': endTime
            }
        )
        return store_counts
    
    def preprocess_and_upload(self, name, load_type='truncate'):
        local_stage = self.project_dir.get_directories(name)
        snowflake_stage = self.project_dir.get_directories('snowflake_stage')
        col_definition_string = self.local_stage_orchestrator.process_flat_files(local_stage)
        self.dataloader.manage_data_loading(name=name, local_stage_path=snowflake_stage, col_def_str=col_definition_string, load_type=load_type)
        self.local_stage_orchestrator.delete_folder_contents(folder_path=snowflake_stage)
        logger.info(f"Preprocessing & Upload of {name} has been completed.")
        return None

    
    def extract_and_stage(self):

        if self.startDate == self.endDate:
            logger.info("State indicated injestion completed for the day. Skipping injestion...")
            return None
        
        self.project_dir.create_ds_if_not_exists('store_counts', 'store_cust_seg_counts', 'store_entrance_info', 'store_info')
        
        # Get store info
        store_info = self.get_store_info(endpoint='api/v1/base/plazaInfo', method='GET')
        store_info = self.data_processor.process_data(store_info['data'])
        store_info.to_csv(
            os.path.join(
                self.project_dir.get_directories('store_info'),
                'store_info.csv'
                )
        )
        self.preprocess_and_upload(name='store_info', load_type='truncate')

        # Get store entrance master
        store_entrance_info = []
        for store_id in store_info['plaza_unid']:
            
            store_entrance_info.append(
                self.get_store_entrance_info(
                    store_id=store_id, 
                    endpoint='api/v1/base/gateInfo', 
                    method='GET'
                    )
            )
        store_entrance_info = self.data_processor.list_dict_to_pd(list_dict=store_entrance_info, key='data')
        store_entrance_info.to_csv(
            os.path.join(
                self.project_dir.get_directories('store_entrance_info'),
                f'store_entrance_info.csv'
                )
        )
        self.preprocess_and_upload(name='store_entrance_info', load_type='truncate')

        while self.startDate < self.endDate:
            store_counts = []
            store_cust_seg_counts = []

            # Need to define logic to assign start date and enddate
            startTime =  dt.datetime.combine(self.startDate, dt.time(0,0,0)).strftime("%Y-%m-%d %H:%M:%S")
            endTime = dt.datetime.combine(self.startDate, dt.time(23,59,59)).strftime("%Y-%m-%d %H:%M:%S")

            timestamp_day = dt.datetime.combine(self.startDate, dt.time(0,0,0)).strftime("%Y%m%d%H%M%S")
            
            # Get store counts
            for store_id in store_info['plaza_unid']:
                
                store_counts.append(
                    self.get_store_count(
                        store_id=store_id,
                        startTime=startTime,
                        endTime=endTime,
                        endpoint='api/v1/face/storeCountingDataHourly',
                        method='GET'
                    )
                )

                store_cust_seg_counts.append(
                    self.get_store_cust_segments(
                        store_id=store_id,
                        startTime=startTime,
                        endTime=endTime,
                        endpoint='api/v2/reid/plazaHour',
                        method='GET'
                    )
                )

            # converting list dict into single data frame
            store_counts = self.data_processor.list_dict_to_pd(list_dict=store_counts, key='data')
            store_cust_seg_counts = self.data_processor.list_dict_to_pd(list_dict=store_cust_seg_counts, key='data')

            # staging the dataframe into persistent memory
            store_counts.to_csv(
                os.path.join(
                    self.project_dir.get_directories('store_counts'),
                    f'store_counts_{timestamp_day}.csv'
                    )
            )
            
            store_cust_seg_counts.to_csv(
                os.path.join(
                    self.project_dir.get_directories('store_cust_seg_counts'),
                    f'store_cust_seg_counts_{timestamp_day}.csv'
                    )
            )
            
            logger.info(f"Completed extraction for date {self.startDate}")

            #increment for while
            self.startDate += dt.timedelta(days=1)

        #Bulk Upload
        self.preprocess_and_upload(name='store_counts', load_type='insert')
        self.preprocess_and_upload(name='store_cust_seg_counts', load_type='insert')
        
        # update the state
        self.state.update_state(last_run_date=self.startDate.strftime("%Y-%m-%d"))
        self.local_stage_orchestrator.delete_folder_contents(folder_path=self.project_dir.name)
        logger.info("Extraction job completed successfully")



if __name__ == "__main__":

    # Call API for data
    xpand_retail_api = XpandRetail()
    xpand_retail_api.extract_and_stage()