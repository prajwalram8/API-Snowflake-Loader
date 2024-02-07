from db.snowflake_loader import DataLoader
from data_processor.data_processor import LocalStageOrchestrator
from utils.utils import ProjectDirectory, py_file_name



if __name__ == "__main__":
    dataloader = DataLoader(config_path='config.ini')
    # project_directory = ProjectDirectory(name = 'xpand_retail')


    # # load process
    local_stage_orchestrator = LocalStageOrchestrator(input_location='./data/xpand_retail/store_info', staging_location='./data/xpand_retail/snowflake_stage')
    col_def_string = local_stage_orchestrator.process_flat_files()
    print(col_def_string)
    
    # Load data
    # print(dataloader.file_format())
    # dataloader.table_exists('STORE_INFO_TABLE')
    dataloader.manage_data_loading(name='STORE_INFO', local_stage_path='./data/xpand_retail/snowflake_stage', col_def_str=col_def_string, load_type='insert')
