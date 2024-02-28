# data_processor/data_processor.py
import os
import csv
import glob
import pandas as pd
from utils.logger import setup_logging 

logger = setup_logging("data_processor")

class DataProcessor:
    """
    Object to process response data from an API call into a format suitable for data analysis.
    """

    @staticmethod
    def normalize_json_to_dataframe(json_data, key=None):
        """
        Normalize nested JSON data into a flat table structure.

        :param json_data: dict or list - The JSON data to normalize.
        :param key: str or None - Optional key to specify the path to the nested element to normalize.
        :return: DataFrame - The normalized data as a Pandas DataFrame.
        """
        return pd.json_normalize(json_data, record_path=key)
    
    def list_json_to_dataframe(self, list_dict, key=None):
        """
        Convert a list of dictionaries to a Pandas DataFrame.

        :param list_dict: list of dicts - The list of dictionaries to convert.
        :param key: str or None - Optional key to specify which nested dictionaries to convert.
        :return: DataFrame - The concatenated DataFrame from the list of dictionaries.
        """
        frames = [self.normalize_json_to_dataframe(dict_unit, key=key) for dict_unit in list_dict if key is None or key in dict_unit]
        return pd.concat(frames, ignore_index=True)
    

class ColumnMismatch:
    """
    Class to log column mismatch
    """
    def __init__(self, column_context):
        self.column_context = column_context

    def log_column_mismatch(self, df, file_name):
        """
        Log the column names if they match with the reference DataFrame.
        If there is a mismatch, log the details of the CSV file.
        """
        df_columns = set(df.columns)
        reference_columns = self.column_context

        if df_columns == reference_columns:
            pass
        else:
            extra_columns = df_columns - reference_columns
            missing_columns = reference_columns - df_columns

            if extra_columns:
                reference_columns = reference_columns.update(extra_columns)
                logger.warning(f"Extra columns in file {file_name}: {', '.join(extra_columns)}")
            if missing_columns:
                logger.warning(f"Missing columns in file {file_name}: {', '.join(missing_columns)}")
            
        return None


class LocalStageOrchestrator:
    def __init__(self, staging_location) -> None:
        # Configuration parameters
        self.sentinel_value = "0001-01-01 00:00:00.000"
        self.datetime_format = "%Y-%m-%d %H:%M:%S.%f"
        self.staging_location = staging_location
        self.column_context = None
        
    def preprocess(self, df):
        """
        Clean the DataFrame: replace 'NaT' values, convert datetime columns to strings,
        and convert columns with more than one type to string
        """

        # Vectorized operation for datetime columns conversion and 'NaT' replacement
        datetime_cols = df.select_dtypes(include=['datetime64[ns]', '<M8[ns]']).columns
        df[datetime_cols] = df[datetime_cols].apply(lambda x: x.dt.strftime(self.datetime_format).fillna(self.sentinel_value), axis=1)  

        # Column level adjustments
        for column in df.columns:
            # Convert columns with mixed data types into string
            if df[column].apply(type).nunique() > 1:
                df[column] = df[column].astype(str)
            
            # check if the column contains numeric values and replace with null
            if pd.api.types.is_numeric_dtype(df[column]):
                df[column].fillna('NULL', inplace=True)

        # Remove any special characters from the DataFrame
        try:
            df.replace(to_replace=[r"\\t|\\n|\\r", "\\t|\\n|\\r",'"'], value=["","",""], regex=True, inplace=True)
        except ValueError:
            df.replace("\\n", "", inplace=True)
        except Exception as e:
            logger.error(f"Error while replacing special characters: {e}")
            raise

        return df

    def stage_locally(self, df ,file_path):
        """
        Preprocess the DataFrame and save it as a CSV file
        """
        # Export the DataFrame to a CSV file
        try:
            df.to_csv(
                file_path,
                index=False,
                sep='~',
                encoding='utf-8',
                na_rep='NULL',  # Replace missing values with 'NULL'
                quoting=csv.QUOTE_NONNUMERIC,  # Quote all non-numeric values
                quotechar='"',  # Use double quotes as the quoting character
                lineterminator='\n'
            )
            return True
        except Exception as e:
            logger.error(f"Error while saving DataFrame to CSV: {e}")
            return False
        
    def generate_col_definitions(self, df):
        column_definitions = [f'"{col}" {self.map_dtype_to_snowflake(dtype)}' for col, dtype in zip(df.columns, df.dtypes)]
        return ', '.join(column_definitions)

    @staticmethod
    def map_dtype_to_snowflake(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'NUMBER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        return 'TEXT'  # Default to TEXT for string and other types

    def process_flat_files(self, input_location):
        """
        Process Excel files: read the files, clean the data, and save it as CSV files
        """
        for i, file_name in enumerate(os.listdir(input_location), start=1):
            file_path = os.path.join(input_location, file_name)
            try:
                if (file_name.endswith('.xlsx') or file_name.endswith('.XLSX') or file_name.endswith('.xls')) and os.path.isfile(file_path):
                    # Read the Excel file
                    df = pd.read_excel(file_path)
                elif file_name.endswith('.csv') and os.path.isfile(file_path):
                    df = pd.read_csv(file_path)
            except Exception as e:
                logger.error(f"Error while reading the files in the input folder: {e}")
                raise

            # Log Column mismatch if any
            if i == 1:
                log_col_mismatch = ColumnMismatch(column_context=set(df.columns))
            log_col_mismatch.log_column_mismatch(df, file_name)
            
            # Clean the DataFrame
            df = self.preprocess(df)
            
            # Add Column for file identifier
            df['File Name'] = file_name
            
            # Save the DataFrame as a CSV file
            stage_file_path = os.path.join(self.staging_location, f'{os.path.splitext(file_name)[0]}.csv')
            self.stage_locally(df, stage_file_path)

        logger.info(f"Contents of {input_location} have successfully been staged in {self.staging_location}")
        
        column_definition = self.generate_col_definitions(df)

        return column_definition
    
    def delete_folder_contents(self,folder_path):
        """
        Recursively delete the contents of a folder.
        """
        try:
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                if os.path.isdir(file_path):
                    self.delete_folder_contents(file_path)
                    os.rmdir(file_path)
                else:
                    os.remove(file_path)
            logger.info(f"Folder content of: {folder_path} deleted!")
        except Exception as e:
            logger.error(f"Error while deleting folder contents: {e}", exc_info=True)
            raise
    




