"""
Data Cleansing Script for CSV Data
Author: Bintang Fajar
Description: Clean CSV data from duplicates and load into database
"""

import os
import json
import pandas as pd
import psycopg2
from datetime import datetime
from typing import Tuple, Dict, Any
import logging
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataCleaner:
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'test_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }
        self.source_path = os.getenv('SOURCE_PATH', './source')
        self.target_path = os.getenv('TARGET_PATH', './target')
        self.test_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
        
    def get_db_connection(self):
        max_retries = 5
        retry_delay = 3
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to database (attempt {attempt + 1}/{max_retries})...")
                logger.info(f"Connection details: host={self.db_config['host']}, port={self.db_config['port']}, db={self.db_config['database']}")
                
                conn = psycopg2.connect(**self.db_config)
                logger.info("Database connection established successfully")
                return conn
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to database after {max_retries} attempts")
                    raise
    
    def read_csv(self, filename: str) -> pd.DataFrame:
        try:
            filepath = os.path.join(self.source_path, filename)
            df = pd.read_csv(filepath)
            logger.info(f"Successfully read CSV file: {filename}")
            logger.info(f"Total rows in CSV: {len(df)}")
            logger.info(f"Columns: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
    
    def clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
   
        try:
            # Identify duplicates based on 'ids' column
            duplicates_mask = df.duplicated(subset=['ids'], keep='first')
            
            clean_data = df[~duplicates_mask].copy()
            duplicate_data = df[duplicates_mask].copy()
            
            logger.info(f"Clean records: {len(clean_data)}")
            logger.info(f"Duplicate records: {len(duplicate_data)}")
            
            return clean_data, duplicate_data
        except Exception as e:
            logger.error(f"Error during data cleaning: {e}")
            raise
    
    def _parse_date(self, date_str):
        if pd.isna(date_str) or date_str == '':
            return None
        
        date_str = str(date_str).strip()
        
        date_formats = [
            '%d/%m/%Y',  # 13/04/2024 (DD/MM/YYYY) 
            '%m/%d/%Y',  # 04/13/2024 (MM/DD/YYYY)
            '%Y-%m-%d',  # 2024-04-13 (ISO format)
            '%d-%m-%Y',  # 13-04-2024
            '%Y/%m/%d',  # 2024/04/13
            '%d.%m.%Y',  # 13.04.2024
            '%Y%m%d',    # 20240413
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except:
                continue
        
        try:
            parsed_date = pd.to_datetime(date_str, dayfirst=True)
            return parsed_date.strftime('%Y-%m-%d')
        except:
            logger.warning(f"Could not parse date: {date_str}, returning as-is")
            return date_str
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df_transformed = df.copy()
            
            if 'dates' in df_transformed.columns:
                logger.info("Transforming dates...")
                df_transformed['dates'] = df_transformed['dates'].apply(self._parse_date)
                logger.info(f"Sample dates after transformation: {df_transformed['dates'].head(3).tolist()}")
            
            if 'names' in df_transformed.columns:
                df_transformed['names'] = df_transformed['names'].str.upper()
            
            numeric_columns = ['monthly_listeners', 'popularity', 'followers', 'num_releases', 'num_tracks']
            for col in numeric_columns:
                if col in df_transformed.columns:
                    df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce').fillna(0).astype(int)
            
            for col in ['genres', 'feat_track_ids']:
                if col in df_transformed.columns:
                    df_transformed[col] = df_transformed[col].apply(self._parse_array)
            
            for col in ['first_release', 'last_release']:
                if col in df_transformed.columns:
                    df_transformed[col] = df_transformed[col].astype(str).replace('nan', '').replace('None', '')
            
            logger.info("Data transformation completed successfully")
            return df_transformed
        except Exception as e:
            logger.error(f"Error during data transformation: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def _parse_array(self, value) -> list:
        if pd.isna(value) or value == '':
            return []
        
        if isinstance(value, str):
            value = value.strip('[]').replace("'", "").replace('"', '')
            if value:
                return [item.strip() for item in value.split(',') if item.strip()]
        
        return []
    
    def insert_to_database(self, df: pd.DataFrame, table_name: str, is_reject: bool = False):
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            inserted_count = 0
            error_count = 0
            
            for idx, row in df.iterrows():
                columns = list(row.index)
                values = []
                
                for col in columns:
                    val = row[col]
                    if col in ['genres', 'feat_track_ids']:
                        if isinstance(val, list):
                            values.append(val)
                        else:
                            values.append([])
                    else:
                        values.append(val)
                
                if is_reject:
                    columns.append('reject_reason')
                    values.append('Duplicate ID')
                
                placeholders = ', '.join(['%s'] * len(values))
                columns_str = ', '.join(columns)
                
                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                try:
                    cursor.execute(insert_query, values)
                    inserted_count += 1
                except Exception as row_error:
                    logger.warning(f"Error inserting row {idx}: {row_error}")
                    error_count += 1
                    continue
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Successfully inserted {inserted_count} rows into {table_name} table")
            if error_count > 0:
                logger.warning(f"Failed to insert {error_count} rows")
        except Exception as e:
            logger.error(f"Error inserting data to database: {e}")
            raise
    
    def save_to_csv(self, df: pd.DataFrame, filename: str):
        try:
            os.makedirs(self.target_path, exist_ok=True)
            filepath = os.path.join(self.target_path, filename)
            df.to_csv(filepath, index=False)
            logger.info(f"CSV file saved: {filepath}")
        except Exception as e:
            logger.error(f"Error saving CSV file: {e}")
            raise
    
    def save_to_json(self, df: pd.DataFrame, filename: str):
        try:
            os.makedirs(self.target_path, exist_ok=True)
            
            # Prepare data in required format
            data_list = []
            for _, row in df.iterrows():
                record = {
                    "dates": str(row['dates']) if pd.notna(row['dates']) else "",
                    "ids": str(row['ids']),
                    "names": str(row['names']),
                    "monthly_listeners": int(row['monthly_listeners']) if pd.notna(row['monthly_listeners']) else 0,
                    "popularity": int(row['popularity']) if pd.notna(row['popularity']) else 0,
                    "followers": int(row['followers']) if pd.notna(row['followers']) else 0,
                    "genres": row['genres'] if isinstance(row['genres'], list) else [],
                    "first_release": str(row['first_release']) if pd.notna(row['first_release']) and str(row['first_release']) not in ['nan', 'None', ''] else "",
                    "last_release": str(row['last_release']) if pd.notna(row['last_release']) and str(row['last_release']) not in ['nan', 'None', ''] else "",
                    "num_releases": int(row['num_releases']) if pd.notna(row['num_releases']) else 0,
                    "num_tracks": int(row['num_tracks']) if pd.notna(row['num_tracks']) else 0,
                    "playlists_found": str(row['playlists_found']) if pd.notna(row['playlists_found']) else "",
                    "feat_track_ids": row['feat_track_ids'] if isinstance(row['feat_track_ids'], list) else []
                }
                data_list.append(record)
            
            output_data = {
                "row_count": len(df),
                "data": data_list
            }
            
            filepath = os.path.join(self.target_path, filename)
            with open(filepath, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            logger.info(f"JSON file saved: {filepath}")
        except Exception as e:
            logger.error(f"Error saving JSON file: {e}")
            raise
    
    def get_table_count(self, table_name: str) -> int:
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count
        except Exception as e:
            logger.error(f"Error getting table count: {e}")
            return 0
    
    def run(self):
        try:
            logger.info("=" * 50)
            logger.info("Starting Data Cleansing Process")
            logger.info("=" * 50)
            
            logger.info("Step 1: Reading CSV file...")
            df = self.read_csv('scrap.csv')
            
            logger.info("Step 2: Cleaning data (removing duplicates)...")
            clean_data, duplicate_data = self.clean_data(df)
            
            logger.info("Step 3: Transforming data...")
            clean_data_transformed = self.transform_data(clean_data)
            duplicate_data_transformed = self.transform_data(duplicate_data)
            
            logger.info("Step 4: Inserting data to database...")
            if not clean_data_transformed.empty:
                self.insert_to_database(clean_data_transformed, 'data', is_reject=False)
            
            if not duplicate_data_transformed.empty:
                self.insert_to_database(duplicate_data_transformed, 'data_reject', is_reject=True)
            
            logger.info("Step 5: Saving duplicate data to CSV...")
            csv_filename = f"data_reject_{self.test_datetime}.csv"
            if not duplicate_data.empty:
                self.save_to_csv(duplicate_data, csv_filename)
            else:
                logger.info("No duplicate data to save")
            
            logger.info("Step 6: Saving clean data to JSON...")
            json_filename = f"data_{self.test_datetime}.json"
            self.save_to_json(clean_data_transformed, json_filename)
            
            logger.info("Step 7: Verifying database records...")
            data_count = self.get_table_count('data')
            data_reject_count = self.get_table_count('data_reject')
            
            logger.info("=" * 50)
            logger.info("Process Completed Successfully!")
            logger.info(f"Clean records in database: {data_count}")
            logger.info(f"Duplicate records in database: {data_reject_count}")
            logger.info(f"JSON file: {json_filename}")
            logger.info(f"CSV file: {csv_filename}")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Error in main execution: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise


if __name__ == "__main__":
    try:
        cleaner = DataCleaner()
        cleaner.run()
    except Exception as e:
        logger.error(f"Application failed: {e}")
        exit(1)