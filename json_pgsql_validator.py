"""
JSON to PostgreSQL Data Consistency Validator (Updated for new enforcement schema)

This program validates the consistency between JSON source files and the imported PostgreSQL data
by sampling records and comparing their attributes.
"""
import os
import json
import glob
import random
import psycopg2
import pandas as pd
import re
from datetime import datetime
from tqdm.auto import tqdm
from typing import Dict, List, Any, Tuple, Optional

# Base directory configurations - update with actual paths
BASE_DIR = '/Volumes/Lexar SSD NM1090 PRO 2TB/GitHub/FAERS/datafiles/unzip/device/'  # Root directory
DATA_DIRS = {
    'classification_dir': os.path.join(BASE_DIR, 'classification'),
    'enforcement_dir': os.path.join(BASE_DIR, 'enforcement'),
    'event_dir': os.path.join(BASE_DIR, 'event'),
    'recall_dir': os.path.join(BASE_DIR, 'recall'),
    'udi_dir': os.path.join(BASE_DIR, 'udi'),
}

# Database configuration - update with actual credentials
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'fda_device',
    'user': 'postgres',
    'password': '12345687'
}

# Color formatting for console output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class JsonPgsqlValidator:
    """Validates consistency between JSON files and PostgreSQL database"""
    
    def __init__(self, db_config: Dict, data_dirs: Dict):
        """Initialize the validator with database and directory configurations"""
        self.db_config = db_config
        self.data_dirs = data_dirs
        self.conn = None
        self.cur = None
        self.results = {
            'classification': {'total': 0, 'sampled': 0, 'matched': 0, 'mismatched': 0, 'details': []},
            'recall': {'total': 0, 'sampled': 0, 'matched': 0, 'mismatched': 0, 'details': []},
            'enforcement': {'total': 0, 'sampled': 0, 'matched': 0, 'mismatched': 0, 'details': []},
            'adverse_event': {'total': 0, 'sampled': 0, 'matched': 0, 'mismatched': 0, 'details': []},
            'udi': {'total': 0, 'sampled': 0, 'matched': 0, 'mismatched': 0, 'details': []}
        }
        
    def connect_to_db(self) -> bool:
        """Connect to the PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            self.cur.execute("SET search_path TO device;")
            print(f"{Colors.GREEN}Connected to PostgreSQL database {self.db_config['dbname']}{Colors.ENDC}")
            return True
        except Exception as e:
            print(f"{Colors.RED}Database connection failed: {str(e)}{Colors.ENDC}")
            return False
    
    def close_db_connection(self):
        """Close the database connection"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print(f"{Colors.BLUE}Database connection closed{Colors.ENDC}")
    
    def get_file_paths(self, data_type: str) -> List[str]:
        """Get file paths for the given data type"""
        if data_type == 'classification':
            return glob.glob(os.path.join(self.data_dirs['classification_dir'], 'device-classification-*.json'))
        elif data_type == 'enforcement':
            return glob.glob(os.path.join(self.data_dirs['enforcement_dir'], 'device-enforcement-*.json'))
        elif data_type == 'recall':
            return glob.glob(os.path.join(self.data_dirs['recall_dir'], 'device-recall-*.json'))
        elif data_type == 'adverse_event':
            event_files = []
            for root, dirs, files in os.walk(self.data_dirs['event_dir']):
                for file in files:
                    if file.startswith('device-event-') and file.endswith('.json'):
                        event_files.append(os.path.join(root, file))
            return event_files
        elif data_type == 'udi':
            return glob.glob(os.path.join(self.data_dirs['udi_dir'], 'device-udi-*.json'))
        return []
    
    def load_json_file(self, file_path: str) -> Dict:
        """Load JSON file and return its content"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"{Colors.RED}Failed to load JSON file {file_path}: {str(e)}{Colors.ENDC}")
            return {}
    
    def sample_json_records(self, data_type: str, sample_size: int = 10, sample_ratio: float = 0.01) -> List[Dict]:
        """Sample records from JSON files for the given data type"""
        files = self.get_file_paths(data_type)
        
        if not files:
            print(f"{Colors.YELLOW}No {data_type} files found{Colors.ENDC}")
            return []
        
        print(f"{Colors.BLUE}Sampling {data_type} records from {len(files)} files...{Colors.ENDC}")
        
        all_records = []
        total_records = 0
        
        # First pass: count total records
        for file_path in tqdm(files, desc=f"Counting {data_type} records"):
            data = self.load_json_file(file_path)
            if data and 'results' in data:
                total_records += len(data['results'])
        
        # Calculate actual sample size based on ratio or minimum sample size
        actual_sample_size = max(sample_size, int(total_records * sample_ratio))
        actual_sample_size = min(actual_sample_size, total_records)  # Don't sample more than available
        
        # Update total count in results
        self.results[data_type]['total'] = total_records
        self.results[data_type]['sampled'] = actual_sample_size
        
        print(f"{Colors.BLUE}Total {data_type} records: {total_records}, sampling {actual_sample_size} records{Colors.ENDC}")
        
        # Second pass: collect records and perform random sampling
        if actual_sample_size == total_records:  # If sampling all records
            for file_path in tqdm(files, desc=f"Collecting all {data_type} records"):
                data = self.load_json_file(file_path)
                if data and 'results' in data:
                    all_records.extend(data['results'])
        else:
            # Reservoir sampling algorithm for large datasets
            sample_indices = set(random.sample(range(total_records), actual_sample_size))
            current_index = 0
            
            for file_path in tqdm(files, desc=f"Sampling {data_type} records"):
                data = self.load_json_file(file_path)
                if data and 'results' in data:
                    file_records = data['results']
                    for record in file_records:
                        if current_index in sample_indices:
                            # Add file path for reference
                            record['_source_file'] = os.path.basename(file_path)
                            all_records.append(record)
                        current_index += 1
                        # Break early if we've collected all samples
                        if len(all_records) >= actual_sample_size:
                            break
                if len(all_records) >= actual_sample_size:
                    break
        
        print(f"{Colors.GREEN}Sampled {len(all_records)} {data_type} records{Colors.ENDC}")
        return all_records
    
    def get_primary_key_for_data_type(self, data_type: str) -> Tuple[str, str]:
        """Get the primary key field name and table name for the given data type"""
        if data_type == 'classification':
            return 'product_code', 'device_classifications'
        elif data_type == 'enforcement':
            return 'recall_number', 'enforcement_actions'  # Updated to use recall_number
        elif data_type == 'recall':
            return 'recall_number', 'device_recalls'
        elif data_type == 'adverse_event':
            return 'report_number', 'adverse_events'
        elif data_type == 'udi':
            return 'public_device_record_key', 'udi_records'
        return '', ''
    
    def get_key_field_for_json_record(self, data_type: str, json_record: Dict) -> Optional[str]:
        """Extract the primary key value from a JSON record"""
        if data_type == 'classification':
            return json_record.get('product_code')
        elif data_type == 'enforcement':
            # For enforcement records, get the recall_number directly
            return json_record.get('recall_number')
        elif data_type == 'recall':
            return json_record.get('product_res_number') or json_record.get('recall_number')
        elif data_type == 'adverse_event':
            return json_record.get('report_number')
        elif data_type == 'udi':
            return json_record.get('public_device_record_key')
        return None
    
    def get_fields_to_compare(self, data_type: str) -> List[Tuple[str, str]]:
        """Get list of fields (json_field, db_field) to compare for each data type"""
        if data_type == 'classification':
            return [
                ('product_code', 'product_code'),
                ('device_name', 'device_name'),
                ('device_class', 'device_class'),
                ('regulation_number', 'regulation_number'),
                ('medical_specialty', 'medical_specialty'),
                ('medical_specialty_description', 'medical_specialty_description')
            ]
        elif data_type == 'enforcement':
            return [
                # Direct field mappings for enforcement records
                ('recall_number', 'recall_number'),
                ('status', 'status'),
                ('recall_status', 'status'),  # Handle both status and recall_status
                ('classification', 'classification'),
                ('product_code', 'product_code'),
                ('recalling_firm', 'firm_name'),  # Field name mapping
                ('event_id', 'event_id'),
                ('product_description', 'product_description'),
                ('reason_for_recall', 'reason_for_recall'),
                ('code_info', 'code_info')
            ]
        elif data_type == 'recall':
            return [
                ('recall_number', 'recall_number'),
                ('product_res_number', 'recall_number'),  # Map product_res_number to recall_number
                ('product_code', 'product_code'),
                ('status', 'status'),
                ('recall_status', 'status'),  # Map recall_status to status
                ('classification', 'classification'),
                ('recalling_firm', 'recalling_firm'),
                ('product_description', 'product_description')
            ]
        elif data_type == 'adverse_event':
            return [
                ('report_number', 'report_number'),
                ('event_type', 'event_type'),
                ('manufacturer_name', 'manufacturer_name'),
                ('report_source_code', 'report_source_code')
            ]
        elif data_type == 'udi':
            return [
                ('public_device_record_key', 'public_device_record_key'),
                ('brand_name', 'brand_name'),
                ('device_description', 'device_description'),
                ('company_name', 'company_name'),
                ('record_status', 'record_status')
            ]
        return []
    
    def get_db_record(self, data_type: str, key_value: str) -> Optional[Dict]:
        """Query database to get record with the given key value"""
        key_field, table_name = self.get_primary_key_for_data_type(data_type)
        if not key_field or not table_name:
            print(f"{Colors.RED}Unknown data type: {data_type}{Colors.ENDC}")
            return None
        
        try:
            fields_to_compare = self.get_fields_to_compare(data_type)
            db_fields = []
            for json_field, db_field in fields_to_compare:
                if db_field not in db_fields:
                    db_fields.append(db_field)
            
            query = f"""
                SELECT {', '.join(db_fields)}
                FROM device.{table_name}
                WHERE {key_field} = %s
            """
            
            self.cur.execute(query, (key_value,))
            result = self.cur.fetchone()
            
            if result:
                return {field: value for field, value in zip(db_fields, result)}
                
            # If no record found and it's an enforcement record, log additional info
            if data_type == 'enforcement' and not result:
                print(f"{Colors.YELLOW}No enforcement record found with recall_number={key_value}{Colors.ENDC}")
                
                # Check if any records exist in the enforcement_actions table
                self.cur.execute("SELECT COUNT(*) FROM device.enforcement_actions")
                count = self.cur.fetchone()[0]
                if count == 0:
                    print(f"{Colors.RED}WARNING: enforcement_actions table is empty!{Colors.ENDC}")
                else:
                    # Sample a few records to see what's in the table
                    self.cur.execute("SELECT recall_number FROM device.enforcement_actions LIMIT 5")
                    samples = self.cur.fetchall()
                    print(f"{Colors.BLUE}Sample recall_number values in database: {[s[0] for s in samples]}{Colors.ENDC}")
            
            return None
        except Exception as e:
            print(f"{Colors.RED}Database query failed for {data_type} record with key {key_value}: {str(e)}{Colors.ENDC}")
            print(f"{Colors.YELLOW}Query attempted: SELECT ... FROM device.{table_name} WHERE {key_field} = '{key_value}'{Colors.ENDC}")
            return None
    
    def compare_json_with_db(self, data_type: str, json_record: Dict, db_record: Dict) -> Tuple[bool, List[Dict]]:
        """Compare JSON record with database record and return comparison results"""
        fields_to_compare = self.get_fields_to_compare(data_type)
        differences = []
        is_match = True
        
        # Track fields we've already compared to avoid duplicates
        compared_db_fields = set()
        
        for json_field, db_field in fields_to_compare:
            # Skip if we've already compared this database field with another JSON field
            if db_field in compared_db_fields:
                continue
                
            json_value = json_record.get(json_field)
            db_value = db_record.get(db_field)
            
            # Skip comparison if json field doesn't exist in this record
            if json_field not in json_record:
                continue
                
            # Skip comparison if both values are None or empty
            if (json_value is None or json_value == '') and (db_value is None or db_value == ''):
                compared_db_fields.add(db_field)
                continue
            
            # Basic type conversions for comparison
            if isinstance(db_value, str) and isinstance(json_value, (int, float)):
                json_value = str(json_value)
            if isinstance(json_value, str) and isinstance(db_value, (int, float)):
                db_value = str(db_value)
            
            # Convert lists to strings for comparison if needed
            if isinstance(json_value, list):
                json_value = str(json_value)
            if isinstance(db_value, list):
                db_value = str(db_value)
            
            # Date format normalization if values look like dates
            if isinstance(json_value, str) and isinstance(db_value, str):
                # Try to normalize date formats like "2025-01-31" and "20250131"
                if re.match(r'\d{4}-\d{2}-\d{2}', json_value) and re.match(r'\d{8}', db_value):
                    json_value = json_value.replace('-', '')
                elif re.match(r'\d{8}', json_value) and re.match(r'\d{4}-\d{2}-\d{2}', db_value):
                    db_value = db_value.replace('-', '')
            
            # Compare values
            if json_value != db_value:
                is_match = False
                differences.append({
                    'field': f"{json_field} -> {db_field}",
                    'json_value': json_value,
                    'db_value': db_value
                })
            
            # Mark this DB field as compared
            compared_db_fields.add(db_field)
        
        return is_match, differences
    
    def perform_database_diagnostics(self):
        """Perform diagnostics on database tables and report findings"""
        print(f"\n{Colors.HEADER}Performing Database Diagnostics...{Colors.ENDC}")
        
        try:
            # Check schema existence
            self.cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'device'")
            if not self.cur.fetchone():
                print(f"{Colors.RED}CRITICAL: 'device' schema does not exist in the database!{Colors.ENDC}")
                return False
                
            # Check table existence and record counts
            table_mapping = {
                'classification': 'device_classifications',
                'enforcement': 'enforcement_actions',
                'recall': 'device_recalls',
                'adverse_event': 'adverse_events',
                'udi': 'udi_records'
            }
            
            print("\nTable Record Counts:")
            for data_type, table_name in table_mapping.items():
                try:
                    self.cur.execute(f"SELECT COUNT(*) FROM device.{table_name}")
                    count = self.cur.fetchone()[0]
                    print(f"  - {table_name}: {count} records")
                except Exception as e:
                    print(f"{Colors.RED}  - {table_name}: ERROR - {str(e)}{Colors.ENDC}")
            
            # Check enforcement_actions table structure
            print("\nEnforcement Actions Table Structure:")
            try:
                self.cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'device' AND table_name = 'enforcement_actions'
                    ORDER BY ordinal_position
                """)
                columns = self.cur.fetchall()
                if columns:
                    for col in columns:
                        print(f"  - {col[0]}: {col[1]} (Nullable: {col[2]})")
                else:
                    print(f"{Colors.RED}  No columns found in enforcement_actions table{Colors.ENDC}")
                
                # Sample a few enforcement records to see actual data
                self.cur.execute("""
                    SELECT recall_number, status, classification, firm_name, event_id
                    FROM device.enforcement_actions
                    LIMIT 5
                """)
                samples = self.cur.fetchall()
                if samples:
                    print("\nSample Enforcement Records:")
                    for i, sample in enumerate(samples):
                        print(f"  Record {i+1}: recall_number={sample[0]}, status={sample[1]}, classification={sample[2]}, event_id={sample[4]}")
                else:
                    print(f"{Colors.RED}  No sample records found in enforcement_actions table{Colors.ENDC}")
                
            except Exception as e:
                print(f"{Colors.RED}  Error examining enforcement_actions table: {str(e)}{Colors.ENDC}")
            
            return True
        except Exception as e:
            print(f"{Colors.RED}Database diagnostics failed: {str(e)}{Colors.ENDC}")
            return False
    
    def validate_data_type(self, data_type: str, sample_size: int = 10, sample_ratio: float = 0.01):
        """Validate consistency between JSON and database records for a specific data type"""
        print(f"\n{Colors.HEADER}Validating {data_type} data...{Colors.ENDC}")
        
        # Sample JSON records
        json_records = self.sample_json_records(data_type, sample_size, sample_ratio)
        
        if not json_records:
            print(f"{Colors.YELLOW}No {data_type} records to validate{Colors.ENDC}")
            return
        
        # Check the table exists and has records
        table_name = self.get_primary_key_for_data_type(data_type)[1]
        self.cur.execute(f"SELECT COUNT(*) FROM device.{table_name}")
        db_count = self.cur.fetchone()[0]
        if db_count == 0:
            print(f"{Colors.RED}WARNING: {table_name} table exists but contains no records!{Colors.ENDC}")
            # Continue validation anyway to document the mismatches
        
        # Debug: Print first few records to understand JSON structure
        if len(json_records) > 0:
            print(f"\nJSON record key field debug for {data_type}:")
            for i, record in enumerate(json_records[:3]):
                key_value = self.get_key_field_for_json_record(data_type, record)
                print(f"  Record {i+1} key field: {key_value}")
                if data_type == 'enforcement':
                    # Print additional debug info for enforcement records
                    print(f"    Fields: recall_number={record.get('recall_number')}, event_id={record.get('event_id')}")
        
        # Compare JSON records with database records
        for json_record in tqdm(json_records, desc=f"Validating {data_type} records"):
            key_value = self.get_key_field_for_json_record(data_type, json_record)
            if not key_value:
                print(f"{Colors.YELLOW}Missing key field in JSON record: {json_record.get('_source_file', 'unknown file')}{Colors.ENDC}")
                continue
            
            db_record = self.get_db_record(data_type, key_value)
            
            if db_record:
                is_match, differences = self.compare_json_with_db(data_type, json_record, db_record)
                
                if is_match:
                    self.results[data_type]['matched'] += 1
                else:
                    self.results[data_type]['mismatched'] += 1
                    self.results[data_type]['details'].append({
                        'key_value': key_value,
                        'source_file': json_record.get('_source_file', 'unknown'),
                        'differences': differences
                    })
            else:
                self.results[data_type]['mismatched'] += 1
                self.results[data_type]['details'].append({
                    'key_value': key_value,
                    'source_file': json_record.get('_source_file', 'unknown'),
                    'differences': [{'field': 'record', 'json_value': 'exists', 'db_value': 'not found'}]
                })
        
        # Summary for this data type
        matched = self.results[data_type]['matched']
        sampled = self.results[data_type]['sampled']
        match_percentage = (matched / sampled) * 100 if sampled > 0 else 0
        
        print(f"\n{Colors.BLUE}Summary for {data_type}:{Colors.ENDC}")
        print(f"  Total records: {self.results[data_type]['total']}")
        print(f"  Sampled records: {sampled}")
        print(f"  Matched records: {matched} ({match_percentage:.2f}%)")
        print(f"  Mismatched records: {self.results[data_type]['mismatched']}")
        
        if self.results[data_type]['mismatched'] > 0:
            print(f"\n{Colors.YELLOW}First 5 mismatches for {data_type}:{Colors.ENDC}")
            for i, mismatch in enumerate(self.results[data_type]['details'][:5]):
                print(f"  Mismatch {i+1}: Key={mismatch['key_value']}, File={mismatch['source_file']}")
                for diff in mismatch['differences']:
                    print(f"    Field: {diff['field']}")
                    print(f"      JSON: {diff['json_value']}")
                    print(f"      DB:   {diff['db_value']}")
    
    def generate_report(self, output_file: str = None):
        """Generate a comprehensive report of the validation results"""
        print(f"\n{Colors.HEADER}Generating validation report...{Colors.ENDC}")
        
        # Calculate overall statistics
        total_records = sum(self.results[dt]['total'] for dt in self.results)
        total_sampled = sum(self.results[dt]['sampled'] for dt in self.results)
        total_matched = sum(self.results[dt]['matched'] for dt in self.results)
        total_mismatched = sum(self.results[dt]['mismatched'] for dt in self.results)
        overall_match_percentage = (total_matched / total_sampled) * 100 if total_sampled > 0 else 0
        
        # Create report dataframe
        report_data = []
        for data_type in self.results:
            sampled = self.results[data_type]['sampled']
            matched = self.results[data_type]['matched']
            match_percentage = (matched / sampled) * 100 if sampled > 0 else 0
            
            report_data.append({
                'Data Type': data_type.replace('_', ' ').title(),
                'Total Records': self.results[data_type]['total'],
                'Sampled Records': sampled,
                'Matched Records': matched,
                'Mismatched Records': self.results[data_type]['mismatched'],
                'Match Percentage': f"{match_percentage:.2f}%"
            })
        
        report_df = pd.DataFrame(report_data)
        
        # Add a summary row
        summary_row = pd.DataFrame([{
            'Data Type': 'OVERALL',
            'Total Records': total_records,
            'Sampled Records': total_sampled,
            'Matched Records': total_matched,
            'Mismatched Records': total_mismatched,
            'Match Percentage': f"{overall_match_percentage:.2f}%"
        }])
        
        report_df = pd.concat([report_df, summary_row], ignore_index=True)
        
        # Print report
        print(f"\n{Colors.HEADER}Data Consistency Validation Report{Colors.ENDC}")
        print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nSummary Statistics:")
        print(report_df.to_string(index=False))
        
        # Generate detailed mismatch report
        mismatch_data = []
        for data_type in self.results:
            for mismatch in self.results[data_type]['details']:
                for diff in mismatch['differences']:
                    mismatch_data.append({
                        'Data Type': data_type.replace('_', ' ').title(),
                        'Key Value': mismatch['key_value'],
                        'Source File': mismatch['source_file'],
                        'Field': diff.get('field', ''),
                        'JSON Value': str(diff.get('json_value', '')),
                        'DB Value': str(diff.get('db_value', ''))
                    })
        
        if mismatch_data:
            mismatch_df = pd.DataFrame(mismatch_data)
            print(f"\n{Colors.YELLOW}Mismatches Details (First 20):{Colors.ENDC}")
            print(mismatch_df.head(20).to_string(index=False))
        
        # Save to file if requested
        if output_file:
            try:
                # Create report directory if it doesn't exist
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                # Write summary report
                with open(output_file, 'w') as f:
                    f.write("Data Consistency Validation Report\n")
                    f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    f.write("Summary Statistics:\n")
                    f.write(report_df.to_string(index=False))
                    f.write("\n\n")
                    
                    if mismatch_data:
                        f.write("Mismatches Details:\n")
                        f.write(pd.DataFrame(mismatch_data).to_string(index=False))
                
                print(f"{Colors.GREEN}Report saved to {output_file}{Colors.ENDC}")
                
                # Also save as Excel if pandas has Excel support
                try:
                    excel_file = output_file.replace('.txt', '.xlsx')
                    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                        report_df.to_excel(writer, sheet_name='Summary', index=False)
                        if mismatch_data:
                            pd.DataFrame(mismatch_data).to_excel(writer, sheet_name='Mismatches', index=False)
                    print(f"{Colors.GREEN}Excel report saved to {excel_file}{Colors.ENDC}")
                except:
                    pass  # Excel export is optional
                
            except Exception as e:
                print(f"{Colors.RED}Failed to save report to {output_file}: {str(e)}{Colors.ENDC}")
        
        return report_df, mismatch_data if mismatch_data else None
    
    def run_validation(self, sample_size: int = 10, sample_ratio: float = 0.01, output_file: str = None):
        """Run the full validation process for all data types"""
        print(f"{Colors.HEADER}Starting JSON to PostgreSQL Data Consistency Validation{Colors.ENDC}")
        start_time = datetime.now()
        
        if not self.connect_to_db():
            return None
        
        try:
            # Run database diagnostics first
            self.perform_database_diagnostics()
            
            # Validate each data type
            for data_type in self.results:
                self.validate_data_type(data_type, sample_size, sample_ratio)
            
            # Generate final report
            report = self.generate_report(output_file)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            print(f"\n{Colors.GREEN}Validation completed in {duration:.2f} seconds{Colors.ENDC}")
            
            return report
        finally:
            self.close_db_connection()

def main():
    """Main function to run the validation"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate consistency between JSON files and PostgreSQL database')
    parser.add_argument('--sample-size', type=int, default=10, help='Minimum number of records to sample from each data type')
    parser.add_argument('--sample-ratio', type=float, default=0.01, help='Percentage of records to sample (0.01 = 1%)')
    parser.add_argument('--output', type=str, default='./validation_report.txt', help='Output file for the validation report')
    parser.add_argument('--host', type=str, default=DB_CONFIG['host'], help='Database host')
    parser.add_argument('--port', type=int, default=DB_CONFIG['port'], help='Database port')
    parser.add_argument('--dbname', type=str, default=DB_CONFIG['dbname'], help='Database name')
    parser.add_argument('--user', type=str, default=DB_CONFIG['user'], help='Database user')
    parser.add_argument('--password', type=str, default=DB_CONFIG['password'], help='Database password')
    parser.add_argument('--data-dir', type=str, default=BASE_DIR, help='Base directory for JSON files')
    parser.add_argument('--type', type=str, default=None, help='Only validate specific data type (classification, enforcement, recall, adverse_event, udi)')
    
    args = parser.parse_args()
    
    # Update DB config with command-line arguments
    db_config = {
        'host': args.host,
        'port': args.port,
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password
    }
    
    # Update data directories with command-line argument
    data_dirs = {
        'classification_dir': os.path.join(args.data_dir, 'classification'),
        'enforcement_dir': os.path.join(args.data_dir, 'enforcement'),
        'event_dir': os.path.join(args.data_dir, 'event'),
        'recall_dir': os.path.join(args.data_dir, 'recall'),
        'udi_dir': os.path.join(args.data_dir, 'udi'),
    }
    
    validator = JsonPgsqlValidator(db_config, data_dirs)
    
    if args.type and args.type in validator.results:
        # Validate only the specified data type
        if not validator.connect_to_db():
            return
        
        try:
            validator.perform_database_diagnostics()
            validator.validate_data_type(args.type, args.sample_size, args.sample_ratio)
            validator.generate_report(args.output)
        finally:
            validator.close_db_connection()
    else:
        # Validate all data types
        validator.run_validation(args.sample_size, args.sample_ratio, args.output)

if __name__ == "__main__":
    main()