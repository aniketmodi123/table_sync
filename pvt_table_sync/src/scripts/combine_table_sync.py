import json
from config import get_set_db_data, SessionLocal
from utils import logs
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from models import TariffConfig

def update_or_insert_data(shifting_data, table_name, model_name, column_mapping, new_table_main_column, errors):
    try:
        db: Session = SessionLocal()

        if not shifting_data:
            logs("No data fetched from the source table.", type="warning")
            return False

        # Fetch all existing records in one query
        existing_ids = [record[new_table_main_column] for record in shifting_data if new_table_main_column in record]
        if not existing_ids:
            logs("No existing record IDs found in the fetched data.", type="warning")
            return False

        existing_records = db.query(model_name).filter(
            getattr(model_name, new_table_main_column).in_(existing_ids)
        ).all()

        # Create a dictionary for quick lookup
        existing_records_dict = {getattr(record, new_table_main_column): record for record in existing_records}

        # Prepare lists for bulk updates and inserts
        to_update = []
        to_insert = []

        for record in shifting_data:
            record_id = record.get(new_table_main_column)
            try:

                if record_id in existing_records_dict:
                    # Update existing record only if there are changes
                    existing_record = existing_records_dict[record_id]
                    needs_update = False
                    for src_col, target_col in column_mapping.items():
                        if src_col in record and getattr(existing_record, target_col) != record[src_col]:
                            setattr(existing_record, target_col, record[src_col])
                            needs_update = True
                    if needs_update:
                        to_update.append(existing_record)
                        logs(f"Updated record with {new_table_main_column} {record_id}", type="info")
                else:
                    # Create new record if ID does not already exist
                    new_record_data = {target_col: record[src_col] for src_col, target_col in column_mapping.items() if src_col in record}
                    new_record = model_name(**new_record_data)
                    to_insert.append(new_record)
                    logs(f"Inserted new record with {new_table_main_column} {record_id}", type="info")
            except Exception as e:
                errors.append(f"Unexpected error: {str(e)}")
                logs(f"Unexpected error: {str(e)}", type="error")
        try:
            if to_update:
                db.bulk_save_objects(to_update)  # Bulk update existing records
        except Exception as e:
            errors.append(f"Unexpected error during bulk update: {str(e)}")
            logs(f"Unexpected error during bulk update: {str(e)}", type="error")
        try:
            if to_insert:
                db.bulk_save_objects(to_insert)  # Bulk insert new records
        except Exception as e:
            errors.append(f"Unexpected error during bulk update: {str(e)}")
            logs(f"Unexpected error during bulk update: {str(e)}", type="error")

        db.commit()  # Commit the transaction
        logs(f"Database sync complete for {table_name}.", type="success")
        return True
    except SQLAlchemyError as e:
        db.rollback()  # Rollback the session on error
        errors.append(f"Error syncing data from {table_name}: {str(e)}")
        logs(f"Error syncing data from {table_name}: {str(e)}", type="error")
        return False
    except Exception as e:
        db.rollback()  # Rollback the session on any other error
        errors.append(f"Unexpected error: {str(e)}")
        logs(f"Unexpected error: {str(e)}", type="error")
        return False
    finally:
        db.close()  # session is closed

"""
    this code can sync the data from more than one table and save it in the another table
    * the first table must be the table which all rows will be imported
    * the second table be the supporting table which will give the othere remaining data values to first table
    * merging_on is used to merge the data between two tables
    *
"""
def combine_table_and_sync():
    sync_table = [
        {
            "tables": [
                {
                    "db_name": "db_office.",
                    "table_name": "tbl_site_initialization",
                    "select_columns": ["site_id", "meter_ip", "status", "timestamp"],
                    "primary_column": "meter_ip", #this column is main column which can't be duplicat
                    "merging_on": "meter_ip"
                },
                {

                    "db_name": "db_office.",
                    "table_name": "tbl_backup_dcu_info",
                    "select_columns": ["meter_address", "dg_price", "eb_price", "dg_full_tariff", "eb_full_tariff"],
                    "primary_column": "meter_address",
                    "merging_on": "meter_address"
                }
            ],
            "new_table_main_column": "meter_ip",  # Specify the common column explicitly
            "model_name": TariffConfig,
            "table_name": "tariff_config",
            "column_mapping": {  # Map source columns to target model columns (source column name: target column name)
                "site_id": "site_id",
                "meter_ip": "meter_ip",
                "status": "status",
                "eb_price": "eb_price",
                "dg_price": "dg_price",
                "eb_full_tariff": "eb_full_tariff",
                "dg_full_tariff": "dg_full_tariff",
                "timestamp": "timestamp"
            },
        }
    ]

    errors = []  # List to accumulate errors
    for sync_item in sync_table:
        try:
            logs(f"Syncing for {sync_item['model_name']} is starting")

            # Initialize a dictionary to hold merged data
            merged_data = []
            merging_column = sync_item["tables"][0]['merging_on']

            # Fetch data from each source table
            for count, table in enumerate(sync_item["tables"], start=1):
                columns = ", ".join(table["select_columns"])
                query = f"""
                    SELECT
                        {columns}
                    FROM {table['db_name']}{table['table_name']}
                    """
                    # where {table['primary_column']} = '5.0.134.6'
                shifting_data = get_set_db_data(query, get='all')

                if not shifting_data:
                    logs(f"No data fetched from {table['table_name']}.", type="warning")
                    continue

                # Deduplicate records by the common column
                deduplicated_data = {record[table["primary_column"]]: record for record in shifting_data if table["primary_column"] in record}
                shifting_data = list(deduplicated_data.values())  # Convert back to list

                if not shifting_data:
                    logs("No unique record IDs found in the fetched data.", type="warning")
                    continue

                if count == 1:
                    merged_data = shifting_data
                else:
                    df1 = pd.DataFrame(merged_data)
                    df2 = pd.DataFrame(shifting_data)

                    if merging_column not in df1.columns:
                        logs(f"The {table['merging_on']} column is missing in table.", type="error")
                        continue
                    if table['merging_on'] not in df2.columns:
                        logs(f"The {table['merging_on']} column is missing in table.", type="error")
                        continue
                    merged_data = pd.merge(df1, df2, left_on=merging_column, right_on= table['merging_on'], how='left')
                    merged_data = merged_data.drop(columns=table['merging_on'])
            if merged_data is not None and not isinstance(merged_data, list):
            # Convert DataFrame to list of dictionaries
                merged_data = merged_data.to_dict(orient='records')

            print(json.dumps(merged_data,indent=4))

            update_or_insert_data(merged_data,sync_item['table_name'], sync_item['model_name'], sync_item['column_mapping'], sync_item['new_table_main_column'], errors)

        except Exception as e:
            logs(f"Error syncing {sync_item['model_name']}: {str(e)}", type="error")
            errors.append(f"Error syncing {sync_item['model_name']}: {str(e)}")

    # Log all accumulated errors after processing all tables
    if errors:
        logs("Errors encountered during sync process:", type="error")
        for error in errors:
            logs(error, type="error")

    return not errors  # Return True if no errors were encountered

# Example usage
if __name__ == "__main__":
    combine_table_and_sync()
