from config import get_set_db_data, psql_cursor, SessionLocal
from apscheduler.schedulers.background import BlockingScheduler
from utils import logs

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError




"""
    this code is used to copy the data from one table to another table  this code either update the table or insert a new one into the table.
"""

def update_or_insert_data(household_data, table_name, model_name, errors):
    try:
        db: Session = SessionLocal()

        if not household_data:
            logs("No data fetched from the source table.", type="warning")
            return False

        # Fetch all existing records in one query
        existing_ids = [record['id'] for record in household_data if 'id' in record]
        if not existing_ids:
            logs("No existing record IDs found in the fetched data.", type="warning")
            return False

        existing_records = db.query(model_name).filter(
            model_name.id.in_(existing_ids)
        ).all()

        # Create a dictionary for quick lookup
        existing_records_dict = {record.id: record for record in existing_records}

        # Prepare lists for bulk updates and inserts
        to_update = []
        to_insert = []

        for record in household_data:
            record_id = record.get('id')
            if record_id in existing_records_dict:
                # Update existing record only if there are changes
                existing_record = existing_records_dict[record_id]
                needs_update = False
                for key, value in record.items():
                    if getattr(existing_record, key) != value:
                        setattr(existing_record, key, value)
                        needs_update = True
                if needs_update:
                    to_update.append(existing_record)
                    logs(f"Updated record with id {record_id}", type="info")
            else:
                # Create new record if ID does not already exist
                new_record = model_name(**record)
                to_insert.append(new_record)
                logs(f"Inserted new record with id {record_id}", type="info")

        # Perform bulk updates and inserts
        if to_update:
            db.bulk_save_objects(to_update)  # Bulk update existing records
        if to_insert:
            db.bulk_save_objects(to_insert)  # Bulk insert new records

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
        db.close()  # Ensure the session is closed

def sync_table():
    sync_table = [
        # {
        #     "db_name": "db_office.",
        #     "table_name": "microgrid_surveyhouseholdinfo",
        #     "model_name": MicrogridSurveyHouseholdInfo
        # },
        {
            "db_name": "db_ems.",
            "table_name": "user_meter_detail",
            "model_name": UserMeterDetail
        }
    ]

    errors = []  # List to accumulate errors
    for table in sync_table:
        try:
            logs(f"{table['table_name']} syncing is starting")
            query = f"SELECT * FROM {table['db_name']}{table['table_name']}"

            household_data = get_set_db_data(query, get='all')
            if not household_data:
                logs(f"No data fetched from {table['table_name']}.", type="warning")
                continue

            # Deduplicate records by ID
            deduplicated_data = {record['id']: record for record in household_data if 'id' in record}
            household_data = list(deduplicated_data.values())  # Convert back to list

            if not household_data:
                logs("No unique record IDs found in the fetched data.", type="warning")
                continue

            update_or_insert_data(household_data, table['table_name'], table['model_name'], errors)

        except Exception as e:
            logs(f"Error syncing {table['table_name']}: {str(e)}", type="error")
            errors.append(f"Error syncing {table['table_name']}: {str(e)}")

    # Log all accumulated errors after processing all tables
    if errors:
        logs("Errors encountered during sync process:", type="error")
        for error in errors:
            logs(error, type="error")

    return not errors  # Return True if no errors were encountered

# Example usage
if __name__ == "__main__":
    sync_table()
