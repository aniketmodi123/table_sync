from config import get_set_db_data, SessionLocal
from utils import logs
from models import TowerConfig
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

def update_or_insert_data(shifting_data, table_name, model_name, column_mapping, common_column, errors):
    try:
        db: Session = SessionLocal()

        if not shifting_data:
            logs("No data fetched from the source table.", type="warning")
            return False

        # Fetch all existing records in one query
        existing_ids = [record[common_column] for record in shifting_data if common_column in record]
        if not existing_ids:
            logs("No existing record IDs found in the fetched data.", type="warning")
            return False

        existing_records = db.query(model_name).filter(
            getattr(model_name, common_column).in_(existing_ids)
        ).all()

        # Create a dictionary for quick lookup
        existing_records_dict = {getattr(record, common_column): record for record in existing_records}

        # Prepare lists for bulk updates and inserts
        to_update = []
        to_insert = []

        for record in shifting_data:
            record_id = record.get(common_column)
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
                    logs(f"Updated record with {common_column} {record_id}", type="info")
            else:
                # Create new record if ID does not already exist
                new_record_data = {target_col: record[src_col] for src_col, target_col in column_mapping.items() if src_col in record}
                new_record = model_name(**new_record_data)
                to_insert.append(new_record)
                logs(f"Inserted new record with {common_column} {record_id}", type="info")

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
        db.close()  # session is closed

def selective_column_sync_table():
    sync_table = [
        {
            "db_name": "db_office.",
            "table_name": "re_developer_config",
            "model_name": TowerConfig,
            "select_columns": ["site_id", "site_name", "load_type", "nam", "email", "contact", "project", "gst_no", "pan_no","monthly_maintain", "monthly_maintain_gst", "other_charges", "other_gst_charge", "address", "dev_logo", "created_by", "edited_by"],  # Specify the source columns
            "column_mapping": {  # Map source columns to target model columns (source column name: target column name)
            # source column name : target column name
                "site_id" : "site_id",
                "site_name" : "site_name",
                "load_type" : "load_type",
                "nam" : "tower_name",
                "email" : "email",
                "https://gpsurvey.s3.amazonaws.com/realestate/Kamlesh_Jain_2024_08_2…contact" : "contact",
                "project" : "project",
                "gst_no" : "gst_no",
                "pan_no" : "pan_no",
                "address" : "address",
                "dev_logo" : "dev_logo",
                "created_by" : "created_by",
                "edited_by" : "edited_by",
                "monthly_maintain":"maintenance_charge",
                "monthly_maintain_gst": "maintenance_gst_charge",
                "other_charges":"other_charges",
                "other_gst_charge":"other_gst_charge"
            },
            "common_column": "site_id"  # Specify the common column explicitly
        }
    ]

    errors = []  # List to accumulate errors
    for table in sync_table:
        try:
            logs(f"{table['table_name']} syncing is starting")

            # Construct the SELECT query with specific columns
            columns = ", ".join(table["select_columns"])
            query = f"SELECT {columns} FROM {table['db_name']}{table['table_name']}"

            shifting_data = get_set_db_data(query, get='all')

            if not shifting_data:
                logs(f"No data fetched from {table['table_name']}.", type="warning")
                continue

            # print(json.dumps(shifting_data, indent=4))

            # Deduplicate records by the common column
            deduplicated_data = {record[table["common_column"]]: record for record in shifting_data if table["common_column"] in record}
            shifting_data = list(deduplicated_data.values())  # Convert back to list

            if not shifting_data:
                logs("No unique record IDs found in the fetched data.", type="warning")
                continue

            update_or_insert_data(shifting_data, table['table_name'], table['model_name'], table['column_mapping'], table['common_column'], errors)

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
    selective_column_sync_table()
