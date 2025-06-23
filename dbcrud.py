import pandas as pd
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import insert, delete, and_, func, cast, select, Numeric, Table, text, MetaData, desc
from sqlalchemy.exc import SQLAlchemyError
from models.kbe.kbe_models import KBEImportExport
from models.shiprocket.shiprocket_models import ShiprocketOrder

tables = {
    'kbe_import_export':KBEImportExport,
    "shiprocket_orders":ShiprocketOrder,
}


class DatabaseCrud:
    def __init__(self, db_connector) -> None:
        self.db_connector = db_connector
        self.db_engine = db_connector.engine
        self.Session = scoped_session(sessionmaker(bind=self.db_connector.engine, autoflush=False))


    def delete_date_range_query(self, table_name: str, start_date: str, end_date: str, commit: bool) -> None:
        """
        Deletes rows in the specified table within the given date range.

        Args:
            table_name (str): The name of the table from which rows are to be deleted.
            start_date (str): The start date of the date range in 'YYYY-MM-DD' format.
            end_date (str): The end date of the date range in 'YYYY-MM-DD' format.
            commit (bool): Whether to commit the transaction.

        Returns:
            None
        """
        table_class = tables.get(table_name)
        if not table_class:
            print(f"Table '{table_name}' not found in table mapping. Delete query failed to execute.")
            return

        if start_date > end_date:
            print(f"Start date '{start_date}' should be less than or equal to end date '{end_date}'.")
            return

        date_condition = table_class.date.between(start_date, end_date)
        delete_query = delete(table_class).where(date_condition)

        try:
            with self.db_engine.connect() as connection:
                transaction = connection.begin()
                try:
                    result = connection.execute(delete_query)
                    deleted_count = result.rowcount
                    print(f"Deleted {deleted_count} rows from '{table_name}' between {start_date} and {end_date}.")
                    
                    if commit:
                        transaction.commit()
                        print("Transaction committed.")
                    else:
                        transaction.rollback()
                        print("Transaction not committed.")
                except SQLAlchemyError as e:
                    transaction.rollback()
                    print(f"Error occurred during deletion: {e}")
        except SQLAlchemyError as e:
            print(f"Connection error: {e}")

    def get_row_count(self, table_name):
        table_class = tables.get(table_name)
        
        if not table_class:
            print(f"Table '{table_name}' not found in table_mapping.")
            return None

        with self.db_engine.connect() as connection:
            if isinstance(table_class, Table):  # If it's a Table object
                count_query = select([func.count()]).select_from(table_class)
                row_count = connection.execute(count_query).scalar()
            else:  # If it's an ORM class
                table_name = table_class.__tablename__
                row_count = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

        return row_count

    def import_data(self, table_name, df: pd.DataFrame, commit):
        if df is not None:
            row_count_before = self.get_row_count(table_name)
            row_count_after = None
            
            try:
                with self.db_engine.connect() as connection:
                    df.to_sql(table_name, self.db_connector.engine, if_exists='append', index=False, method='multi', chunksize=500)
                    row_count_after = self.get_row_count(table_name)
                    if commit:
                        connection.commit()
            except SQLAlchemyError as e:
                print(f"Error inserting data into {table_name}: {e}")
                if row_count_after is not None:
                    connection.rollback()
                    print(f"Rolling back changes in {table_name} due to import error.")
            except Exception as e:
                print(f"Unknown error occurred: {e}")

            if (row_count_before is not None) and (row_count_after is not None):
                rows_inserted = row_count_after - row_count_before
                print(f"Data imported into {table_name}. {rows_inserted} rows inserted.")
            else:
                print("Failed to determine rows inserted.")
        else:
            print(f"Empty Dataframe hence 0 rows imported in {table_name}")

    def truncate_table(self, table_name: str, commit: bool) -> None:
        """
        Truncate the specified database table.

        This method deletes all rows from the specified table using SQLAlchemy's delete operation.

        Args:
            table_name (str): The name of the table to truncate.
            commit (bool): If True, commit the transaction; otherwise, roll back.

        Returns:
            None
        """
        # Retrieve table class from metadata
        table_class = tables.get(table_name)
        if table_class:
            # Prepare truncate query
            truncate_query = delete(table_class)

            try:
                with self.db_engine.connect() as connection:
                    # Execute truncate query
                    connection.execute(truncate_query)
                    if commit:
                        connection.commit()
                        print(f"Table '{table_name}' truncated successfully.")
                    else:
                        connection.rollback()
                        print(f"Transaction rolled back for truncating '{table_name}'.")

            except SQLAlchemyError as e:
                print(f"Error truncating table '{table_name}': {e}")
                connection.rollback()
                print(f"Rolling back changes in '{table_name}' due to truncation error.")
            except Exception as e:
                print(f"Unknown error occurred while truncating '{table_name}': {e}")
                connection.rollback()
                print(f"Rolling back changes in '{table_name}' due to an unknown error.")
        else:
            print(f"Table '{table_name}' not found in table_mapping.")

    def delete_shiprocket_id_wise(self, shiprocket_id: list):
        try:
            queryset = ShiprocketOrder.objects.filter(shiprocket_id__in=shiprocket_id)
            deleted_count, _ = queryset.delete()
            return deleted_count
        except Exception as e:
            return 0
        


