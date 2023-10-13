from datetime import datetime
import sqlite3 as sq
import os


class EtlDataBase:
    def __init__(self, db_folder, schema, table_name, db_type="sqlite"):
        self.db_folder = db_folder
        self.schema = schema
        self.table_name = table_name
        self.db_type = db_type
        self.cursor = self._getCursor()

    def _getCursor(self) -> sq.Cursor:
        db_path = os.path.abspath(os.path.join(self.db_folder, f"{self.schema}.db"))
        db_dir = os.path.dirname(db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        connection = sq.connect(db_path, isolation_level=None)

        return connection.cursor()

    def create_source_meta_table(self, table_columns):
        self._drop_table()
        self._create_table(table_columns)
        self._add_rows(table_columns)

    def _drop_table(self):
        DROP_TABLE = f"DROP TABLE IF EXISTS {self.table_name}"
        try:
            self.cursor.execute(DROP_TABLE)
            print(f"Table {self.table_name} dropped.")
        except Exception as e:
            print(f"Error dropping table: {self.table_name}")
            print(str(e))

    def _create_table(self, table_columns):
        columns = ", ".join([f"{col} TEXT" for col in table_columns[0].split(",")])
        CREATE_TABLE = (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns}, datetime TEXT)"
        )
        print(f"Creating table with query: {CREATE_TABLE}")
        try:
            self.cursor.execute(CREATE_TABLE)
            print(f"Table {self.table_name} created.")
        except Exception as e:
            print(f"Error creating table: {self.table_name}")
            print(str(e))

    def _add_rows(self, table_rows):
        value_place_holder = ("?," * (len(table_rows[0].split(",")) + 1))[:-1]
        INSERT_INTO = f"INSERT INTO {self.table_name} VALUES ({value_place_holder})"
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%f")
        data = [tuple(item.split(",") + [current_timestamp]) for item in table_rows[1:]]
        try:
            self.cursor.executemany(INSERT_INTO, data)
            print(f"Inserted {len(data)} rows into {self.table_name}.")
        except Exception as e:
            print(f"Error inserting into {self.table_name}")
            print(f"Query: {INSERT_INTO}")
            print(f"Exception: {e}")

    def _getTables(self):
        available_tables = []
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        self.cursor.execute(query)
        tables = self.cursor.fetchall()
        available_tables = [table[0] for table in tables]
        print(f"Available tables: {available_tables}")
        return available_tables
