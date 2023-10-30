import configparser


from .etl_abc import ETL
from . import utils
from .etl_db import LandingDb


class Extract(ETL):
    """This class helops in handelling the logics of loading the data as per the configurations given to the config loader

    Exports the following functions,

    1. load_landing (Self@Extract) -> None
    """

    def __init__(self) -> None:
        self.project_properties = configparser.ConfigParser()
        self._sanity_check()
        # print(
        #     self.project_properties.get("landing", "landing.db.source.meta.table.name")
        # )
        self.db = LandingDb(
            schema="landing",
            db_folder=self.project_properties.get("project", "db.sqlite.folder"),
        )

    @utils.property_file_readability_check
    def _sanity_check(self):
        ...

    def load_landing(self):
        """Creates a landing tabled for every entry mentioned in the `LANDING_META_CONFIG` table

        if you'r file name is `sample.csv` this function creates a table in the landing schema with the name 'SAMPLE' and loads the data from `sample.csv` to thus created table.

        TODO: Data type determination for columns
        """

        table_content: list[list] = self.db.read_table(
            table_name=self.project_properties.get(
                "landing", "landing.db.source.meta.table.name"
            )
        )

        # print(table_content)
        for row in table_content:
            """A row is a record in the landing meta config table"""
            self.db.create_table(row=row)
            self._read_file()

    def _config_data_scanity_check(self):
        ...

    def _read_file(self):
        ...
