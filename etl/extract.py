import configparser
import os

from . import utils
from .etl_db import LandingDb


class Extract:
    """This class helops in handelling the logics of loading the data as per the configurations given to the config loader

    Exports the following functions,

    1. load_landing (Self@Extract) -> None
    """

    def __init__(self) -> None:
        self.project_properties = configparser.ConfigParser()
        self._sanity_check()
        print(
            self.project_properties.get("landing", "landing.db.source.meta.table.name")
        )
        self.db = LandingDb(
            schema="landing",
            db_folder=self.project_properties.get("project", "db.sqlite.folder"),
        )

    def _sanity_check(self):
        utils.property_file_readability_check(
            project_properties=self.project_properties
        )

    def load_landing(self):
        table_content: list[list] = self.db.read_table(
            table_name=self.project_properties.get(
                "landing", "landing.db.source.meta.table.name"
            )
        )

        print(table_content)
        for row in table_content:
            self.db.create_table(table_name=row[3])
            self._read_file()

    def _config_data_scanity_check(self):
        ...

    def _read_file(self):
        ...
