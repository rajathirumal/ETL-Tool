import configparser
import os

from .etl_db import ConfigLoaderDbOps
from . import utils


class ConfigLoader:
    """Class for initialising the ETL by loading the source_meta.csv

    Use the function `load_meta()` to start the loading process
    """

    def __init__(self):
        self.project_properties = configparser.ConfigParser()
        self._sanity_check()
        self.db = ConfigLoaderDbOps(
            db_folder=self.project_properties.get("project", "db.sqlite.folder"),
            schema="landing",
            db_type="sqlite",  # This is defaulted now
            table_name=self.project_properties.get(
                "landing", "landing.db.source.meta.table.name"
            ),
        )

    def _sanity_check(self) -> None:
        utils.property_file_readability_check(
            project_properties=self.project_properties
        )

        schema_folder = self.project_properties.get("project", "db.sqlite.folder")
        utils.dir_check(schema_folder)

    def _read_source_meta_config(self):
        load_config_file = os.path.join(
            self.project_properties.get("landing", "landing.db.source.meta.file.path"),
            self.project_properties.get("landing", "landing.db.source.meta.name"),
        )

        with open(load_config_file) as lc:
            return [line.strip() for line in lc]

    def load_meta(self):
        """load your `source_meta.csv` file data to the landing.db"""
        config_content = self._read_source_meta_config()
        utils.config_data_scanity_check(file_data=config_content)
        self.db.create_source_meta_table(table_columns=config_content)


if __name__ == "__main__":
    loader = ConfigLoader()
    loader.load_meta()
