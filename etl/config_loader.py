import configparser
import os

from .etl_db import EtlDataBase
from . import utils


class ConfigLoader:
    """Class for initialising the ETL by loading the source_meta.csv

    Use the function `load_meta()` to start the loading process
    """

    def __init__(self):
        self.project_properties = configparser.ConfigParser()
        self._sanity_check()
        self.db = EtlDataBase(
            db_folder=self.project_properties.get("project", "db.sqlite.folder"),
            schema="landing",
            db_type="sqlite",  # This is defaulted now
            table_name=self.project_properties.get(
                "landing", "landing.db.source.meta.table.name"
            ),
        )

    def _sanity_check(self):
        self._property_file_readability_check()
        self._dir_check()
        # configparser.

    def _dir_check(self):
        # schema_folder = self.project_properties.get("schema", "project.schema.folder")
        schema_folder = self.project_properties.get("project", "db.sqlite.folder")
        utils.dir_check(schema_folder)

    def _property_file_readability_check(self):
        property_file = "conf/project.properties"
        if os.path.exists(property_file):
            try:
                with open(property_file) as f:
                    self.project_properties.read_file(f)
            except Exception as e:
                raise Exception(
                    f"Unable to read the property file: {property_file}"
                ) from e
        else:
            raise FileNotFoundError(f"Property file not found: {property_file}")

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
        self.db.create_source_meta_table(table_columns=config_content)


if __name__ == "__main__":
    loader = ConfigLoader()
    loader.load_meta()
