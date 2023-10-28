import csv
from dataclasses import dataclass
import os

from .etl_abc import ETL
from etl import utils


@dataclass
class CSVReader(ETL):
    file_name: str
    path: str

    def __post_init__(self):
        self.file_name = f"{self.file_name}.csv"
        self._sanity_check()

    def _sanity_check(self):
        utils.source_meta_accessability_check(file=self.file_name, path=self.path)

    def get_column_info(self) -> dict:
        """
        : return --> {
            'content': list[list],

            'types': ['column 1':'dataype', 'column 2':'dataype', ... ]

            }
        """

        def _map_data_type_tosqlite(value) -> str:
            """Mapps Python datatypes to the equivalent SQLite types.

            ### :return
                - NULL: Represents a missing or unknown value.
                - REAL: Represents floating-point values (real numbers).
                - TEXT: Represents text or string values.
                - BLOB: Represents binary large objects, typically used for storing binary data like images, audio, or other non-textual data.

            ### :rtype
                - string

            ### :default
                - TEXT

            ### Note:
                - For numbers we eliminate INTEGER as a type. Insted we use REAL.
                - For boolean values we use INTEGRE as type. Use `1` for `True` and `0` for `False`
                - Currently we do not support BLOB
            """

            if value == "":
                return "NULL"

            try:
                int_value = int(value)
                return "REAL"
            except ValueError:
                pass

            try:
                float_value = float(value)
                return "REAL"
            except ValueError:
                pass

            if value.lower() == "true" or value.lower() == "false":
                bool_value = value.lower() == "true"
                return "INTEGER"

            return "TEXT"

        def _determine_type(col_data: list):
            """Find all the unique types on the given list, if only one type available return it else `TEXT`"""
            dtype: set[str] = set(map(_map_data_type_tosqlite, col_data))
            return dtype.pop() if len(dtype) == 1 else "TEXT"

        _prediction = {}
        print(f"Determining colmun data type with the following data: {self}")
        _file = os.path.join(self.path, self.file_name)
        _file_data: list[str]
        with open(
            _file, mode="r", encoding=utils.detect_encoding(file_path=_file)
        ) as f:
            reader = csv.reader(f, delimiter=",")
            file_content = list(reader)
            transpose_content = list(zip(*file_content))
        for col in transpose_content:
            _prediction[col[0]] = _determine_type(col[1:])
        _prediction["content"] = file_content

        print(_prediction)


@dataclass
class XMLReader(ETL):
    file_name: str
    path: str

    def __post_init__(self):
        ...

    def _sanity_check(self):
        ...

    def get_column_info(self) -> dict:
        """
        : return --> {
            'content': list[list],

            'types': ['column 1':'dataype', 'column 2':'dataype', ... ]

            }
        """
        ...


@dataclass
class JsonReader(ETL):
    file_name: str
    path: str

    def __post_init__(self):
        ...

    def _sanity_check(self):
        ...

    def get_column_info(self) -> dict:
        """
        : return --> {
            'content': list[list],

            'types': ['column 1':'dataype', 'column 2':'dataype', ... ]

            }
        """
        ...
