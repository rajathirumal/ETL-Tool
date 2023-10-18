from configparser import ConfigParser
import os

import pandas as pd


def dir_check(path: str) -> None:
    """Create ::path if not exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created : {path}")


def property_file_readability_check(project_properties: ConfigParser) -> None:
    """Check if you are able to read data from property file"""
    property_file = "conf/project.properties"
    if os.path.exists(property_file):
        try:
            with open(property_file) as f:
                # Looks like you can fetch data form property file only if you read it once.
                project_properties.read_file(f)
        except Exception as e:
            raise Exception(f"Unable to read the property file: {property_file}") from e
    else:
        raise FileNotFoundError(f"Property file not found: {property_file}")


def config_data_scanity_check(file_data: list[str]):
    _loader_format = ["csv", "db", "json", "xml"]
    _loader_kind = ["null", "meta"]
    # Split the data and create a DataFrame
    df = pd.DataFrame([row.split(",") for row in file_data])

    # If you want to exclude the header, you can use iloc
    # formats = df[1].iloc[1:].tolist()
    formats = [format.lower() for format in df[1].iloc[1:].tolist()]
    kinds = [kind.lower() for kind in df[2].iloc[1:].tolist()]
    if not all(format in _loader_format for format in formats):
        print("Not all formats are valid")
        raise Exception(
            f"Not all formats are valid,\nValid formats are : {_loader_format}"
        )
    if not all(kind in _loader_kind for kind in kinds):
        print("Not all kinds are valid")
        raise Exception(f"Not all kinds are valid,\nValid kinds are : {_loader_kind}")

    # Print the result
    print(formats)
    print(kinds)
