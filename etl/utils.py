from functools import wraps
from chardet.universaldetector import UniversalDetector
import pandas as pd

from configparser import ConfigParser
import os


def dir_check(path: str) -> None:
    """Create ::path if not exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created : {path}")  # logging


def property_file_readability_check(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        print("Wrapper")
        property_file = "conf/project.properties"
        if os.path.exists(property_file):
            try:
                with open(
                    property_file, mode="r", encoding=detect_encoding(property_file)
                ) as f:
                    # Looks like you can fetch data from a property file only if you read it once.
                    self.project_properties.read_file(f)
            except Exception as e:
                raise Exception(
                    f"Unable to read the property file: {property_file}"
                ) from e
            finally:
                f.close()
        else:
            raise FileNotFoundError(f"Property file not found: {property_file}")
        result = func(self, *args, **kwargs)
        return result

    return wrapper


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
        raise Exception(
            f"Not all formats are valid,\nValid formats are : {_loader_format}"
        )
    if not all(kind in _loader_kind for kind in kinds):
        raise Exception(f"Not all kinds are valid,\nValid kinds are : {_loader_kind}")


def detect_encoding(file_path):
    """Predicts the encodig of the file using `chardet` module"""
    detector = UniversalDetector()
    with open(file_path, "rb") as file:
        for line in file:
            detector.feed(line)
            if detector.done:
                break
    detector.close()
    return detector.result["encoding"]


def source_meta_accessability_check(file: str, path: str):
    file = os.path.join(path, file)

    if not os.path.exists(file):
        raise FileNotFoundError(f"The given file ({file}) is not found")
    try:
        f = open(file, mode="r", encoding=detect_encoding(file))
        if not f.readable():
            raise Exception(f"{file} is not readable")
    except Exception as e:
        raise Exception(f"Unable to read the file: {file}") from e
    finally:
        f.close()
