import logging
import sys
import os
from dataclasses import dataclass, field
import utils


@dataclass
class EtlLogger:
    log_file_name: str = field(init=True)

    def __post__inti__(self):
        utils.dir_check(path="../log")

    def configure_logging(self):
        if self.log_file_name is None:
            log_file_name_full = (sys.argv[0]).split("/")
            self.log_file_name = (log_file_name_full[-1]).split(".")[-2]

        log_file = f"../log/{self.log_file_name}.log"

        log_folder_name = os.path.dirname(log_file)

        if not os.path.exists(log_folder_name):
            os.makedirs(log_folder_name)

        logging.basicConfig(
            level=logging.DEBUG,
            filename=log_file,
            filemode="a",
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    def info(self, str):
        logging.info(str)

    def error(self, str):
        logging.info(str)

    def debug(self, str):
        logging.info(str)

    def warn(self, str):
        logging.info(str)
