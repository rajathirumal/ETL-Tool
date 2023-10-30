from abc import ABC, abstractclassmethod


class ETL(ABC):
    """The class implimenting this should have

    1. function called `_sanity_check` wich should deal with the accessability of the file.
    """

    @abstractclassmethod
    def _sanity_check(self) -> None:
        pass
