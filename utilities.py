import json
from ydata_profiling import ProfileReport
import pandas as pd
from pathlib import Path
import logging
import sys


logger = logging.getLogger(__name__)


def data_checking(func):
    """
    Decorator for handling dictionary data errors.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as error:
            logger.error(f"Error in specification file. {error}")
        except AttributeError as error:
            logger.error(f"Check column's data type. {error}")
    return wrapper


def abs_path(func):
    """
    Decorator takes a file name and replaces it
    with an absolute path to this file.
    """
    def wrapper(*args, **kwargs):
        filename = kwargs.get("filename")
        kwargs["filename"] = Path(__file__).resolve().parent / filename
        return func(*args, **kwargs)
    return wrapper


def check_path(func):
    """
    Decorator for handling errors in case
    of non-existent files and directories.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as error:
            logger.error(error)
            sys.exit(1)
        except OSError as error:
            logger.error(error)
    return wrapper



class FileHandler:
    """
    The class contains file handling methods.
    """

    @staticmethod
    @check_path
    @abs_path
    def read_csv(filename: str, sep: str = ",") -> pd.DataFrame:
        """
        Method returns data as pandas data frame from a given CSV file.
        """
        return pd.read_csv(filename, sep=sep)


    @staticmethod
    @check_path
    @abs_path
    def read_json(filename: str) -> dict:
        """
        Method returns data as dict from a given JSON file.
        """
        with open(filename, mode="r", encoding="utf-8") as f:
                return json.load(f)


    @staticmethod
    @check_path
    @abs_path
    def write_csv(data: pd.DataFrame, filename: str, sep: str = ",", index: bool = False) -> None:
        """
        Method saves pandas data frame to a given CSV file.
        """
        data.to_csv(filename, sep=sep, index=index)
        logger.info(f"Data set has been successfully saved to {filename}.")


    @staticmethod
    @check_path
    @abs_path
    def create_profile(report: ProfileReport, filename: str) -> None:
        """
        Method creates html profile from a given profile report.
        """
        report.to_file(filename)
        logger.info(f"Report file'{filename}' created.")
