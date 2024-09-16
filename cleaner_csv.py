import pandas as pd
from utilities import FileHandler, data_checking
from ydata_profiling import ProfileReport
import logging


logging.basicConfig(
    filename="cleaner.log",
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s (%(filename)s)\n%(name)s-%(levelname)s: %(message)s"
)


class CleanerCSV:
    def __init__(self, data: pd.DataFrame, specs: dict) -> None:
        """
        Initialization method gets cleaning parameters
        and instructions from JSON specification file,

        original data set from specified CSV file and saves
        its original version and a deep copy as pandas data frames.

        """
        self.specs = specs
        self.df_original = data
        self.df_copy = self.df_original.copy()

    
    def clean(self) -> None:
        """
        Main method for managing data cleaning methods
        based on specification.
        """
        if self.specs.get("drop_duplicates"):
            self._drop_duplicates()
            logging.info("Drop duplicates.")

        if self.specs.get("drop_na"):
            self._drop_na()
            logging.info("Drop NaN values-containing rows.")

        if drop_col := self.specs.get("drop_col"):
            self._drop_columns(cols=drop_col)
            logging.info("Drop specified columns.")

        if self.specs.get("clean_str_columns") and (
            str_col := self.specs.get("str_col")):
            self._clean_str_columns(cols=str_col)
            logging.info("Clean string columns.")

        if self.specs.get("drop_row_title"):
            self._drop_row_title()
            logging.info("Drop rows identical to header.")

        if self.specs.get("replace_row_char"):
            self._replace_row_char()
            logging.info("Replace chars in specified columns.")

        self._correct_data_type()
        logging.info("Data type correction.")

        if self.specs.get("clean_outliers") and (
            col_outlier := self.specs.get("col_outlier")):
            self._clean_outliers(cols=col_outlier)
            logging.info("Clean outliers.")

        self._export_output_file()
        logging.info("Export to CSV.")


    def _drop_duplicates(self) -> None:
        """
        Method removes duplicate rows.
        """
        self.df_copy.drop_duplicates(inplace=True)


    def _drop_na(self) -> None:
        """
        Method removes NaN values-containing rows.
        """
        self.df_copy.dropna(how="all", inplace=True)


    @data_checking
    def _drop_columns(self, cols: list[str]) -> None:
        """
        Method removes specified columns.
        """
        self.df_copy.drop(columns=cols, axis=1, inplace=True)


    @data_checking
    def _clean_str_columns(self, cols: list[str]) -> None:
        """
        Method removes whitespaces in specified string columns.
        """
        self.df_copy[cols] = self.df_copy[cols].map(str.strip)


    @data_checking
    def _drop_row_title(self) -> None:
        """
        Method removes rows identical to header row.
        """
        criteria = self.df_copy[self.df_copy.columns].ne(self.df_copy.columns).all(axis=1)
        self.df_copy = self.df_copy[criteria]


    @data_checking
    def _replace_row_char(self) -> None:
        """
        Method for replacing characters in specified columns.
        """
        replace_details = self.specs.get("replace_row_char_details")
        cols = replace_details.get("col")
        change = replace_details.get("change")

        self.df_copy[cols] = self.df_copy[cols].replace(change, regex=True)


    @data_checking
    def _correct_data_type(self) -> None:
        """
        Method for data type correction for specified columns.
        """
        numeric_cols = self.specs.get("numeric_col")
        self.df_copy[numeric_cols] = self.df_copy[numeric_cols].map(pd.to_numeric)

        datetime_cols = self.specs.get("datetime_col")
        self.df_copy[datetime_cols] = self.df_copy[datetime_cols].map(pd.to_datetime)

        convert_dict = {}
        convert_dict.update({col: str for col in self.specs.get("str_col")})
        convert_dict.update({col: float for col in self.specs.get("float_col")})
        convert_dict.update({col: int for col in self.specs.get("int_col")})

        self.df_copy = self.df_copy.astype(convert_dict)


    @data_checking
    def _clean_outliers(self, cols: list[str]):
        """
        Method removes outliers from data set using IQR
        """
        for col in cols:
            Q1 = self.df_copy[col].quantile(25 / 100)
            Q3 = self.df_copy[col].quantile(75 / 100)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            self.df_copy = self.df_copy[
                (self.df_copy[col] >= lower_bound) & (self.df_copy[col] <= upper_bound)
            ]


    def _export_output_file(self) -> None:
        """
        Method for saving cleaned data set to CSV file.
        """
        FileHandler.write_csv(
            data=self.df_copy,
            filename=self.specs.get("output_file")
        )


    def create_profiles(self) -> None:
        """
        Method generates detailed report for cleaned data.
        """

        clean_file = self.specs.get("output_file_profile")
        clean_report = ProfileReport(self.df_copy, title="Clean")
        FileHandler.create_profile(report=clean_report, filename=clean_file)
