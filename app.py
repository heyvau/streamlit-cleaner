import streamlit as st
import shutil
import pandas as pd
from cleaner_csv import CleanerCSV
import os
from pathlib import Path

os.chdir(Path(__file__).parent)

FILE_NAME = "clean_data"

def specs_info(df: pd.DataFrame) -> dict:
    st.write("# Specification for data cleaning")

    drop_duplicates = st.checkbox("Drop duplicates", value=True)

    drop_na = st.checkbox("Drop NaNs", value=True)

    clean_str_columns = st.checkbox("Clean string columns", value=True)

    drop_row_title = st.checkbox("Drop header duplicates", value=True)

    replace_row_char = st.checkbox("Replace charecters in specified string columns", value=True)
    replace_in_columns = (st.multiselect("Replace in columns:", options=df.columns) if replace_row_char else [])
    char_to_replace = (st.text_input("Replace") if replace_in_columns else "")
    replacing = (st.text_input("To") if replace_in_columns else "")

    clean_outliers = st.checkbox("Clean outliers", value=True)
    clean_outliers_in_columns = (st.multiselect("Clean outliers in columns:", options=df.columns) if clean_outliers else [])

    st.write(" ## Types of columns data:")
    
    str_columns = st.multiselect("String columns:", options=df.columns)
    float_columns = st.multiselect("Float columns:", options=df.columns)
    int_columns = st.multiselect("Int columns:", options=df.columns)
    numeric_columns = st.multiselect("Numeric columns:", options=df.columns)
    datetime_columns = st.multiselect("Datetime columns:", options=df.columns)

    columns_to_drop = st.multiselect("Columns to drop:", options=df.columns)

    if st.button("Confirm"):
        return {
            "output_file": "./export/clean.csv",
            "output_file_profile": "./export/clean.html",

            "drop_duplicates": drop_duplicates,
            "drop_na": drop_na,
            "clean_str_columns": clean_str_columns,
            "drop_row_title": drop_row_title,
            "replace_row_char": replace_row_char,
            "clean_outliers": clean_outliers,

            "str_col": str_columns,
            "float_col": float_columns,
            "int_col": int_columns,
            "numeric_col": numeric_columns,
            "datetime_col": datetime_columns,

            "drop_col" : columns_to_drop,
            "col_outlier" : clean_outliers_in_columns,

            "replace_row_char_details": {
                "col": replace_in_columns,
                "change": {char_to_replace: replacing}
            }
        }


uploaded_csv = st.file_uploader("Choose a csv to clean", type=["csv"])

if uploaded_csv:
    df = pd.read_csv(uploaded_csv)
    spec = specs_info(df)
    if spec:
        cleaner = CleanerCSV(data=df, specs=spec)
        cleaner.clean()
        cleaner.create_profiles()
        st.success("Your data was cleaned based on specification. You can download it now.")

        shutil.make_archive(FILE_NAME, "zip", "./export/")

        with open(f"{FILE_NAME}.zip", "rb") as f:
            st.download_button("Download", f, file_name=f"{FILE_NAME}.zip")
