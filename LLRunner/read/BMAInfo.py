from dataclasses import dataclass, field
import pandas as pd
from LLRunner.read.read_config import *


@dataclass
class BMAInfo:
    """Class for keeping track of clinical information of the BMA slides."""

    csv_path: str = bma_info_path
    df: pd.DataFrame = field(init=False)

    def __post_init__(self):
        full_df = pd.read_csv(self.csv_path)

        # only keep the columns specnum_formatted, accession_date, part_description, text_data_clindx, blasts, blast-equivalents,
        # promyelocytes, myelocytes, metamyelocytes, neutrophils/bands, monocytes, eosinophils, erythroid precursors, lymphocytes, plasma cells
        self.df = full_df[
            [
                "specnum_formatted",
                "accession_date",
                "part_description",
                "text_data_clindx",
                "blasts",
                "blast-equivalents",
                "promyelocytes",
                "myelocytes",
                "metamyelocytes",
                "neutrophils/bands",
                "monocytes",
                "eosinophils",
                "erythroid precursors",
                "lymphocytes",
                "plasma cells",
            ]
        ]

    def get_row_from_slide_name(self, slide_name: str) -> pd.Series:
        """
        Get the row of the slide from the BMA info.
        """

        # split the slidename by ";" and take the first part to get the accession number
        accession_number = slide_name.split(";")[0]

        # the accession number should match the specnum_formatted column
        rows = self.df.loc[self.df["specnum_formatted"] == accession_number]

        # assert that either 0 or 1 rows are found
        if len(rows) > 1:
            # get the rows with most recent accession_date
            rows = rows.sort_values("accession_date", ascending=False).head(1)

        # if no rows are found, return None, else return the row
        if rows.empty:
            return None
        else:
            return rows.iloc[0]
        
    def get_diff_dct_from_accession_number(self, accession_number: str) -> dict:
        """
        Get the differential counts from the accession number.
        """

        # the accession number should match the specnum_formatted column
        rows = self.df.loc[self.df["specnum_formatted"] == accession_number]

        # assert that either 0 or 1 rows are found
        if len(rows) > 1:
            # get the rows with most recent accession_date
            rows = rows.sort_values("accession_date", ascending=False).head(1)

        # if no rows are found, return None, else return the row
        if rows.empty:
            return None
        else:
            row = rows.iloc[0]
            diff_dct = {
                "blasts": row["blasts"],
                "blast-equivalents": row["blast-equivalents"],
                "promyelocytes": row["promyelocytes"],
                "myelocytes": row["myelocytes"],
                "metamyelocytes": row["metamyelocytes"],
                "neutrophils/bands": row["neutrophils/bands"],
                "monocytes": row["monocytes"],
                "eosinophils": row["eosinophils"],
                "erythroid precursors": row["erythroid precursors"],
                "lymphocytes": row["lymphocytes"],
                "plasma cells": row["plasma cells"],
            }
        
            # make sure that the values are floats, if not, set them to -1
            for key in diff_dct:
                if not isinstance(diff_dct[key], (int, float)):
                    diff_dct[key] = -1

            return diff_dct

bma_info = BMAInfo()

