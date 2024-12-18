from dataclasses import dataclass, field
import pandas as pd
from LLRunner.read.read_config import *
from LLRunner.custom_errors import AccessionNumberNotFoundError


@dataclass
class SST:
    """
    Class for the slide scanning tracker of the slide.
    """

    df: pd.DataFrame = field(init=False)  # Move this field before fields with defaults
    xlsx_path: str = field(default_factory=lambda: slide_scanning_tracker_path)
    sheet_name: str = field(default_factory=lambda: slide_scanning_tracker_sheet_name)

    def __post_init__(self):
        try:
            self.load_dataframe()
        except FileNotFoundError as e:
            print(f"Error loading file {self.xlsx_path}: {e}")
            self.df = None

    def load_dataframe(self):
        """Load the DataFrame from the Excel file."""
        self.df = pd.read_excel(self.xlsx_path, sheet_name=self.sheet_name)

    def get_dx(self, accession_number: str) -> tuple:
        """
        Get the diagnosis and sub-diagnosis of the slide.
        """
        if self.df is None:
            self.load_dataframe()

        required_columns = {"Accession Number", "General Dx", "Sub Dx"}
        if not required_columns.issubset(self.df.columns):
            raise ValueError(
                f"Missing required columns in the DataFrame: {required_columns - set(self.df.columns)}"
            )

        rows = self.df.loc[self.df["Accession Number"] == accession_number]
        dx_box = rows["General Dx"]
        subdx_box = rows["Sub Dx"]
        dx_str = dx_box.iloc[0] if not dx_box.empty else None
        subdx_str = subdx_box.iloc[0] if not subdx_box.empty else None

        dx_str = dx_str.strip() if isinstance(dx_str, str) else None
        subdx_str = subdx_str.strip() if isinstance(subdx_str, str) else None

        if dx_str is None and subdx_str is None:
            raise AccessionNumberNotFoundError(accession_number)

        return dx_str, subdx_str


sst = SST()
