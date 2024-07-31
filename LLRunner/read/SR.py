from dataclasses import dataclass, field
import pandas as pd
from LLRunner.read.read_config import *
from LLRunner.custom_errors import SlideNotFoundError


@dataclass
class SR:
    """
    Class for the status results of the slide.
    """

    csv_path: str = status_results_path
    df: pd.DataFrame = field(init=False)

    def __post_init__(self):
        self.df = pd.read_csv(self.csv_path)

    def get_part_description(self, slide_name: str) -> str:
        """
        Get the part description of the slide.
        """
        part_description_box = self.df.loc[
            self.df.index.str.strip() == slide_name, "Part Description"
        ]

        if part_description_box.empty:
            raise SlideNotFoundError(
                f"Slide name '{slide_name}' not found in the dataset."
            )

        part_description = str(part_description_box.iloc[0]).strip()
        assert isinstance(
            part_description, str
        ), f"The part description is not a string. {part_description} is of type {type(part_description)}."

        return part_description

    def get_recorded_specimen_type(self, slide_name: str) -> str:
        """
        Get the specimen type of the slide.
        """
        specimen_type_str = self.get_part_description(slide_name)
        
        if "bone marrow aspirate" in specimen_type_str.lower():
            specimen_type = "BMA"
        elif "peripheral blood" in specimen_type_str.lower():
            specimen_type = "PB"
        else:
            specimen_type = "Others"

        return specimen_type


sr = SR()

if __name__ == "__main__":
    print(sr.get_recorded_specimen_type("H22-391;S10;MSK1 - 2023-05-31 21.16.06.ndpi"))
