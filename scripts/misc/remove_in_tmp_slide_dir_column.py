import pandas as pd
from LLRunner.config import slide_metadata_path

# open the slide metadata
slide_metadata = pd.read_csv(slide_metadata_path)

# remove the 'tmp_slide_dir' column
slide_metadata = slide_metadata.drop(columns=["tmp_slide_dir"])

# overwrite the slide metadata
slide_metadata.to_csv(slide_metadata_path, index=False)
