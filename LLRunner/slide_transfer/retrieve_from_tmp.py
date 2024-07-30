import pandas as pd
from LLRunner.config import slide_metadata_path

def find_BMA_from_Dx(Dx):
    """ First find all the rows in the slide metadata where the column in_tmp_slide_dir is True. 
    Then further filter the rows based on the Dx column. 

    Return a list of all the wsi_name in the filtered rows.
    """

    # read the slide metadata
    slide_metadata = pd.read_csv(slide_metadata_path)

    # filter the rows based on the Dx column
    slide_metadata_filtered = slide_metadata[slide_metadata["Dx"] == Dx]

    # get the wsi_name from the filtered rows
    wsi_names = slide_metadata_filtered["wsi_name"].tolist()

    return wsi_names