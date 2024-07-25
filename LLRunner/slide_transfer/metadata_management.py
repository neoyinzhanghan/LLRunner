import os
import datetime
from pathlib import Path
from LLRunner.read.BMAInfo import *
from LLRunner.read.SST import *
from LLRunner.read.SR import *
from LLRunner.config import *


def get_slide_metadata_row(wsi_name):
    """ Use the SST and SR to get the metadata for the slide. """

    row_dct = {
        "wsi_name": None,
        "accession_number": None,
        "wsi_extension": None,
        "part_description": None,
        "reported_BMA": None,
        "reported_PB": None,
        "metadata_last_updated": None,
        "Dx": None,
        "sub_Dx": None,
        "slide_last_updated": None,
        # "blasts": None,
        # "blast-equivalents": None,
        # "promyelocytes": None,
        # "myelocytes": None,
        # "metamyelocytes": None,
        # "neutrophils/bands": None,
        # "monocytes": None,
        # "eosinophils": None,
        # "erythroid precursors": None,
        # "lymphocytes": None,
        # "plasma cells": None,
        "in_tmp_slide_dir": None,
        "level_0_mpp": -1,
        "topview_error": False,
        "level_0_mpp_error": False,
    }



    row_dct["wsi_name"] = wsi_name
    accession_number = wsi_name.split(";")[0]
    row_dct["accession_number"] = accession_number
    row_dct["wsi_extension"] = Path(wsi_name).suffix
    
    try:
        Dx, sub_Dx = sst.get_dx(accession_number)
    except AccessionNumberNotFoundError:
        Dx = "NA"
        sub_Dx = "NA"

    row_dct["Dx"] = Dx
    row_dct["sub_Dx"] = sub_Dx

    try:
        part_description = sr.get_part_description(wsi_name)
    except SlideNotFoundError:
        part_description = "NA"

    row_dct["part_description"] = part_description
    
    row_dct["reported_BMA"] = "bone marrow aspirate" in part_description.lower()
    row_dct["reported_PB"] = "peripheral blood" in part_description.lower()

    row_dct["metadata_last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    
    return row_dct


def get_bma_diff_metadata_row(wsi_name):
    """ Get the differential counts from the BMA info. """

    row_dct = {
        "wsi_name": None,
        "accession_number": None,
        "wsi_extension": None,
        # "part_description": None,
        # "reported_BMA": None,
        # "reported_PB": None,
        # "metadata_last_updated": None,
        # "Dx": None,
        # "sub_Dx": None,
        # "slide_last_updated": None,
        "diff_last_updated": None,
        "blasts": None,
        "blast-equivalents": None,
        "promyelocytes": None,
        "myelocytes": None,
        "metamyelocytes": None,
        "neutrophils/bands": None,
        "monocytes": None,
        "eosinophils": None,
        "erythroid precursors": None,
        "lymphocytes": None,
        "plasma cells": None,
        # "in_tmp_slide_dir": None,
        # "level_0_mpp": -1,
        # "topview_error": False,
        # "level_0_mpp_error": False,
    }

    row_dct["wsi_name"] = wsi_name
    accession_number = wsi_name.split(";")[0]
    row_dct["accession_number"] = accession_number
    row_dct["wsi_extension"] = Path(wsi_name).suffix

    diff_dct = bma_info.get_diff_dct_from_accession_number(accession_number)

    if diff_dct is not None:
        for key in diff_dct:
            row_dct[key] = diff_dct[key]
    else: # set all the values to -1
        row_dct["blasts"] = -1
        row_dct["blast-equivalents"] = -1
        row_dct["promyelocytes"] = -1
        row_dct["myelocytes"] = -1
        row_dct["metamyelocytes"] = -1
        row_dct["neutrophils/bands"] = -1
        row_dct["monocytes"] = -1
        row_dct["eosinophils"] = -1
        row_dct["erythroid precursors"] = -1
        row_dct["lymphocytes"] = -1
        row_dct["plasma cells"] = -1
    
    row_dct["diff_last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return row_dct

def update_slide_metadata(metadata_row_dct, overwrite=True):
    """ Update the slide metadata file by adding the new metadata row to the slide metadata file.
    If the row with the same slide name is already in the metadata file, then do nothing.
    Unless overwrite is set to True, then overwrite the row in the metadata file.
    """

    df = pd.read_csv(slide_metadata_path)
    
    wsi_name = metadata_row_dct["wsi_name"]

    # turn the metadata_row_dct into a df
    new_df_row = pd.DataFrame(metadata_row_dct, index=[0])
    
    # check to see if there is a row with the same slide name first
    if wsi_name in df["wsi_name"].values:
        if overwrite:
            df = df[df["wsi_name"] != wsi_name]
            df = pd.concat([df, new_df_row], ignore_index=True) 
    else:
        df = pd.concat([df, new_df_row], ignore_index=True)

    # save the df to replace the old metadata file
    df.to_csv(slide_metadata_path, index=False)


def update_bma_diff_metadata(metadata_row_dct, overwrite=True):
    """ Update the bma diff metadata file by adding the new metadata row to the bma diff metadata file.
    If the row with the same slide name is already in the metadata file, then do nothing.
    Unless overwrite is set to True, then overwrite the row in the metadata file.
    """

    df = pd.read_csv(bma_diff_metadata_path)
    
    wsi_name = metadata_row_dct["wsi_name"]

    # turn the metadata_row_dct into a df
    new_df_row = pd.DataFrame(metadata_row_dct, index=[0])
    
    # check to see if there is a row with the same slide name first
    if wsi_name in df["wsi_name"].values:
        if overwrite:
            df = df[df["wsi_name"] != wsi_name]
            df = pd.concat([df, new_df_row], ignore_index=True) 
    else:
        df = pd.concat([df, new_df_row], ignore_index=True)

    # save the df to replace the old metadata file
    df.to_csv(bma_diff_metadata_path, index=False)


def add_bma_diff_metadata_row(wsi_name):
    """ Update the bma_diff_metadata file by adding the new metadata row to the bma_diff_metadata file. """

    metadata_row_dct=get_bma_diff_metadata_row(wsi_name)
    update_bma_diff_metadata(metadata_row_dct, overwrite=True)