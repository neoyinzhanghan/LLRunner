import os
import datetime
from pathlib import Path
from tqdm import tqdm
from LLRunner.read.BMAInfo import *
from LLRunner.read.SST import *
from LLRunner.read.SR import *
from LLRunner.config import *
from LLRunner.slide_transfer.sshos import SSHOS
from LLRunner.custom_errors import PipelineNotFoundError


def get_slide_metadata_row(wsi_name):
    """Use the SST and SR to get the metadata for the slide."""

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

    row_dct["metadata_last_updated"] = datetime.datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return row_dct


def get_bma_diff_metadata_row(wsi_name):
    """Get the differential counts from the BMA info."""

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
    else:  # set all the values to -1
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
    """Update the slide metadata file by adding the new metadata row to the slide metadata file.
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
    """Update the bma diff metadata file by adding the new metadata row to the bma diff metadata file.
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
    """Update the bma_diff_metadata file by adding the new metadata row to the bma_diff_metadata file."""

    metadata_row_dct = get_bma_diff_metadata_row(wsi_name)
    update_bma_diff_metadata(metadata_row_dct, overwrite=True)


def pool_metadata_one_time(wsi_name_filter_func, overwrite=True):
    """Look for all the slides in the slide_source_dir and update the slide metadata file.
    wsi_name_filter_func is a function that takes in a wsi_name and returns True if the slide should be updated in the metadata file.
    """

    with SSHOS() as sshos:

        print("Looking for files in the slide source directory.")

        # first get the list of all the slides in the slide_source_dir
        files = sshos.listdir(slide_source_dir)
        print("Looking for WSIs amongst the files.")

        # only keep the slides such that the extension is in "allowed_extensions" and is a file
        wsi_names = [
            f for f in files if Path(f).suffix in allowed_extensions
        ]  # checking for sshos.isfile(f) is redundant and super slow for large directories

        print("Looking for slides that satisfy the specified conditions.")

        # only keep the slides such that wsi_name_filter_func(wsi_name) is True
        wsi_names = [f for f in wsi_names if wsi_name_filter_func(f)]

        print(
            f"{len(wsi_names)} slides are found satisfying the specified conditions. Beginning metadata pooling and update."
        )

        missing_Dx, missing_sub_Dx, missing_part_description = 0, 0, 0

        for wsi_name in tqdm(wsi_names, desc="Pooling and updating metadata"):
            metadata_row_dct = get_slide_metadata_row(wsi_name)
            update_slide_metadata(
                metadata_row_dct=metadata_row_dct, overwrite=overwrite
            )

            if metadata_row_dct["reported_BMA"]:
                bma_metadata_row_dct = get_bma_diff_metadata_row(wsi_name)
                update_bma_diff_metadata(
                    metadata_row_dct=bma_metadata_row_dct, overwrite=overwrite
                )

            if metadata_row_dct["Dx"] == "NA":
                missing_Dx += 1
            if metadata_row_dct["sub_Dx"] == "NA":
                missing_sub_Dx += 1
            if metadata_row_dct["part_description"] == "NA":
                missing_part_description += 1

        print(f"Finished updating metadata for {len(wsi_names)} slides.")
        print(f"Number of Slides Missing Dx: {missing_Dx}")
        print(f"Number of Slides Missing Sub Dx: {missing_sub_Dx}")
        print(f"Number of Slides Missing Part Description: {missing_part_description}")


def decide_what_to_run(processing_filter_func, pipeline):
    """Decide what to run based on the processing_filter_func and the pipeline.
    The processing_filter_func should take in the pipeline_run_history_path dataframe and then return a filtered dataframe.
    """

    # first open the slide_metadata_path file
    slide_md = pd.read_csv(slide_metadata_path)

    if pipeline not in available_pipelines:
        raise PipelineNotFoundError(pipeline)

    elif pipeline == "BMA-diff":
        # only keep the rows in slide_md where reported_BMA is True
        slide_md = slide_md[
            slide_md["reported_BMA"]
        ]  # TODO DEPRECATED once we implement specimen classificaiton

    # open the pipeline_run_history_path file
    df = pd.read_csv(pipeline_run_history_path)

    # only keep the rows in the pipeline_run_history_path for the specified pipeline
    df = df[df["pipeline"] == pipeline]

    filtered_df = processing_filter_func(df)

    # look for all the wsi_names in slide_md that are not in the filtered_df
    wsi_names = slide_md["wsi_name"].values

    wsi_names_to_run = []

    for wsi_name in wsi_names:
        if wsi_name not in filtered_df["wsi_name"].values:
            wsi_names_to_run.append(wsi_name)

    return wsi_names_to_run


def update_slide_time(wsi_name):
    # open the slide_metadata_path file
    slide_md = pd.read_csv(slide_metadata_path)

    # change the slide_last_updated to the current datetime
    slide_md.loc[slide_md["wsi_name"] == wsi_name, "slide_last_updated"] = (
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )


if __name__ == "__main__":

    # Here is a filter function which is strict equality
    def equality_filter(wsi_name):
        return wsi_name == "H23-7455;S11;MSK1 - 2024-02-07 21.43.57.ndpi"

    def H_filter(wsi_name):
        """Look for slides with name that starts with H."""
        return wsi_name.startswith("H")

    def identity_filter(pipeline_history_df):
        return pipeline_history_df

    pool_metadata_one_time(wsi_name_filter_func=H_filter, overwrite=True)

    wsi_names_to_run = decide_what_to_run(identity_filter, pipeline="BMA-diff")
    print(f"Found {len(wsi_names_to_run)} slides to run the BMA-diff pipeline on.")
