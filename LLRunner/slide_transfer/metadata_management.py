import os
import datetime
from pathlib import Path
from tqdm import tqdm
from LLRunner.read.BMAInfo import *
from LLRunner.read.SST import *
from LLRunner.read.SR import *
from LLRunner.config import *
from LLRunner.slide_transfer.sshos import (
    SSHOS,
    get_pipeline_run_history_df,
    get_dzsave_metadata_df,
)
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


def get_pbs_diff_metadata_row(wsi_name):

    raise NotImplementedError(
        "The function get_pbs_diff_metadata_row is not implemented."
    )


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


def update_slide_metadata(metadata_row_dct, overwrite=False):
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


def update_pbs_diff_metadata(metadata_row_dct, overwrite=False):
    """Update the pbs diff metadata file by adding the new metadata row to the pbs diff metadata file.
    If the row with the same slide name is already in the metadata file, then do nothing.
    Unless overwrite is set to True, then overwrite the row in the metadata file.
    """

    df = pd.read_csv(pbs_diff_metadata_path)

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
    df.to_csv(pbs_diff_metadata_path, index=False)


def update_bma_diff_metadata(metadata_row_dct, overwrite=False):
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


def initialize_reported_bma_metadata(wsi_name_filter_func, overwrite=False):
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
            update_slide_metadata(metadata_row_dct, overwrite=overwrite)

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


def initialize_reported_pbs_metadata(wsi_name_filter_func, overwrite=False):
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
            update_slide_metadata(metadata_row_dct, overwrite=overwrite)

            # if metadata_row_dct["reported_BMA"]:
            #     bma_metadata_row_dct = get_bma_diff_metadata_row(wsi_name)
            #     update_bma_diff_metadata(
            #         metadata_row_dct=bma_metadata_row_dct, overwrite=overwrite
            #     )

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


def decide_what_to_run(wsi_name_filter_func, processing_filter_func, pipeline):
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

    elif pipeline == "PBS-diff":
        # only keep the rows in slide_md where reported_PB is True
        slide_md = slide_md[
            slide_md["reported_PB"]
        ]  # TODO DEPRECATED once we implement specimen classificaiton

    # use wsi_name_filter_func to filter the slide_md based on wsi_name column
    slide_md = slide_md[slide_md["wsi_name"].apply(wsi_name_filter_func)]

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


def decide_what_to_run_with_specimen_clf_cross_machine(
    wsi_name_filter_func, processing_filter_func, pipelines, rerun=False
):
    """Decide what to run based on the processing_filter_func and the pipeline.
    The processing_filter_func should take in the pipeline_run_history_path dataframe and then return a filtered dataframe.

    Instead basing on reported specimen, we will use the specimen classification model to decide what to run.
    Which means that at the metadata data stage we do not look at reported part description.
    Also make sure to check the pipeline_run_history_path for all machines
    """

    # first open the slide_metadata_path file
    slide_md = pd.read_csv(slide_metadata_path)

    for pipeline in pipelines:
        if pipeline not in available_pipelines:
            raise PipelineNotFoundError(pipeline)

    # use wsi_name_filter_func to filter the slide_md based on wsi_name column
    slide_md = slide_md[slide_md["wsi_name"].apply(wsi_name_filter_func)]

    # open the pipeline_run_history_path file for all machines
    pipeline_run_history_dfs = []
    for machine in available_machines:
        df = get_pipeline_run_history_df(machine)

        # only keep the rows in the pipeline_run_history_path for the specified pipelines in the list pipelines
        df = df[df["pipeline"].isin(pipelines)]

        filtered_df = processing_filter_func(df)

        pipeline_run_history_dfs.append(filtered_df)

    # concatenate all the dfs
    df = pd.concat(pipeline_run_history_dfs, ignore_index=True)

    # look for all the wsi_names in slide_md that are not in the filtered_df
    wsi_names = slide_md["wsi_name"].values

    wsi_names_to_run = []

    for wsi_name in wsi_names:
        if wsi_name not in df["wsi_name"].values:
            wsi_names_to_run.append(wsi_name)
        elif rerun:
            wsi_names_to_run.append(wsi_name)

    return wsi_names_to_run


def decide_what_to_run_dzsave_across_machines(
    wsi_name_filter_func,
    processing_filter_func,
):
    """Decide what to run based on the processing_filter_func and the pipeline.
    The processing_filter_func should take in the pipeline_run_history_path dataframe and then return a filtered dataframe.

    Instead basing on reported specimen, we will use the specimen classification model to decide what to run.
    Which means that at the metadata data stage we do not look at reported part description.
    Also make sure to check the pipeline_run_history_path for all machines
    """

    # first open the slide_metadata_path file
    slide_md = pd.read_csv(slide_metadata_path)

    # use wsi_name_filter_func to filter the slide_md based on wsi_name column
    slide_md = slide_md[slide_md["wsi_name"].apply(wsi_name_filter_func)]

    # open the pipeline_run_history_path file for all machines
    dzsave_history_dfs = []
    for machine in available_machines:
        df = get_dzsave_metadata_df(machine)

        filtered_df = processing_filter_func(df)

        dzsave_history_dfs.append(filtered_df)

    # concatenate all the dfs
    df = pd.concat(dzsave_history_dfs, ignore_index=True)

    # look for all the wsi_names in slide_md that are not in the filtered_df
    wsi_names = slide_md["wsi_name"].values

    wsi_names_to_run = []

    for wsi_name in wsi_names:
        if wsi_name not in df["wsi_name"].values:
            wsi_names_to_run.append(wsi_name)

    return wsi_names_to_run


def update_slide_time(wsi_name):
    # open the slide_metadata_path file
    slide_md = pd.read_csv(slide_metadata_path)

    # change the slide_last_updated to the current datetime
    slide_md.loc[slide_md["wsi_name"] == wsi_name, "slide_last_updated"] = (
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )


def which_are_already_ran(wsi_names, note=""):
    """Check which slides have already been ran for the specified pipelines and note."""
    # open the pipeline_run_history_path file for all machines
    pipeline_run_history_dfs = []
    for machine in available_machines:
        df = get_pipeline_run_history_df(machine)

        pipeline_run_history_dfs.append(df)

    # concatenate all the dfs
    df = pd.concat(pipeline_run_history_dfs, ignore_index=True)

    already_ran = []

    for wsi_name in tqdm(wsi_names, desc="Checking what has already been ran"):
        if wsi_name in df["wsi_name"].values:
            # check if the note is the same as the note in the df
            if df[df["wsi_name"] == wsi_name]["note"].values[0] == note:
                already_ran.append(wsi_name)

    return already_ran
