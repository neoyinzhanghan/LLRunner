import os
import random
import csv
import math
import paramiko
import pandas as pd
from PIL import Image
from io import BytesIO
from pathlib import Path
from PIL import Image
from LLRunner.read.read_config import *
from LLRunner.config import (
    pipeline_run_history_path,
    slide_metadata_path,
    bma_diff_metadata_path,
)
from LLRunner.slide_transfer.sshos import SSHOS


####################################################################################################
# HELPER FUNCTIONS
####################################################################################################


def csv_to_dict(file_path):
    result_dict = {}
    with open(file_path, mode="r") as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            key = row[0]  # Access the first column as the key
            value = float(
                row[1]
            )  # Access the second column as the value, converting it to float
            result_dict[key] = value
    return result_dict


####################################################################################################
# LOCAL RESULT DIRECTORY IMPLEMENTATION
####################################################################################################


class BMAResult:
    """
    Class for keeping track of the results of the LLRunner on a BMA slide.

    === Class Attributes ===
    -- result_dir: the directory where the results are stored
    -- result_folder_name: the name of the folder where the results are stored
    -- pipeline: result_folder_name.split("_")[0]
    -- datetime_processed: result_folder_name.split("_")[1]
    -- error: a boolean indicating if the result directory is an error directory
    -- cell_info: a pandas DataFrame containing the cell information
    """

    def __init__(self, result_dir):
        """Use the result directory to get the results of the LLRunner."""

        # result_dir is a string, make it a Path object
        result_dir = Path(result_dir)

        # first check if the result directory exists
        assert result_dir.exists(), f"Result directory {result_dir} does not exist."

        # get the name of the folder where the results are stored
        self.result_folder_name = result_dir.name

        # get the pipeline from the result_folder_name
        self.pipeline = self.result_folder_name.split("_")[0]

        # get the datetime_processed from the result_folder_name
        self.datetime_processed = self.result_folder_name.split("_")[1]

        # second check if the result directory's folder name starts with "ERROR_"
        self.error = self.has_error()

        # check that the result directory exists and is actually a directory
        assert result_dir.is_dir(), f"{result_dir} is not a valid directory."

        self.result_dir = result_dir
        self.cell_info = pd.read_csv(result_dir / "cells" / "cells_info.csv")

    def get_stacked_differential(self):
        """In the cell_info dataframe there are columns corresponding to each cell type in the list cellnames.
        Take the average of the probabilities for each cell type and return a dictionary with the cell type as the key and the average probability as the value.
        """

        assert not self.error, "Cannot get stacked differential for error directory."

        # get the columns corresponding to the cell types
        cell_columns = self.cell_info[cellnames]

        # take the average of the probabilities for each cell type
        average_probabilities = cell_columns.mean()

        # return a dictionary with the cell type as the key and the average probability as the value
        return average_probabilities.to_dict()

    def get_one_hot_differential(self):
        """
        In the cell_info dataframe there are columns corresponding to each cell type in the list cellnames.
        The predicted class is the class with the highest probability, return a dictionary with the cell type as the key and the proportion of cells predicted as that type as the value.
        """

        assert not self.error, "Cannot get one hot differential for error directory."

        # get the columns corresponding to the cell types
        cell_columns = self.cell_info[cellnames]

        # get the predicted class for each cell
        predicted_classes = cell_columns.idxmax(axis=1)

        # get the proportion of cells predicted as each cell type
        one_hot_differential = predicted_classes.value_counts(normalize=True)

        # return a dictionary with the cell type as the key and the proportion of cells predicted as that type as the value
        return one_hot_differential.to_dict()

    def get_grouped_differential(self):
        """
        First set the probability in the omitted classes to 0.
        Then classify the cells into the cellnames classes and remove all the cells in the removed classes.
        Finally, give the cell a class from the differential_group_dict based on their cellname classes.
        (The differential_group_dict should map a grouped class to the a list of cellname classes that grouped class contains)
        """

        assert not self.error, "Cannot get grouped differential for error directory."

        # set the values of columns corresponding to the omitted classes to 0
        self.cell_info[omitted_classes] = 0

        # classify the cells into the cellnames classes
        self.cell_info["cell_class"] = self.cell_info[cellnames].idxmax(axis=1)

        # remove all the cells in the removed classes
        self.cell_info = self.cell_info[
            ~self.cell_info["cell_class"].isin(removed_classes)
        ]

        # give the cell a class from the differential_group_dict based on their cellname classes
        self.cell_info["grouped_class"] = self.cell_info["cell_class"].apply(
            lambda x: next(
                (
                    grouped_class
                    for grouped_class in differential_group_dict
                    if x in differential_group_dict[grouped_class]
                ),
                None,
            )
        )

        # get the proportion of cells in each grouped class
        grouped_differential = self.cell_info["grouped_class"].value_counts(
            normalize=True
        )

        # return a dictionary with the grouped class as the key and the proportion of cells in that class as the value
        return grouped_differential.to_dict()

    def get_grouped_stacked_differential(self):
        """First set the omitted class probabilities to zero, and then remove all rows where the top probability class is in removed_classes.
        Then use the differential_group_dict to group the cells into the grouped classes.
        But you stack the probablities together, for example if "neutrophils/bands": ["M5", "M6"]" then you sum the probabilities of M5 and M6 together to get the probability of neutrophils/bands.
        Then return a dictionary with the grouped class as the key and the average probability as the value.
        """

        assert (
            not self.error
        ), "Cannot get grouped stacked differential for error directory."

        # set the values of columns corresponding to the omitted classes to 0
        self.cell_info[omitted_classes] = 0

        # classify the cells into the cellnames classes
        self.cell_info["cell_class"] = self.cell_info[cellnames].idxmax(axis=1)

        # remove all the cells in the removed classes
        self.cell_info = self.cell_info[
            ~self.cell_info["cell_class"].isin(removed_classes)
        ]

        for grouped_class, cellnames_list in differential_group_dict.items():
            self.cell_info[grouped_class + "_grouped_stacked"] = self.cell_info[
                cellnames_list
            ].sum(axis=1)

        # get the average probability of cells in each grouped class
        grouped_stacked_differential = self.cell_info[
            [
                grouped_class + "_grouped_stacked"
                for grouped_class in differential_group_dict
            ]
        ].mean()

        # return a dictionary with the grouped class as the key and the average probability of cells in that class as the value
        prob_dict = grouped_stacked_differential.to_dict()

        # normalize the probabilities so it sums to 1
        prob_sum = sum(prob_dict.values())

        return {key: value / prob_sum for key, value in prob_dict.items()}

    def get_raw_counts(self):
        """
        Return the raw counts of the cells in the cell_info dataframe after classifying the cells into the cellnames classes.
        """

        assert not self.error, "Cannot get raw counts for error directory."

        # classify the cells into the cellnames classes
        self.cell_info["cell_class"] = self.cell_info[cellnames].idxmax(axis=1)

        # get the raw counts of the cells in the cell_info dataframe
        raw_counts = self.cell_info["cell_class"].value_counts()

        # return a dictionary with the cell type as the key and the raw count of cells of that type as the value
        return raw_counts.to_dict()

    def get_grouped_raw_counts(self):
        """
        Return the raw counts of the cells in the cell_info dataframe after classifying the cells into the cellnames classes
        after setting omitted class probabilities to 0 and removing all the cells in the removed classes.
        The counts should be grouped based on the differential_group_dict.
        """

        assert not self.error, "Cannot get grouped raw counts for error directory."

        # set the values of columns corresponding to the omitted classes to 0
        self.cell_info[omitted_classes] = 0

        # classify the cells into the cellnames classes
        self.cell_info["cell_class"] = self.cell_info[cellnames].idxmax(axis=1)

        # remove all the cells in the removed classes
        self.cell_info = self.cell_info[
            ~self.cell_info["cell_class"].isin(removed_classes)
        ]

        # give the cell a class from the differential_group_dict based on their cellname classes
        self.cell_info["grouped_class"] = self.cell_info["cell_class"].apply(
            lambda x: next(
                (
                    grouped_class
                    for grouped_class in differential_group_dict
                    if x in differential_group_dict[grouped_class]
                ),
                None,
            )
        )

        # get the raw counts of the cells in the cell_info dataframe
        grouped_raw_counts = self.cell_info["grouped_class"].value_counts()

        # return a dictionary with the grouped class as the key and the raw count of cells in that class as the value
        return grouped_raw_counts.to_dict()

    def get_grid_rep(self):
        """Return the grid rep image of the slide.
        Which is located at the directory/top_view_grid_rep.png.
        Use PIL
        """

        assert not self.error, "Cannot get grid rep for error directory."

        grid_rep_path = self.result_dir / "top_view_grid_rep.png"

        return Image.open(grid_rep_path)

    def get_confidence_heatmap(self):
        """Return the confidence heatmap image of the slide.
        Which is located at the directory/confidence_heatmap.png.
        Use PIL
        """

        assert not self.error, "Cannot get confidence heatmap for error directory."

        confidence_heatmap_path = self.result_dir / "confidence_heatmap.png"

        return Image.open(confidence_heatmap_path)

    def has_error(self):
        return bool(self.get_run_history()["error"])

    def get_region_confidence(self, region_idx):
        """Use the focus_regions/focus_regions_info.csv file and the focus_regions/high_mag_focus_regions_info.csv file to get the confidence of the region with region_idx."""

        assert not self.error, "Cannot get region confidence for error directory."

        results_dir_Path = Path(self.result_dir)

        # read the focus_regions_info.csv file
        focus_regions_info = pd.read_csv(
            results_dir_Path / "focus_regions" / "focus_regions_info.csv"
        )

        # get the row in the focus_regions_info dataframe that corresponds to the region_idx
        row = focus_regions_info[focus_regions_info["idx"] == int(region_idx)]

        assert (
            len(row) >= 1
        ), f"Region with idx {region_idx} not found in focus_regions_info.csv"
        assert (
            len(row) == 1
        ), f"Multiple regions with idx {region_idx} found in focus_regions_info.csv This should never happen and the pipeline log could be corrupted."

        # get the confidence of the region
        low_mag_confidence = row["adequate_confidence_score"].iloc[0]

        # now do the samething with the high_mag_focus_regions_info.csv file
        high_mag_focus_regions_info = pd.read_csv(
            self.result_dir / "focus_regions" / "high_mag_focus_regions_info.csv"
        )

        row = high_mag_focus_regions_info[
            high_mag_focus_regions_info["idx"] == int(region_idx)
        ]

        # if there is no row found, then just None
        if len(row) == 0:
            return low_mag_confidence, None

        assert (
            len(row) == 1
        ), f"Multiple regions with idx {region_idx} found in high_mag_focus_regions_info.csv This should never happen and the pipeline log could be corrupted."

        high_mag_confidence = row["adequate_confidence_score_high_mag"].iloc[0]

        return low_mag_confidence, high_mag_confidence

    def get_focus_regions(self, num_to_sample=5):
        """Return the top regions image of the slide.
        Which is located at the directory/focus_regions.png.
        Use PIL
        """

        assert not self.error, "Cannot get focus regions for error directory."

        # get the list of all the top regions images which are stored in dir/focus_regions/high_mag_unannotated
        result_dir_Path = Path(self.result_dir)
        focus_regions_dir = result_dir_Path / "focus_regions"
        high_mag_unannotated_dir = focus_regions_dir / "high_mag_unannotated"

        # get the list of all the top regions images jpg files in the directory
        focus_regions_files = list(high_mag_unannotated_dir.glob("*.jpg"))

        # randomly sample num_to_sample images from the list
        focus_regions_files_sample = random.sample(focus_regions_files, num_to_sample)

        # open the images using PIL and return them as a list, also the corresponding annotated images as a list
        focus_regions_images = [Image.open(file) for file in focus_regions_files_sample]
        # the annotated images have the same name but comes from high_mag_annotated instead of high_mag_unannotated
        focus_regions_annotated_images = [
            Image.open(focus_regions_dir / "high_mag_annotated" / file.name)
            for file in focus_regions_files_sample
        ]

        # for each focus region, the filename without the extension is the region id
        region_idxs = [file.stem for file in focus_regions_files]

        low_mag_confidences = []
        high_mag_confidences = []

        for region_idx in region_idxs:
            low_mag_confidence, high_mag_confidence = self.get_region_confidence(
                region_idx
            )

            assert (
                high_mag_confidence is not None
            ), f"High mag confidence for region {region_idx} is None. This should not happen because the region should have passed high mag check."

            low_mag_confidences.append(low_mag_confidence)
            high_mag_confidences.append(high_mag_confidence)

        return (
            focus_regions_images,
            focus_regions_annotated_images,
            region_idxs,
            low_mag_confidences,
            high_mag_confidences,
        )

    def get_runtime_breakdown(self):  # TODO
        """Return the runtime breakdown of the slide.
        Which is located at the directory/runtime_breakdown.csv.
        """

        assert not self.error, "Cannot get runtime breakdown for error directory."

        # the runtime_data_path is dir/runtime_data.csv
        runtime_data_path = self.result_dir / "runtime_data.csv"

        # read the runtime_data.csv file
        runtime_data_dict = csv_to_dict(runtime_data_path)

        return runtime_data_dict

    def _convert_size(self, size_bytes):
        """Convert bytes to a more human-readable format (KB, MB, GB, etc.)."""
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"

    def get_storage_consumption_breakdown(self):
        """Return the storage consumption breakdown of the slide result folder."""
        if not os.path.isdir(self.result_dir):
            raise NotADirectoryError(f"{self.result_dir} is not a valid directory.")

        breakdown = {}
        for root, dirs, files in os.walk(self.result_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                extension = os.path.splitext(file)[1].lower()  # Get file extension

                if extension not in breakdown:
                    breakdown[extension] = 0
                breakdown[extension] += file_size

        # Convert file sizes to a more readable format (e.g., MB, GB)
        breakdown_readable = {
            ext: self._convert_size(size) for ext, size in breakdown.items()
        }

        return breakdown_readable

    def get_run_history(self):
        """Return the run history of the slide.
        Which is located at the pipeline_run_history file.
        """

        run_history = pd.read_csv(pipeline_run_history_path)

        # look for the row in the run_history dataframe that corresponds to the pipeline and datetime_processed
        row = run_history[
            (run_history["pipeline"] == self.pipeline)
            & (run_history["datetime_processed"] == self.datetime_processed)
        ]

        assert (
            len(row) >= 1
        ), f"Row not found in run_history for {self.pipeline} and {self.datetime_processed}"

        assert (
            len(row) == 1
        ), f"Multiple rows found in run_history for {self.pipeline} and {self.datetime_processed}. This should never happen and the pipeline log could be corrupted."

        # convert row to dictionary
        run_history_dict = row.to_dict(orient="records")[0]

        return run_history_dict

    def get_wsi_name(self):
        """From the slide_metadata.csv file, get the wsi_name of the slide."""

        run_history_dict = self.get_run_history()

        return run_history_dict["wsi_name"]

    def get_slide_metadata_dict(self):
        """Return the slide metadata dictionary of the slide.
        Which is located at the directory/slide_metadata.csv.
        """

        wsi_name = self.get_wsi_name()

        slide_metadata = pd.read_csv(slide_metadata_path)

        # get the row in the slide_metadata dataframe that corresponds to the wsi_name
        row = slide_metadata[slide_metadata["wsi_name"] == wsi_name]

        assert len(row) >= 1, f"Row not found in slide_metadata for {wsi_name}"

        assert (
            len(row) == 1
        ), f"Multiple rows found in slide_metadata for {wsi_name}. This should never happen and the pipeline log could be corrupted."

        # convert row to dictionary
        slide_metadata_dict = row.to_dict(orient="records")[0]

        return slide_metadata_dict

    def get_datetime_processed(self):  # TODO
        return self.datetime_processed

    def get_pipeline(self):
        return self.pipeline

    def get_part_description(self):
        """From the slide_metadata.csv file, get the part_description of the slide."""
        part_desc = self.get_slide_metadata_dict()["part_description"]

        # if the part_desc is nan, or a string of spaces, or None, or empty string, then return "Missing part description"
        if pd.isna(part_desc) or part_desc.isspace() or not part_desc:
            return "Missing part description"

        return part_desc

    def get_Dx_and_sub_Dx(self):  # TODO
        """From the slide_metadata.csv file, get the Dx and sub_Dx of the slide."""

        Dx, sub_Dx = (
            self.get_slide_metadata_dict()["Dx"],
            self.get_slide_metadata_dict()["sub_Dx"],
        )

        if pd.isna(Dx) or Dx.isspace() or not Dx:
            Dx = "Missing Dx"

        if pd.isna(sub_Dx) or sub_Dx.isspace() or not sub_Dx:
            sub_Dx = "Missing sub_Dx"

        return Dx, sub_Dx

    def get_reported_differential(self):
        """Use the bma_diff_metadata.csv file to get the reported differential of the slide."""

        diff_dict = {
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
        }

        bma_diff_metadata = pd.read_csv(bma_diff_metadata_path)

        # get the row in the bma_diff_metadata dataframe that corresponds to the wsi_name
        row = bma_diff_metadata[bma_diff_metadata["wsi_name"] == self.get_wsi_name()]

        if len(row) == 0:
            print(
                "User Warning: No row found in bma_diff_metadata for this slide. Returning None for all cell types. However, this means that the slide differential has not been pulled from the database. This behaviour could indicate a problem in the data pipeline."
            )
            return diff_dict

        assert (
            len(row) == 1
        ), f"Multiple rows found in bma_diff_metadata for {self.get_wsi_name()}. This should never happen and the pipeline log could be corrupted."

        blasts = row["blasts"].iloc[0]
        blast_equivalents = row["blast-equivalents"].iloc[0]
        promyelocytes = row["promyelocytes"].iloc[0]
        myelocytes = row["myelocytes"].iloc[0]
        metamyelocytes = row["metamyelocytes"].iloc[0]
        neutrophils_bands = row["neutrophils/bands"].iloc[0]
        monocytes = row["monocytes"].iloc[0]
        eosinophils = row["eosinophils"].iloc[0]
        erythroid_precursors = row["erythroid precursors"].iloc[0]
        lymphocytes = row["lymphocytes"].iloc[0]
        plasma_cells = row["plasma cells"].iloc[0]

        diff_dict = {
            "blasts": blasts,
            "blast-equivalents": blast_equivalents,
            "promyelocytes": promyelocytes,
            "myelocytes": myelocytes,
            "metamyelocytes": metamyelocytes,
            "neutrophils/bands": neutrophils_bands,
            "monocytes": monocytes,
            "eosinophils": eosinophils,
            "erythroid precursors": erythroid_precursors,
            "lymphocytes": lymphocytes,
            "plasma cells": plasma_cells,
        }

        return diff_dict

    def get_num_regions(self):
        """Count how many regio jpg images are in the high_mag_unannotated folder."""

        assert not self.error, "Cannot get num regions for error directory."

        high_mag_unannotated_dir = (
            self.result_dir / "focus_regions" / "high_mag_unannotated"
        )

        return len(list(high_mag_unannotated_dir.glob("*.jpg")))

    def get_num_cells(self):
        """Recursively count how many jpg files are in the cells folder and its subfolders."""

        assert not self.error, "Cannot get num cells for error directory."

        cells_dir = self.result_dir / "cells"

        num_cells = 0
        for root, dirs, files in os.walk(cells_dir):
            for file in files:
                if file.endswith(".jpg"):
                    num_cells += 1

        return num_cells

    def get_note(self):
        """Return the note from the run history."""
        return self.get_run_history()["note"]


####################################################################################################
# SSH RESULT DIRECTORY IMPLEMENTATION
####################################################################################################


class BMAResultSSH:
    def __init__(
        self,
        hostname,
        username,
        remote_result_dir,
        rsa_key_path=None,
        max_retries=3,
        backoff_factor=2,
    ):
        """Initialize the BMAResultSSH class with remote SSH access."""
        self.hostname = hostname
        self.username = username
        self.remote_result_dir = Path(remote_result_dir)
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(
            hostname=self.hostname, username=self.username, key_filename=rsa_key_path
        )
        self.sftp_client = self.ssh_client.open_sftp()

        try:
            self.sftp_client.stat(str(self.remote_result_dir))
        except FileNotFoundError:
            raise NotADirectoryError(
                f"Result directory {self.remote_result_dir} does not exist on the remote server."
            )

        self.result_folder_name = self.remote_result_dir.name
        self.pipeline = self.result_folder_name.split("_")[0]
        self.datetime_processed = self.result_folder_name.split("_")[1]

        self.error = self.has_error()

        if self.error:
            pass  # TODO

        with SSHOS(hostname=self.hostname, username=self.username) as sshos:
            # check that the remote result directory exists
            assert sshos.isdir(
                self.remote_result_dir
            ), f"Remote result directory {self.remote_result_dir} does not exist on the server {self.username}@{self.hostname}."

        with self.sftp_client.open(
            str(self.remote_result_dir / "cells" / "cells_info.csv"), "r"
        ) as f:
            self.cell_info = pd.read_csv(f)

    def get_stacked_differential(self):
        """In the cell_info dataframe there are columns corresponding to each cell type in the list cellnames.
        Take the average of the probabilities for each cell type and return a dictionary with the cell type as the key and the average probability as the value.
        """

        assert not self.error, "Cannot get stacked differential for error directories."

        # get the columns corresponding to the cell types
        cell_columns = self.cell_info[cellnames]

        # take the average of the probabilities for each cell type
        average_probabilities = cell_columns.mean()

        # return a dictionary with the cell type as the key and the average probability as the value
        return average_probabilities.to_dict()

    def get_one_hot_differential(self):
        """
        In the cell_info dataframe there are columns corresponding to each cell type in the list cellnames.
        The predicted class is the class with the highest probability, return a dictionary with the cell type as the key and the proportion of cells predicted as that type as the value.
        """

        assert not self.error, "Cannot get stacked differential for error directories."

        # get the columns corresponding to the cell types
        cell_columns = self.cell_info[cellnames]

        # get the predicted class for each cell
        predicted_classes = cell_columns.idxmax(axis=1)

        # get the proportion of cells predicted as each cell type
        one_hot_differential = predicted_classes.value_counts(normalize=True)

        # return a dictionary with the cell type as the key and the proportion of cells predicted as that type as the value
        return one_hot_differential.to_dict()

    def get_grouped_differential(self):
        """
        First set the probability in the omitted classes to 0.
        Then classify the cells into the cellnames classes and remove all the cells in the removed classes.
        Finally, give the cell a class from the differential_group_dict based on their cellname classes.
        (The differential_group_dict should map a grouped class to the a list of cellname classes that grouped class contains)
        """

        assert not self.error, "Cannot get stacked differential for error directories."

        # set the values of columns corresponding to the omitted classes to 0
        self.cell_info[omitted_classes] = 0

        # classify the cells into the cellnames classes
        self.cell_info["cell_class"] = self.cell_info[cellnames].idxmax(axis=1)

        # remove all the cells in the removed classes
        self.cell_info = self.cell_info[
            ~self.cell_info["cell_class"].isin(removed_classes)
        ]

        # give the cell a class from the differential_group_dict based on their cellname classes
        self.cell_info["grouped_class"] = self.cell_info["cell_class"].apply(
            lambda x: next(
                (
                    grouped_class
                    for grouped_class in differential_group_dict
                    if x in differential_group_dict[grouped_class]
                ),
                None,
            )
        )

        # get the proportion of cells in each grouped class
        grouped_differential = self.cell_info["grouped_class"].value_counts(
            normalize=True
        )

        # return a dictionary with the grouped class as the key and the proportion of cells in that class as the value
        return grouped_differential.to_dict()

    def get_grouped_stacked_differential(self):
        """First set the omitted class probabilities to zero, and then remove all rows where the top probability class is in removed_classes.
        Then use the differential_group_dict to group the cells into the grouped classes.
        But you stack the probablities together, for example if "neutrophils/bands": ["M5", "M6"]" then you sum the probabilities of M5 and M6 together to get the probability of neutrophils/bands.
        Then return a dictionary with the grouped class as the key and the average probability as the value.
        """

        assert not self.error, "Cannot get stacked differential for error directories."

        # set the values of columns corresponding to the omitted classes to 0
        self.cell_info[omitted_classes] = 0

        # classify the cells into the cellnames classes
        self.cell_info["cell_class"] = self.cell_info[cellnames].idxmax(axis=1)

        # remove all the cells in the removed classes
        self.cell_info = self.cell_info[
            ~self.cell_info["cell_class"].isin(removed_classes)
        ]

        for grouped_class, cellnames_list in differential_group_dict.items():
            self.cell_info[grouped_class + "_grouped_stacked"] = self.cell_info[
                cellnames_list
            ].sum(axis=1)

        # get the average probability of cells in each grouped class
        grouped_stacked_differential = self.cell_info[
            [
                grouped_class + "_grouped_stacked"
                for grouped_class in differential_group_dict
            ]
        ].mean()

        # return a dictionary with the grouped class as the key and the average probability of cells in that class as the value
        prob_dict = grouped_stacked_differential.to_dict()

        # normalize the probabilities so it sums to 1
        prob_sum = sum(prob_dict.values())

        return {key: value / prob_sum for key, value in prob_dict.items()}

    def get_raw_counts(self):
        """
        Return the raw counts of the cells in the cell_info dataframe after classifying the cells into the cellnames classes.
        """

        assert not self.error, "Cannot get stacked differential for error directories."

        # classify the cells into the cellnames classes
        self.cell_info["cell_class"] = self.cell_info[cellnames].idxmax(axis=1)

        # get the raw counts of the cells in the cell_info dataframe
        raw_counts = self.cell_info["cell_class"].value_counts()

        # return a dictionary with the cell type as the key and the raw count of cells of that type as the value
        return raw_counts.to_dict()

    def get_grouped_raw_counts(self):
        """
        Return the raw counts of the cells in the cell_info dataframe after classifying the cells into the cellnames classes
        after setting omitted class probabilities to 0 and removing all the cells in the removed classes.
        The counts should be grouped based on the differential_group_dict.
        """

        assert not self.error, "Cannot get stacked differential for error directories."

        # set the values of columns corresponding to the omitted classes to 0
        self.cell_info[omitted_classes] = 0

        # classify the cells into the cellnames classes
        self.cell_info["cell_class"] = self.cell_info[cellnames].idxmax(axis=1)

        # remove all the cells in the removed classes
        self.cell_info = self.cell_info[
            ~self.cell_info["cell_class"].isin(removed_classes)
        ]

        # give the cell a class from the differential_group_dict based on their cellname classes
        self.cell_info["grouped_class"] = self.cell_info["cell_class"].apply(
            lambda x: next(
                (
                    grouped_class
                    for grouped_class in differential_group_dict
                    if x in differential_group_dict[grouped_class]
                ),
                None,
            )
        )

        # get the raw counts of the cells in the cell_info dataframe
        grouped_raw_counts = self.cell_info["grouped_class"].value_counts()

        # return a dictionary with the grouped class as the key and the raw count of cells in that class as the value
        return grouped_raw_counts.to_dict()

    def has_error(self):
        return bool(self.get_run_history()["error"])

    def get_confidence_heatmap(self):
        """Return the confidence heatmap image of the slide.
        Which is located at the directory/confidence_heatmap.png.
        Use PIL
        """

        assert not self.error, "Cannot get confidence heatmap for error directories."

        confidence_heatmap_path = self.remote_result_dir / "confidence_heatmap.png"

        with self.sftp_client.open(str(confidence_heatmap_path), "rb") as f:
            return Image.open(BytesIO(f.read()))

    def get_region_confidence(self, region_idx):
        """Get the confidence scores for a specific region."""

        assert not self.error, "Cannot get region confidence for error directories."

        focus_regions_info_path = (
            self.remote_result_dir / "focus_regions" / "focus_regions_info.csv"
        )
        high_mag_focus_regions_info_path = (
            self.remote_result_dir / "focus_regions" / "high_mag_focus_regions_info.csv"
        )

        with self.sftp_client.open(str(focus_regions_info_path), "r") as f:
            focus_regions_info = pd.read_csv(f)

        row = focus_regions_info[focus_regions_info["idx"] == int(region_idx)]

        assert (
            len(row) >= 1
        ), f"Region with idx {region_idx} not found in focus_regions_info.csv"
        assert (
            len(row) == 1
        ), f"Multiple regions with idx {region_idx} found. This should not happen."

        low_mag_confidence = row["adequate_confidence_score"].iloc[0]

        with self.sftp_client.open(str(high_mag_focus_regions_info_path), "r") as f:
            high_mag_focus_regions_info = pd.read_csv(f)

        row = high_mag_focus_regions_info[
            high_mag_focus_regions_info["idx"] == int(region_idx)
        ]

        if len(row) == 0:
            return low_mag_confidence, None

        assert (
            len(row) == 1
        ), f"Multiple regions with idx {region_idx} found in high_mag_focus_regions_info.csv. This should not happen."

        high_mag_confidence = row["adequate_confidence_score_high_mag"].iloc[0]

        return low_mag_confidence, high_mag_confidence

    def get_focus_regions(self, num_to_sample=5):
        """Return images of the top regions of the slide."""

        assert not self.error, "Cannot get focus regions for error directories."

        high_mag_unannotated_dir = (
            self.remote_result_dir / "focus_regions" / "high_mag_unannotated"
        )
        high_mag_annotated_dir = (
            self.remote_result_dir / "focus_regions" / "high_mag_annotated"
        )

        focus_regions_files = self.sftp_client.listdir(str(high_mag_unannotated_dir))
        focus_regions_files = [f for f in focus_regions_files if f.endswith(".jpg")]

        focus_regions_files_sample = random.sample(focus_regions_files, num_to_sample)

        focus_regions_images = []
        focus_regions_annotated_images = []
        for file_name in focus_regions_files_sample:
            with self.sftp_client.open(
                str(high_mag_unannotated_dir / file_name), "rb"
            ) as f:
                focus_regions_images.append(Image.open(BytesIO(f.read())))
            with self.sftp_client.open(
                str(high_mag_annotated_dir / file_name), "rb"
            ) as f:
                focus_regions_annotated_images.append(Image.open(BytesIO(f.read())))

        region_idxs = [Path(file).stem for file in focus_regions_files_sample]

        low_mag_confidences = []
        high_mag_confidences = []

        for region_idx in region_idxs:
            low_mag_confidence, high_mag_confidence = self.get_region_confidence(
                region_idx
            )

            assert (
                high_mag_confidence is not None
            ), f"High mag confidence for region {region_idx} is None."

            low_mag_confidences.append(low_mag_confidence)
            high_mag_confidences.append(high_mag_confidence)

        return (
            focus_regions_images,
            focus_regions_annotated_images,
            region_idxs,
            low_mag_confidences,
            high_mag_confidences,
        )

    def get_runtime_breakdown(self):
        """Return the runtime breakdown of the slide."""

        assert not self.error, "Cannot get runtime breakdown for error directories."

        runtime_data_path = self.remote_result_dir / "runtime_data.csv"

        with self.sftp_client.open(str(runtime_data_path), "r") as f:
            runtime_data_dict = self.csv_to_dict(f)

        return runtime_data_dict

    def _convert_size(self, size_bytes):
        """Convert bytes to a more human-readable format (KB, MB, GB, etc.)."""
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"

    def get_storage_consumption_breakdown(self):
        """Return the storage consumption breakdown of the slide result folder."""
        breakdown = {}
        for attr in self.sftp_client.listdir_attr(str(self.remote_result_dir)):
            if not attr.filename.startswith("."):
                extension = os.path.splitext(attr.filename)[1].lower()
                if extension not in breakdown:
                    breakdown[extension] = 0
                breakdown[extension] += attr.st_size

        breakdown_readable = {
            ext: self._convert_size(size) for ext, size in breakdown.items()
        }

        return breakdown_readable

    def get_run_history(self):
        """Return the run history of the slide."""

        with self.sftp_client.open(str(pipeline_run_history_path), "r") as f:
            run_history = pd.read_csv(f)

        row = run_history[
            (run_history["pipeline"] == self.pipeline)
            & (run_history["datetime_processed"] == self.datetime_processed)
        ]

        assert (
            len(row) >= 1
        ), f"Row not found in run_history for {self.pipeline} and {self.datetime_processed}"
        assert (
            len(row) == 1
        ), f"Multiple rows found in run_history for {self.pipeline} and {self.datetime_processed}. This should not happen."

        run_history_dict = row.to_dict(orient="records")[0]

        return run_history_dict

    def get_wsi_name(self):
        """Return the wsi_name of the slide."""
        run_history_dict = self.get_run_history()
        return run_history_dict["wsi_name"]

    def get_slide_metadata_dict(self):
        """Return the slide metadata dictionary."""
        wsi_name = self.get_wsi_name()

        with self.sftp_client.open(str(slide_metadata_path), "r") as f:
            slide_metadata = pd.read_csv(f)

        row = slide_metadata[slide_metadata["wsi_name"] == wsi_name]

        assert len(row) >= 1, f"Row not found in slide_metadata for {wsi_name}"
        assert (
            len(row) == 1
        ), f"Multiple rows found in slide_metadata for {wsi_name}. This should not happen."

        slide_metadata_dict = row.to_dict(orient="records")[0]

        return slide_metadata_dict

    def get_datetime_processed(self):
        return self.datetime_processed

    def get_pipeline(self):
        return self.pipeline

    def get_grid_rep(self):
        """Return the grid rep image of the slide.
        Which is located at the directory/top_view_grid_rep.png.
        Use PIL
        """

        assert not self.error, "Cannot get grid rep for error directories."

        grid_rep_path = self.remote_result_dir / "top_view_grid_rep.png"

        with self.sftp_client.open(str(grid_rep_path), "rb") as f:
            return Image.open(BytesIO(f.read()))

    def get_part_description(self):
        """Return the part_description of the slide."""
        part_desc = self.get_slide_metadata_dict().get(
            "part_description", "Missing part description"
        )

        if pd.isna(part_desc) or part_desc.isspace() or not part_desc:
            return "Missing part description"

        return part_desc

    def get_Dx_and_sub_Dx(self):
        """Return the Dx and sub_Dx of the slide."""
        Dx, sub_Dx = (
            self.get_slide_metadata_dict().get("Dx", "Missing Dx"),
            self.get_slide_metadata_dict().get("sub_Dx", "Missing sub_Dx"),
        )

        if pd.isna(Dx) or Dx.isspace() or not Dx:
            Dx = "Missing Dx"

        if pd.isna(sub_Dx) or sub_Dx.isspace() or not sub_Dx:
            sub_Dx = "Missing sub_Dx"

        return Dx, sub_Dx

    def get_reported_differential(self):
        """Use the bma_diff_metadata.csv file to get the reported differential of the slide."""

        diff_dict = {
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
        }

        # Assuming bma_diff_metadata_path is defined or passed as an argument
        with self.sftp_client.open(bma_diff_metadata_path, "r") as f:
            bma_diff_metadata = pd.read_csv(f)

        # Get the row in the bma_diff_metadata dataframe that corresponds to the wsi_name
        row = bma_diff_metadata[bma_diff_metadata["wsi_name"] == self.get_wsi_name()]

        if len(row) == 0:
            print(
                "User Warning: No row found in bma_diff_metadata for this slide. Returning None for all cell types. However, this means that the slide differential has not been pulled from the database. This behaviour could indicate a problem in the data pipeline."
            )
            return diff_dict

        assert (
            len(row) == 1
        ), f"Multiple rows found in bma_diff_metadata for {self.get_wsi_name()}. This should never happen and the pipeline log could be corrupted."

        # Extracting values from the DataFrame
        diff_dict = {
            "blasts": row["blasts"].iloc[0],
            "blast-equivalents": row["blast-equivalents"].iloc[0],
            "promyelocytes": row["promyelocytes"].iloc[0],
            "myelocytes": row["myelocytes"].iloc[0],
            "metamyelocytes": row["metamyelocytes"].iloc[0],
            "neutrophils/bands": row["neutrophils/bands"].iloc[0],
            "monocytes": row["monocytes"].iloc[0],
            "eosinophils": row["eosinophils"].iloc[0],
            "erythroid precursors": row["erythroid precursors"].iloc[0],
            "lymphocytes": row["lymphocytes"].iloc[0],
            "plasma cells": row["plasma cells"].iloc[0],
        }

        return diff_dict

    def csv_to_dict(self, file_obj):
        """Convert a CSV file into a dictionary."""
        result_dict = {}
        reader = pd.read_csv(file_obj)
        for _, row in reader.iterrows():
            key = row.iloc[0]
            value = float(row.iloc[1])
            result_dict[key] = value
        return result_dict

    def get_num_regions(self):
        """Count how many region jpg images are in the high_mag_unannotated folder."""

        assert not self.error, "Cannot get num regions for error directories."

        high_mag_unannotated_dir = str(
            self.remote_result_dir / "focus_regions" / "high_mag_unannotated"
        )

        try:
            # List files in the directory using SFTP
            files = self.sftp_client.listdir(high_mag_unannotated_dir)
            # Filter out only .jpg files
            jpg_files = [file for file in files if file.endswith(".jpg")]
            return len(jpg_files)
        except FileNotFoundError:
            return 0  # If the directory does not exist, return 0

    def get_num_cells(self):
        """Recursively count how many jpg files are in the cell`s folder and its subfolders."""

        assert not self.error, "Cannot get num cells for error directories."

        cells_dir = str(self.remote_result_dir / "cells")

        num_cells = 0
        try:
            # Recursively list all files in the cells directory
            for dirpath, dirnames, filenames in self.sftp_client.walk(cells_dir):
                num_cells += len([file for file in filenames if file.endswith(".jpg")])
        except FileNotFoundError:
            return 0  # If the directory does not exist, return 0

        return num_cells

    def get_note(self):
        """Return the note from the run history."""

        return self.get_run_history()["note"]

    def __del__(self):
        """Cleanup the SSH and SFTP connections."""
        if hasattr(self, "sftp_client") and self.sftp_client:
            self.sftp_client.close()
        if hasattr(self, "ssh_client") and self.ssh_client:
            self.ssh_client.close()


if __name__ == "__main__":

    ##########################################
    # TESTING LOCAL VERSION
    ##########################################

    slide_result_path = "/media/hdd3/neo/results_dir/BMA-diff_2024-07-25 08:22:42"

    bma_result = BMAResult(slide_result_path)

    print(f"Slide result path: {bma_result.result_dir}")
    print(f"Pipeline: {bma_result.pipeline}")
    print(f"Datetime processed: {bma_result.datetime_processed}")
    print(f"Error: {bma_result.has_error()}")

    # now all the methods should work
    print(f"Stacked differential: {bma_result.get_stacked_differential()}")
    print(f"One hot differential: {bma_result.get_one_hot_differential()}")
    print(f"Grouped differential: {bma_result.get_grouped_differential()}")
    print(
        f"Grouped stacked differential: {bma_result.get_grouped_stacked_differential()}"
    )
    print(f"Raw counts: {bma_result.get_raw_counts()}")
    print(f"Grouped raw counts: {bma_result.get_grouped_raw_counts()}")

    # now print the images
    grid_rep = bma_result.get_grid_rep()
    confidence_heatmap = bma_result.get_confidence_heatmap()
    (
        focus_regions_images,
        focus_regions_annotated_images,
        region_idxs,
        low_mag_confidences,
        high_mag_confidences,
    ) = bma_result.get_focus_regions()

    print(f"Grid rep: {grid_rep}")
    print(f"Confidence heatmap: {confidence_heatmap}")
    for (
        top_region_image,
        top_region_annotated_image,
        region_idx,
        low_mag_confidence,
        high_mag_confidence,
    ) in zip(
        focus_regions_images,
        focus_regions_annotated_images,
        region_idxs,
        low_mag_confidences,
        high_mag_confidences,
    ):
        print(f"Top region image: {top_region_image}")
        print(f"Top region annotated image: {top_region_annotated_image}")
        print(f"Region idx: {region_idx}")
        print(f"Low mag confidence: {low_mag_confidence}")
        print(f"High mag confidence: {high_mag_confidence}")

    # now print the breakdowns
    runtime_breakdown = bma_result.get_runtime_breakdown()
    storage_consumption_breakdown = bma_result.get_storage_consumption_breakdown()
    run_history = bma_result.get_run_history()
    wsi_name = bma_result.get_wsi_name()

    print(f"Runtime breakdown: {runtime_breakdown}")
    print(f"Storage consumption breakdown: {storage_consumption_breakdown}")
    print(f"Run history: {run_history}")
    print(f"WSI name: {wsi_name}")

    # now print the metadata
    slide_metadata_dict = bma_result.get_slide_metadata_dict()
    part_description = bma_result.get_part_description()
    Dx, sub_Dx = bma_result.get_Dx_and_sub_Dx()

    print(f"Slide metadata dict: {slide_metadata_dict}")
    print(f"Part description: {part_description}")
    print(f"Dx: {Dx}")
    print(f"Sub Dx: {sub_Dx}")

    # now print the reported differential
    reported_differential = bma_result.get_reported_differential()

    print(f"Reported differential: {reported_differential}")

    ##########################################
    # TESTING SSH VERSION
    ##########################################

    hostname = "172.28.164.44"
    username = "greg"
    remote_result_dir = "/media/hdd3/neo/results_dir/BMA-diff_2024-07-31 23:06:08"

    bma_result = BMAResultSSH(
        hostname=hostname,
        username=username,
        remote_result_dir=remote_result_dir,
        max_retries=5,  # Optional: set the max retries for rsync, defaults to 3
        backoff_factor=2,  # Optional: set the backoff factor for rsync, defaults to 2
    )

    # Print basic information
    print(f"Slide result path: {bma_result.remote_result_dir}")
    print(f"Pipeline: {bma_result.pipeline}")
    print(f"Datetime processed: {bma_result.datetime_processed}")
    print(f"Error: {bma_result.error}")

    # Use the methods to get results
    print(f"Stacked differential: {bma_result.get_stacked_differential()}")
    print(f"One hot differential: {bma_result.get_one_hot_differential()}")
    print(f"Grouped differential: {bma_result.get_grouped_differential()}")
    print(
        f"Grouped stacked differential: {bma_result.get_grouped_stacked_differential()}"
    )
    print(f"Raw counts: {bma_result.get_raw_counts()}")
    print(f"Grouped raw counts: {bma_result.get_grouped_raw_counts()}")

    # Print the images
    grid_rep = bma_result.get_grid_rep()
    confidence_heatmap = bma_result.get_confidence_heatmap()
    (
        focus_regions_images,
        focus_regions_annotated_images,
        region_idxs,
        low_mag_confidences,
        high_mag_confidences,
    ) = bma_result.get_focus_regions()

    print(f"Grid rep: {grid_rep}")
    print(f"Confidence heatmap: {confidence_heatmap}")

    for (
        top_region_image,
        top_region_annotated_image,
        region_idx,
        low_mag_confidence,
        high_mag_confidence,
    ) in zip(
        focus_regions_images,
        focus_regions_annotated_images,
        region_idxs,
        low_mag_confidences,
        high_mag_confidences,
    ):
        print(f"Top region image: {top_region_image}")

        print(f"Top region annotated image: {top_region_annotated_image}")

        print(f"Region idx: {region_idx}")
        print(f"Low mag confidence: {low_mag_confidence}")
        print(f"High mag confidence: {high_mag_confidence}")

    # Print the breakdowns
    runtime_breakdown = bma_result.get_runtime_breakdown()
    storage_consumption_breakdown = bma_result.get_storage_consumption_breakdown()
    # run_history = bma_result.get_run_history()  # Assuming this method is implemented

    print(f"Runtime breakdown: {runtime_breakdown}")
    print(f"Storage consumption breakdown: {storage_consumption_breakdown}")
    # print(f"Run history: {run_history}")  # Uncomment if the method is implemented

    # Print metadata information
    # wsi_name = bma_result.get_wsi_name()  # Assuming this method is implemented
    slide_metadata_dict = bma_result.get_slide_metadata_dict()
    part_description = bma_result.get_part_description()
    Dx, sub_Dx = bma_result.get_Dx_and_sub_Dx()

    # print(f"WSI name: {wsi_name}")  # Uncomment if the method is implemented
    print(f"Slide metadata dict: {slide_metadata_dict}")
    print(f"Part description: {part_description}")
    print(f"Dx: {Dx}")
    print(f"Sub Dx: {sub_Dx}")

    # Get the reported differential
    reported_differential = bma_result.get_reported_differential()

    print(f"Reported differential: {reported_differential}")
