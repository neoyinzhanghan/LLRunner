import os
import random
import csv
import pandas as pd
from pathlib import Path
from PIL import Image
from LLRunner.read.read_config import *
from LLRunner.config import pipeline_run_history_path, slide_metadata_path


def csv_to_dict(file_path):
    result_dict = {}
    with open(file_path, mode="r") as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            key = row[0]
            value = float(row[1])
            result_dict[key] = value
    return result_dict


def has_error(result_dir):
    """Check if there results directory corresponds to an error run."""

    # result_dir is a string, make it a Path object
    result_dir_Path = Path(result_dir)

    # first split the result_dir into its components by _, first part is the pipeline and the second part is the datetime_processed
    pipeline, datetime_processed = result_dir_Path.name.split("_", 1)

    # read the pipeline_run_history.csv file
    pipeline_run_history = pd.read_csv(pipeline_run_history_path)

    # look for the row in the pipeline_run_history dataframe that corresponds to the pipeline and datetime_processed
    row = pipeline_run_history[
        (pipeline_run_history["pipeline"] == pipeline)
        & (pipeline_run_history["datetime_processed"] == datetime_processed)
    ]

    # if the row does not exist, then raise an error using assert statement (there should be only one row)
    assert (
        len(row) >= 1
    ), f"Row not found in pipeline_run_history for {pipeline} and {datetime_processed}"

    assert (
        len(row) == 1
    ), f"Multiple rows found in pipeline_run_history for {pipeline} and {datetime_processed}. This should never happen and the pipeline log could be corrupted."

    # get the error column from the row
    error = row["error"].values[0]

    # return the error value as boolean
    return bool(error)


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
        self.error = has_error(self.result_folder_name)

        self.result_dir = result_dir
        self.cell_info = pd.read_csv(result_dir / "cells" / "cells_info.csv")

    def get_stacked_differential(self):
        """In the cell_info dataframe there are columns corresponding to each cell type in the list cellnames.
        Take the average of the probabilities for each cell type and return a dictionary with the cell type as the key and the average probability as the value.
        """

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

        grid_rep_path = self.result_dir / "top_view_grid_rep.png"

        return Image.open(grid_rep_path)

    def get_confidence_heatmap(self):
        """Return the confidence heatmap image of the slide.
        Which is located at the directory/confidence_heatmap.png.
        Use PIL
        """

        confidence_heatmap_path = self.result_dir / "confidence_heatmap.png"

        return Image.open(confidence_heatmap_path)

    def has_error(self):
        return self.error

    def get_region_confidence(self, region_idx):
        """Use the focus_regions/focus_regions_info.csv file and the focus_regions/high_mag_focus_regions_info.csv file to get the confidence of the region with region_idx."""

        # read the focus_regions_info.csv file
        focus_regions_info = pd.read_csv(
            self.result_dir / "focus_regions" / "focus_regions_info.csv"
        )

        # get the row in the focus_regions_info dataframe that corresponds to the region_idx
        row = focus_regions_info[focus_regions_info["idx"] == region_idx]

        assert (
            len(row) >= 1
        ), f"Region with idx {region_idx} not found in focus_regions_info.csv"
        assert (
            len(row) == 1
        ), f"Multiple regions with idx {region_idx} found in focus_regions_info.csv This should never happen and the pipeline log could be corrupted."

        # get the confidence of the region
        low_mag_confidence = row["confidence_score"].values[0]

        # now do the samething with the high_mag_focus_regions_info.csv file
        high_mag_focus_regions_info = pd.read_csv(
            self.result_dir / "focus_regions" / "high_mag_focus_regions_info.csv"
        )

        row = high_mag_focus_regions_info[
            high_mag_focus_regions_info["idx"] == region_idx
        ]

        # if there is no row found, then just None
        if len(row) == 0:
            return low_mag_confidence, None

        assert (
            len(row) == 1
        ), f"Multiple regions with idx {region_idx} found in high_mag_focus_regions_info.csv This should never happen and the pipeline log could be corrupted."

        high_mag_confidence = row["confidence_score_high_mag"].values[0]

        return low_mag_confidence, high_mag_confidence

    def get_focus_regions(self, num_to_sample=5):
        """Return the top regions image of the slide.
        Which is located at the directory/top_regions.png.
        Use PIL
        """

        # get the list of all the top regions images which are stored in dir/top_regions/high_mag_unannotated
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
        region_idxs = [file.stem for file in focus_regions_images]

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

        # the runtime_data_path is dir/runtime_data.csv
        runtime_data_path = self.result_dir / "runtime_data.csv"

        # read the runtime_data.csv file
        runtime_data_dict = csv_to_dict(runtime_data_path)

        return runtime_data_dict

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
        Which is located at the directory/run_history.csv.
        """

        run_history = pd.read_csv(self.result_dir / "run_history.csv")

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

    def get_stored_differential(self):  # TODO We will do this manually for now
        pass  # TODO, we need to check the integrity of the stored differential and make sure that it actually matches with the differential computed here.

if __name__ == "__main__":
    slide_result_path = "/media/hdd3/neo/results_dir/BMA-diff_2024-08-03 21:15:14"

    bma_result = BMAResult(slide_result_path)

    print(f"Slide result path: {bma_result.result_dir}")
    print(f"Pipeline: {bma_result.pipeline}")
    print(f"Datetime processed: {bma_result.datetime_processed}")
    print(f"Error: {bma_result.has_error()}")
    
    # now all the methods should work
    print(f"Stacked differential: {bma_result.get_stacked_differential()}")
    print(f"One hot differential: {bma_result.get_one_hot_differential()}")
    print(f"Grouped differential: {bma_result.get_grouped_differential()}")
    print(f"Grouped stacked differential: {bma_result.get_grouped_stacked_differential()}")
    print(f"Raw counts: {bma_result.get_raw_counts()}")
    print(f"Grouped raw counts: {bma_result.get_grouped_raw_counts()}")
    
    # now print the images
    grid_rep = bma_result.get_grid_rep()
    confidence_heatmap = bma_result.get_confidence_heatmap()
    top_regions_images, top_regions_annotated_images, region_idxs, low_mag_confidences, high_mag_confidences = bma_result.get_top_regions()

    print(f"Grid rep: {grid_rep}")
    print(f"Confidence heatmap: {confidence_heatmap}")
    for top_region_image, top_region_annotated_image, region_idx, low_mag_confidence, high_mag_confidence in zip(top_regions_images, top_regions_annotated_images, region_idxs, low_mag_confidences, high_mag_confidences):
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



