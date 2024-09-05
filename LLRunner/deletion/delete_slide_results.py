import pandas as pd
import os
import shutil
from LLRunner.config import results_dir, pipeline_run_history_path


def delete_results_from_note(note, ask_for_confirmation=True):
    """Delete the results for the specified note."""

    # get the pipeline run history dataframe
    df = pd.read_csv(pipeline_run_history_path)

    # filter the rows with the specified note
    rows_to_delete = df[df["note"] == note]
    rows_to_keep = df[df["note"] != note]

    # get the list of names of the result dir to delete which is pipeline underscore datetime_processed
    result_dirs_to_delete = []

    for i, row in rows_to_delete.iterrows():
        pipeline = row["pipeline"]
        datetime_processed = row["datetime_processed"]

        result_dir = f"{pipeline}_{datetime_processed}"

        result_dirs_to_delete.append(result_dir)

    if not ask_for_confirmation:
        # delete the result directories
        for result_dir in result_dirs_to_delete:
            shutil.rmtree(os.path.join(results_dir, result_dir))

        # write the new pipeline run history dataframe
        rows_to_keep.to_csv(pipeline_run_history_path, index=False)
        print(
            f"Results for note {note} deleted. Number of results deleted: {len(result_dirs_to_delete)}"
        )
    else:
        # default is not to delete if the user fail to either enter y, Y, yes or Yes
        user_confirmation = input(
            f"Are you sure you want to delete the results for note {note}? (y/[n]): "
        )

        if user_confirmation in ["y", "Y", "yes", "Yes"]:
            # delete the result directories
            for result_dir in result_dirs_to_delete:
                shutil.rmtree(os.path.join(results_dir, result_dir))

            # write the new pipeline run history dataframe
            rows_to_keep.to_csv(pipeline_run_history_path, index=False)
            print(
                f"Results for note {note} deleted. Number of results deleted: {len(result_dirs_to_delete)}"
            )
        else:
            print("Results not deleted.")
