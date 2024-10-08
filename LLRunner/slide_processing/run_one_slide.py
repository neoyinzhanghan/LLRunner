import os
import datetime
import pandas as pd
from PIL import Image
from pathlib import Path
from LLRunner.slide_processing.specimen_clf import (
    update_bma_specimen_clf_result,
    update_pbs_specimen_clf_result,
)
from LLBMA.front_end.api import analyse_bma
from LLPBS.front_end.api import analyse_pbs
from LLRunner.slide_transfer.slides_management import (
    copy_slide_to_tmp,
    delete_slide_from_tmp,
)
from LLRunner.config import (
    tmp_slide_dir,
    pipeline_run_history_path,
    available_pipelines,
    results_dir,
    slide_metadata_path,
)
from LLRunner.custom_errors import (
    SlideNotFoundInTmpSlideDirError,
    PipelineNotFoundError,
)


def find_slide(wsi_name, copy_slide=False):
    """Find the slide with the specified name.
    First look for wsi_name (this name includes extension) in the tmp_slide_dir.
    If not found then we need to decide what to do based on the copy_slide flag:

    False: raise SlideNotFoundInTmpSlideDirError(wsi_name)
    True: copy the slide from the source slide directory to the tmp_slide_dir, update the slide metadata, and return the path to the slide in the tmp_slide_dir
    """

    # check if the slide is in the tmp_slide_dir
    slide_path = os.path.join(tmp_slide_dir, wsi_name)

    if os.path.exists(slide_path):
        return slide_path
    # if the slide is not in the tmp_slide_dir
    else:
        if copy_slide:
            # copy the slide from the source slide directory to the tmp_slide_dir
            copy_slide_to_tmp(wsi_name, overwrite=False)
            return slide_path
        else:
            raise SlideNotFoundInTmpSlideDirError(wsi_name)


def run_one_slide(wsi_name, pipeline, delete_slide=False, note="", **kwargs):
    """Run the specified pipeline for one slide.
    The pipeline running code here should be minimal directly through the pipeline api.
    """

    if pipeline not in available_pipelines:
        raise PipelineNotFoundError(pipeline)
    else:
        metadata_row_dct = {
            "wsi_name": wsi_name,
            "pipeline": pipeline,
            "datetime_processed": None,
            "result_dir": None,
            "error": False,
            "note": note,
            "kwargs": str(kwargs),
        }
        if pipeline == "BMA-diff":
            slide_path = find_slide(wsi_name, copy_slide=True)
            result_dir, error = analyse_bma(
                slide_path, dump_dir=results_dir, **kwargs  # then just kwargs
            )

        elif pipeline == "PBS-diff":
            slide_path = find_slide(wsi_name, copy_slide=True)
            result_dir, error = analyse_pbs(
                slide_path, dump_dir=results_dir, **kwargs  # then just kwargs
            )

        metadata_row_dct["datetime_processed"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # the new result_dir is the results_dir/newname where newname is the pipeline followed by datatime_processed
        new_result_dir = os.path.join(
            results_dir, f"{pipeline}_{metadata_row_dct['datetime_processed']}"
        )
        os.rename(result_dir, new_result_dir)

        metadata_row_dct["result_dir"] = new_result_dir
        metadata_row_dct["error"] = error

        new_df_row = pd.DataFrame([metadata_row_dct])
        df = pd.read_csv(pipeline_run_history_path)

        df = pd.concat([df, new_df_row], ignore_index=True)
        df.to_csv(pipeline_run_history_path, index=False)

        if delete_slide:
            delete_slide_from_tmp(wsi_name)


def run_one_slide_with_specimen_clf(
    wsi_name, delete_slide=False, note="", **kwargs
):  # TODO: add the specimen clf stuff
    """Run the specified pipeline for one slide.
    The pipeline running code here should be minimal directly through the pipeline api.
    """

    # now check and update the specimen classification for bma
    bma_specimen_clf_score, bma_specimen_clf_error = update_bma_specimen_clf_result(
        wsi_name
    )

    pbs_specimen_clf_score, pbs_specimen_clf_error = update_pbs_specimen_clf_result(
        wsi_name
    )

    pipeline = None

    if (bma_specimen_clf_score is None or bma_specimen_clf_error is not None) or (
        pbs_specimen_clf_score is None or pbs_specimen_clf_error is not None
    ):
        print(
            f"Skipping {wsi_name} because error in specimen clf: {bma_specimen_clf_error}"
        )
        pipeline = None

    elif bma_specimen_clf_score >= 0.5:
        pipeline = "BMA-diff"
    elif pbs_specimen_clf_score >= 0.5:
        pipeline = "PBS-diff"
    else:
        pipeline = None

    metadata_row_dct = {
        "wsi_name": wsi_name,
        "pipeline": pipeline,
        "datetime_processed": None,
        "result_dir": None,
        "error": False,
        "note": note,
        "kwargs": str(kwargs),
    }
    if pipeline == "BMA-diff":
        slide_path = find_slide(wsi_name, copy_slide=True)

        result_dir, error = analyse_bma(
            slide_path, dump_dir=results_dir, **kwargs  # then just kwargs
        )

        ran = True

    elif pipeline == "PBS-diff":
        slide_path = find_slide(wsi_name, copy_slide=True)

        result_dir, error = analyse_pbs(
            slide_path, dump_dir=results_dir, **kwargs  # then just kwargs
        )

        ran = True

    else:
        ran = False

    if ran:
        metadata_row_dct["datetime_processed"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # the new result_dir is the results_dir/newname where newname is the pipeline followed by datatime_processed
        new_result_dir = os.path.join(
            results_dir, f"{pipeline}_{metadata_row_dct['datetime_processed']}"
        )
        os.rename(result_dir, new_result_dir)

        metadata_row_dct["result_dir"] = new_result_dir
        metadata_row_dct["error"] = error

        new_df_row = pd.DataFrame([metadata_row_dct])
        df = pd.read_csv(pipeline_run_history_path)

        df = pd.concat([df, new_df_row], ignore_index=True)
        df.to_csv(pipeline_run_history_path, index=False)

    if delete_slide:
        delete_slide_from_tmp(wsi_name)
