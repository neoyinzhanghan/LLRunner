import os
import datetime
import pandas as pd
from LLBMA.front_end.api import analyse_bma
from LLRunner.slide_transfer.slides_management import copy_slide_to_tmp
from LLRunner.config import tmp_slide_dir, pipeline_run_history_path, available_pipelines, results_dir

def find_slide(wsi_name, copy_slide=False):
    """ Find the slide with the specified name. 
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

def run_one_slide(wsi_name, pipeline, note="", **kwargs):
    """ Run the specified pipeline for one slide. 
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
            "note": note
        }
        if pipeline == "BMA-diff":
            slide_path = find_slide(wsi_name, copy_slide=True)
            result_dir, error = analyse_bma(slide_path,
                                            dump_dir=results_dir, # then just kwargs
                                            **kwargs)
        else:
            raise PipelineNotFoundError(pipeline) # more coming soon!            
        
        metadata_row_dct["datetime_processed"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # the new result_dir is the results_dir/newname where newname is the pipeline followed by datatime_processed
        new_result_dir = os.path.join(results_dir, f"{pipeline}_{metadata_row_dct['datetime_processed']}")
        os.rename(result_dir, new_result_dir)

        metadata_row_dct["result_dir"] = new_result_dir
        metadata_row_dct["error"] = error

        new_df_row = pd.DataFrame([metadata_row_dct])
        df = pd.read_csv(pipeline_run_history_path)

        df = pd.concat([df, new_df_row], ignore_index=True)
        df.to_csv(pipeline_run_history_path, index=False)

# the SlideNotFoundInTmpSlideDirError
class SlideNotFoundInTmpSlideDirError(Exception):
    def __init__(self, wsi_name="unspecified"):
        self.wsi_name = wsi_name
        self.message = f"Slide {wsi_name} not found in the tmp_slide_dir."
        super().__init__(self.message)

    def __str__(self):
        return self.message
    
# the PipelineNotFoundError
class PipelineNotFoundError(Exception):
    def __init__(self, pipeline="unspecified"):
        self.pipeline = pipeline
        self.message = f"Pipeline {pipeline} not found in the available pipelines."
        super().__init__(self.message)

    def __str__(self):
        return self.message