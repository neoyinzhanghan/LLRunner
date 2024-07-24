import os

from LLRunner.slide_transfer.slides_management import copy_slide_to_tmp
from LLRunner.config import tmp_slide_dir, slide_source_dir

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

def run_one_slide(wsi_name, pipeline, tmp_slide_mode=0):
    """ Run the specified pipeline for one slide. 
    
    """

    pass


# the SlideNotFoundInTmpSlideDirError
class SlideNotFoundInTmpSlideDirError(Exception):
    def __init__(self, wsi_name="unspecified"):
        self.wsi_name = wsi_name
        self.message = f"Slide {wsi_name} not found in the tmp_slide_dir."
        super().__init__(self.message)

    def __str__(self):
        return self.message