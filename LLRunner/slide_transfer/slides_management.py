import os
import shutil
import datetime
import pandas as pd
import openslide
from LLRunner.slide_transfer.metadata_management import (
    get_slide_metadata_row,
    update_slide_metadata,
    get_bma_diff_metadata_row,
    update_bma_diff_metadata,
)
from LLRunner.config import (
    allowed_extensions,
    slide_source_dir,
    tmp_slide_dir,
    slide_metadata_path,
    topview_level,
)
from LLRunner.slide_transfer.sshos import SSHOS


def get_topview(wsi_name):
    """Get the topview of the slide."""

    slide_path = os.path.join(tmp_slide_dir, wsi_name)

    assert os.path.exists(
        slide_path
    ), f"Slide {wsi_name} not found in the tmp_slide_dir."

    slide = openslide.OpenSlide(slide_path)
    topview = slide.read_region(
        (0, 0), topview_level, slide.level_dimensions[topview_level]
    )

    # if the image is not in RGB mode, then convert it to RGB mode
    if topview.mode != "RGB":
        topview = topview.convert("RGB")

    return topview


def get_level_0_mpp(wsi_name):
    """Get the mpp of the level 0 of the slide."""

    slide_path = os.path.join(tmp_slide_dir, wsi_name)

    assert os.path.exists(
        slide_path
    ), f"Slide {wsi_name} not found in the tmp_slide_dir."

    slide = openslide.OpenSlide(slide_path)

    # assert that the slide has the property PROPERTY_NAME_MPP_X, and PROPERTY_NAME_MPP_Y and they are equal
    assert (
        openslide.PROPERTY_NAME_MPP_X in slide.properties
    ), f"Slide {wsi_name} does not have the property {openslide.PROPERTY_NAME_MPP_X}."
    level_0_mpp = slide.properties[openslide.PROPERTY_NAME_MPP_X]

    return level_0_mpp


def check_extension(wsi_name):
    """Check if the slide has the correct extension.
    If the slide does not have the correct extension, then raise an error.
    """

    # get the extension of the slide which is split at . and take -1
    extension = wsi_name.split(".")[-1]

    # check if the extension is in the allowed extensions
    if extension not in allowed_extensions:
        raise IncorrectSlideExtensionError(wsi_name, extension)

    return extension


def copy_slide_to_tmp(wsi_name, overwrite=False, overwrite_topview=False):
    """Make a copy of the slide to the tmp_slide_dir,
    If the slide is already in the tmp_slide_dir, then do nothing.
    Unless overwrite is set to True, then overwrite the slide in the tmp_slide_dir.
    Also update the slide metadata if necessary"""

    # check if the slide is in the tmp_slide_dir
    slide_path = os.path.join(tmp_slide_dir, wsi_name)

    if os.path.exists(slide_path):
        if overwrite:
            os.remove(slide_path)

            with SSHOS() as sshos:
                source_slide_path = os.path.join(slide_source_dir, wsi_name)
                sshos.rsync_file(remote_path=source_slide_path, local_dir=tmp_slide_dir)

            # update the slide metadata
            slide_metadata_row = get_slide_metadata_row(wsi_name)

            slide_metadata_row["slide_last_updated"] = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            slide_metadata_row["in_tmp_slide_dir"] = True

            try:
                topview_image = get_topview(wsi_name)
                # save the topview image in the tmp_slide_dir/topview
                topview_path = os.path.join(
                    tmp_slide_dir,
                    "topview",
                    wsi_name.replace(slide_metadata_row["wsi_extension"], ".jpg"),
                )
                topview_image.save(topview_path)
            except Exception as e:
                slide_metadata_row["topview_error"] = True

            try:
                level_0_mpp = get_level_0_mpp(wsi_name)
                slide_metadata_row["level_0_mpp"] = level_0_mpp
                # save the topview image in the tmp_slide_dir/topview
                topview_path = os.path.join(
                    tmp_slide_dir,
                    "topview",
                    wsi_name.replace(slide_metadata_row["wsi_extension"], ".jpg"),
                )
                topview_image.save(topview_path)
            except Exception as e:
                slide_metadata_row["level_0_mpp_error"] = True

            update_slide_metadata(
                metadata_row_dct=slide_metadata_row, overwrite=overwrite
            )

        elif overwrite_topview:
            # update the slide metadata
            slide_metadata_row = get_slide_metadata_row(wsi_name)

            slide_metadata_row["slide_last_updated"] = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            slide_metadata_row["in_tmp_slide_dir"] = True

            try:
                topview_image = get_topview(wsi_name)
                # save the topview image in the tmp_slide_dir/topview
                topview_path = os.path.join(
                    tmp_slide_dir,
                    "topview",
                    wsi_name.replace(slide_metadata_row["wsi_extension"], ".jpg"),
                )
                topview_image.save(topview_path)
            except Exception as e:
                slide_metadata_row["topview_error"] = True

            try:
                level_0_mpp = get_level_0_mpp(wsi_name)
                slide_metadata_row["level_0_mpp"] = level_0_mpp
                # save the topview image in the tmp_slide_dir/topview
                topview_path = os.path.join(
                    tmp_slide_dir,
                    "topview",
                    wsi_name.replace(slide_metadata_row["wsi_extension"], ".jpg"),
                )
                topview_image.save(topview_path)
            except Exception as e:
                slide_metadata_row["level_0_mpp_error"] = True

            update_slide_metadata(
                metadata_row_dct=slide_metadata_row, overwrite=overwrite
            )

    else:
        with SSHOS() as sshos:
            source_slide_path = os.path.join(slide_source_dir, wsi_name)
            sshos.rsync_file(remote_path=source_slide_path, local_dir=tmp_slide_dir)
        # update the slide metadata
        slide_metadata_row = get_slide_metadata_row(wsi_name)

        slide_metadata_row["slide_last_updated"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        slide_metadata_row["in_tmp_slide_dir"] = True

        try:
            topview_image = get_topview(wsi_name)
            # save the topview image in the tmp_slide_dir/topview
            topview_path = os.path.join(
                tmp_slide_dir,
                "topview",
                wsi_name.replace(slide_metadata_row["wsi_extension"], ".jpg"),
            )
            topview_image.save(topview_path)
        except Exception as e:
            slide_metadata_row["topview_error"] = True

        try:
            level_0_mpp = get_level_0_mpp(wsi_name)
            slide_metadata_row["level_0_mpp"] = level_0_mpp
            # save the topview image in the tmp_slide_dir/topview
            topview_path = os.path.join(
                tmp_slide_dir,
                "topview",
                wsi_name.replace(slide_metadata_row["wsi_extension"], ".jpg"),
            )
            topview_image.save(topview_path)
        except Exception as e:
            slide_metadata_row["level_0_mpp_error"] = True

        update_slide_metadata(metadata_row_dct=slide_metadata_row, overwrite=overwrite)


def delete_slide_from_tmp(wsi_name):
    """Delete the slide from the tmp_slide_dir and update the slide metadata."""

    # check if the slide is in the tmp_slide_dir
    slide_path = os.path.join(tmp_slide_dir, wsi_name)

    if os.path.exists(slide_path):
        os.remove(slide_path)

        slide_md = pd.read_csv(slide_metadata_path)

        # find the row of the slide in the slide metadata
        slide_md_row = slide_md.loc[slide_md["wsi_name"] == wsi_name]

        # update the slide metadata
        slide_md_row.iloc[:, slide_md.columns.get_loc("in_tmp_slide_dir")] = False

        slide_md.to_csv(slide_metadata_path, index=False)

    else:
        print("UserWarning: Slide not found in the tmp_slide_dir.")
        pass


def has_topview(wsi_name):
    """Replace the extension with .jpg and check if the topview exists in the tmp_slide_dir/topview."""

    topview_path = os.path.join(
        tmp_slide_dir, "topview", wsi_name.replace(check_extension(wsi_name), ".jpg")
    )

    return os.path.exists(topview_path)


class IncorrectSlideExtensionError(Exception):
    def __init__(self, wsi_name, extension):
        self.wsi_name = wsi_name
        self.extension = extension
        self.message = f"Slide {wsi_name} has an incorrect extension: {extension}. Allowed extensions are {allowed_extensions}."
        super().__init__(self.message)

    def __str__(self):
        return self.message
