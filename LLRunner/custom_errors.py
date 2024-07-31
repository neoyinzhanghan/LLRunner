class SlideNotFoundError(Exception):
    """
    Raised when the slide is not found in the status results.
    """

    def __init__(self, slide_name: str) -> None:
        self.slide_name = slide_name
        super().__init__(f"Slide {slide_name} not found in the status results.")


class AccessionNumberNotFoundError(Exception):
    """
    Raised when the accession number is not found in the slide scanning tracker.
    """

    def __init__(self, accession_number: str) -> None:
        self.accession_number = accession_number
        super().__init__(
            f"Accession Number {accession_number} not found in the slide scanning tracker."
        )


# the SlideNotFoundInTmpSlideDirError
class SlideNotFoundInTmpSlideDirError():
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
