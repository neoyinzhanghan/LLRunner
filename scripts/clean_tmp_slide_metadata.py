from LLRunner.config import slide_metadata_path


def clean_tmp_slide_metadata():
    """
    Remove all the files in the tmp_slide_metadata directory.
    """
    # check the csv file to look for lines with number of fields longer than 17
    # if found, remove that line from the file

    num_corrupted_lines = 0

    with open(slide_metadata_path, "r") as f:
        lines = f.readlines()

    with open(slide_metadata_path, "w") as f:
        for line in lines:
            fields = line.strip().split(",")
            if len(fields) != 17:
                num_corrupted_lines += 1
                continue
            f.write(line)
