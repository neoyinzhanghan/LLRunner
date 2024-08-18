import os
from LLRunner.config import results_dir

# find all the directories in results_dir that starts with BMA-diff
directories = [x for x in os.listdir(results_dir) if x.startswith("BMA-diff")]
bad_dirs = []
# for each directory, find the slide name and the specimen type
# check to see if the cells subdirs exist

for dir_name in directories:
    path = os.path.join(results_dir, dir_name)

    if not os.path.exists(os.path.join(path, "cells")) and not os.path.exists(
        os.path.join(path, "error.txt")
    ):
        bad_dirs.append(dir_name)

print(bad_dirs)
