import os
from LLRunner.config import results_dir

# find all the directories in results_dir that starts with BMA-diff
directories = [x for x in os.listdir(results_dir) if x.startswith("BMA-diff")]
paths = [os.path.join(results_dir, x) for x in directories]

bad_dirs = []
# for each directory, find the slide name and the specimen type
# check to see if the paths/cells subdirs exist

for path in paths:
    if not os.path.exists(os.path.join(path, "cells")):
        bad_dirs.append(path)

print(bad_dirs)