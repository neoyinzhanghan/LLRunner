import os
import openslide

slides_dir = "/media/hdd2/neo/tmp_slides_dir"

# find the paths to all the ndpi files in the slides_dir
slide_paths = [
    os.path.join(slides_dir, slide_name)
    for slide_name in os.listdir(slides_dir)
    if slide_name.endswith(".ndpi")
]

for slide_path in slide_paths:
    wsi = openslide.OpenSlide(slide_path)

    # print the number of levels in the slide
    print(f"Number of levels in {slide_path}: {wsi.level_count}")

    # print the level 0 mpp
    print(f"MPP of level 0 in {slide_path}: {wsi.properties['openslide.mpp-x']}")
