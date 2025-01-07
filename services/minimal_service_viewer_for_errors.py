import os
import io
import h5py
import pandas as pd
from PIL import Image
from flask import Flask, send_file, request, render_template_string
from LLBMA.tiling.dzsave_h5 import retrieve_tile_h5
from compute_annotated_tiles import (
    get_LLBMA_processing_status,
    get_annotated_focus_region_indices_and_coordinates,
    get_annotated_tile,
)

app = Flask(__name__)

debug_mode = False

# Function to get dimensions from H5 file
def get_dimensions(h5_path):
    with h5py.File(h5_path, "r") as f:
        height = int(f["level_0_height"][()])
        width = int(f["level_0_width"][()])
    return width, height


def get_slide_datetime(slide_name):
    try:
        name = slide_name.split(".h5")[0]
        datetime = name.split(" - ")[-1]

        # convert the datetime to a datetime object
        datetime = pd.to_datetime(datetime, format="%Y-%m-%d %H.%M.%S")
    except Exception as e:
        print(f"Error getting datetime for {slide_name}: {e}")
        raise e
    return datetime


@app.route("/tile_api", methods=["GET"])
def tile_api():
    slide = request.args.get("slide")

    slide_h5_name = os.path.basename(slide)

    level = int(request.args.get("level"))
    row = int(request.args.get("x"))  # Note: x corresponds to row
    col = int(request.args.get("y"))  # Note: y corresponds to col\

    # if level == 18:

    #     # iterate over the rows of the df
    #     for idx, df_row in df.iterrows():
    #         img_row, img_col = df_row["row"], df_row["col"]

    #         if row == img_row and col == img_col:
    #             image_path = df_row["image_path"]

    #             img_io = io.BytesIO()
    #             with open(image_path, "rb") as f:
    #                 img_io.write(f.read())
    #                 img_io.seek(0)
    #             return send_file(img_io, mimetype="image/jpeg")

    # else:
    #     # iterate through the rows of the df
    #     for idx, df_row in df.iterrows():
    #         level_x, level_y = df_row[f"x_{level}"], df_row[f"y_{level}"]

    #         if row == int(level_x // 512) and col == int(level_y // 512):
    #             # return a completely green image of the same size as the tile

    #             tile = retrieve_tile_h5(
    #                 slide, level, row, col
    #             )  # Retrieve the JPEG image file

    #             # get the shape of the tile
    #             width, height = tile.size

    #             # create a new image with the same size as the tile
    #             img = Image.new("RGB", (width, height), color="green")

    #             img_io = io.BytesIO()
    #             img.save(img_io, format="JPEG")

    #             img_io.seek(0)
    #             return send_file(img_io, mimetype="image/jpeg")

    tile = retrieve_tile_h5(slide, level, row, col)  # Retrieve the JPEG image file
    if get_LLBMA_processing_status(slide_h5_name) == "Processed":
        df = get_annotated_focus_region_indices_and_coordinates(slide_h5_name)

        tile = get_annotated_tile(
            tile_image=tile,
            tile_row=row,
            tile_col=col,
            tile_level=level,
            focus_regions_df=df,
            debug_mode=debug_mode,
        )
    img_io = io.BytesIO()
    tile.save(img_io, format="JPEG")
    img_io.seek(0)
    return send_file(img_io, mimetype="image/jpeg")


# @app.route("/", methods=["GET"])
# def index():

#     root_dir = "/media/hdd2/neo/SameDayDzsave"

#     # find all the h5 files in the root_dir
#     slide_h5_names = [
#         slide_name for slide_name in os.listdir(root_dir) if slide_name.endswith(".h5")
#     ]

#     # sort the slide names by datetime
#     slide_h5_names.sort(key=get_slide_datetime)

#     slide_h5_paths = [
#         os.path.join(root_dir, slide_name) for slide_name in slide_h5_names
#     ]

#     slide_options = "".join(
#         [f'<option value="{slide}"> {slide}</option>' for slide in slide_h5_paths]
#     )

#     template = f"""
#     <!DOCTYPE html>
#     <html>
#     <head>
#         <title> H5 Slide Viewer</title>
#         <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Roboto:wght@300;400;700&display=swap" rel="stylesheet">
#         <script src="https://cdnjs.cloudflare.com/ajax/libs/openseadragon/2.4.2/openseadragon.min.js"></script>
#         <style>
#             /* General Styles */
#             body {{
#                 font-family: 'Roboto', sans-serif;
#                 margin: 0;
#                 padding: 0;
#                 background-color: #0a1e34;
#                 color: #d3e3f1;
#                 transition: background-color 0.5s, color 0.5s;
#                 display: flex;
#                 flex-direction: column;
#                 align-items: center;
#                 overflow-x: hidden;
#             }}

#             /* Header Style */
#             .header {{
#                 text-align: center;
#                 padding: 20px;
#                 background: linear-gradient(to right, #162b44, #0a1e34);
#                 color: #00eaff;
#                 font-family: 'Orbitron', sans-serif;
#                 font-size: 28px;
#                 letter-spacing: 2px;
#                 border-bottom: 3px solid #0e6ba8;
#                 width: 100%;
#                 box-shadow: 0 8px 15px rgba(0, 0, 0, 0.6);
#                 transition: background 0.5s, box-shadow 0.5s;
#             }}

#             .header:hover {{
#                 background: linear-gradient(to right, #0e6ba8, #162b44);
#                 box-shadow: 0 12px 20px rgba(0, 0, 0, 0.8);
#             }}

#             /* Theme Toggle Button */
#             .theme-toggle {{
#                 position: fixed;
#                 top: 15px;
#                 right: 20px;
#                 background: #003b66;
#                 border: 2px solid #00eaff;
#                 border-radius: 30px;
#                 color: #d3e3f1;
#                 padding: 10px 25px;
#                 cursor: pointer;
#                 font-size: 14px;
#                 font-family: 'Roboto', sans-serif;
#                 transition: background-color 0.3s, transform 0.2s, color 0.3s;
#                 z-index: 10;
#             }}

#             .theme-toggle:hover {{
#                 background: #00eaff;
#                 color: #003b66;
#                 transform: scale(1.05);
#             }}

#             /* Dropdown Style */
#             select {{
#                 margin: 40px auto;
#                 display: block;
#                 padding: 12px;
#                 font-size: 16px;
#                 font-family: 'Roboto', sans-serif;
#                 border-radius: 10px;
#                 border: 2px solid #0e6ba8;
#                 background: #0a1e34;
#                 color: #d3e3f1;
#                 width: 90%;
#                 max-width: 500px;
#                 text-align: center;
#                 transition: border-color 0.3s, box-shadow 0.3s;
#                 box-shadow: 0 4px 10px rgba(0, 0, 0, 0.4);
#             }}

#             select:hover {{
#                 border-color: #00eaff;
#                 box-shadow: 0 8px 15px rgba(0, 0, 0, 0.7);
#             }}

#             /* OpenSeadragon Viewer */
#             #openseadragon1 {{
#                 width: 90%;
#                 max-width: 1200px;
#                 height: 600px;
#                 border: 5px solid #0e6ba8;
#                 border-radius: 15px;
#                 margin-top: 30px;
#                 box-shadow: 0 8px 20px rgba(0, 0, 0, 0.7);
#                 opacity: 0;
#                 transform: translateY(30px);
#                 transition: opacity 0.5s ease, transform 0.5s ease;
#             }}

#             #openseadragon1.show {{
#                 opacity: 1;
#                 transform: translateY(0);
#             }}

#             /* Footer Style */
#             .footer {{
#                 margin-top: 20px;
#                 font-size: 12px;
#                 text-align: center;
#                 color: #d3e3f1;
#             }}

#             .footer a {{
#                 color: #00eaff;
#                 text-decoration: none;
#             }}

#             .footer a:hover {{
#                 text-decoration: underline;
#             }}

#             /* Light Theme */
#             .light-theme {{
#                 background-color: #e8f4fc;
#                 color: #333;
#             }}

#             .light-theme .header {{
#                 background: linear-gradient(to right, #b8dff7, #e8f4fc);
#                 color: #003b66;
#             }}

#             .light-theme .theme-toggle {{
#                 background: #c8e8ff;
#                 color: #003b66;
#                 border-color: #003b66;
#             }}

#             .light-theme .theme-toggle:hover {{
#                 background: #003b66;
#                 color: #c8e8ff;
#             }}

#             .light-theme select {{
#                 background: #e8f4fc;
#                 color: #003b66;
#                 border-color: #003b66;
#             }}

#             .light-theme select:hover {{
#                 border-color: #005b8a;
#             }}
#         </style>
#     </head>
#     <body>
#         <div class="header"> BMA and PBS Slide Viewer</div>
#         <button class="theme-toggle" onclick="toggleTheme()">🔆 Switch to Light Theme</button>
#         <label for="slide" style="text-align: center; display: block; font-size: 18px; margin-top: 20px;"> Select a Slide:</label>
#         <select id="slide" onchange="initializeViewer()">
#             {slide_options}
#         </select>
#         <div id="openseadragon1"></div>

#         <script>
#             let viewer;
#             let isDarkTheme = true;

#             // Toggle between dark and light themes
#             function toggleTheme() {{
#                 isDarkTheme = !isDarkTheme;
#                 document.body.className = isDarkTheme ? "" : "light-theme";
#                 document.querySelector('.theme-toggle').textContent = isDarkTheme ? '🔆 Switch to Light Theme' : '🌙 Switch to Dark Theme';
#             }}

#             // Initialize OpenSeadragon viewer
#             function initializeViewer() {{
#                 const slide = document.getElementById("slide").value;
#                 fetch(`/get_dimensions?slide=${{encodeURIComponent(slide)}}`)
#                     .then(response => response.json())
#                     .then(dimensions => {{
#                         const width = dimensions.width;
#                         const height = dimensions.height;

#                         if (viewer) {{
#                             viewer.destroy();
#                         }}

#                         viewer = OpenSeadragon({{
#                             id: "openseadragon1",
#                             prefixUrl: "https://cdnjs.cloudflare.com/ajax/libs/openseadragon/2.4.2/images/",
#                             tileSources: {{
#                                 width: width,
#                                 height: height,
#                                 tileSize: 512,
#                                 maxLevel: 18,
#                                 getTileUrl: function(level, x, y) {{
#                                     return `/tile_api?slide=${{encodeURIComponent(slide)}}&level=${{level}}&x=${{x}}&y=${{y}}`;
#                                 }}
#                             }},
#                             showNavigator: true,
#                             navigatorPosition: "BOTTOM_RIGHT",
#                             minZoomLevel: 0.5,
#                             zoomPerScroll: 1.5,
#                         }});

#                         document.getElementById('openseadragon1').classList.add('show');
#                     }})
#                     .catch(error => console.error("Error fetching slide data:", error));
#             }}
#         </script>
#     </body>
#     </html>
#     """

#     return render_template_string(template)


# @app.route("/", methods=["GET"])
# def index():
#     root_dir = "/media/hdd2/neo/SameDayDzsave"

#     print("Finding slide options...")
#     # find all the h5 files in the root_dir
#     slide_h5_names = [
#         slide_name for slide_name in os.listdir(root_dir) if slide_name.endswith(".h5")
#     ]

#     print("Sorting slide options...")
#     # sort the slide names by datetime
#     slide_h5_names.sort(key=get_slide_datetime)

#     print("Creating slide options...")
#     slide_h5_paths = [
#         os.path.join(root_dir, slide_name) for slide_name in slide_h5_names
#     ]

#     slide_options = "".join(
#         [f'<option value="{slide}"> {slide}</option>' for slide in slide_h5_paths]
#     )

#     print(f"Found {len(slide_h5_paths)} slides options.")

#     template = f"""
#     <!DOCTYPE html>
#     <html>
#     <head>
#         <title> H5 Slide Viewer</title>
#         <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Roboto:wght@300;400;700&display=swap" rel="stylesheet">
#         <link href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/css/select2.min.css" rel="stylesheet" />
#         <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
#         <script src="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/js/select2.min.js"></script>
#         <script src="https://cdnjs.cloudflare.com/ajax/libs/openseadragon/2.4.2/openseadragon.min.js"></script>
#         <style>
#             /* General Styles */
#             body {{
#                 font-family: 'Roboto', sans-serif;
#                 margin: 0;
#                 padding: 0;
#                 background-color: #0a1e34;
#                 color: #d3e3f1;
#                 display: flex;
#                 flex-direction: column;
#                 align-items: center;
#             }}

#             .header {{
#                 text-align: center;
#                 padding: 20px;
#                 background: linear-gradient(to right, #162b44, #0a1e34);
#                 color: #00eaff;
#                 font-family: 'Orbitron', sans-serif;
#                 font-size: 28px;
#                 letter-spacing: 2px;
#                 width: 100%;
#                 box-shadow: 0 8px 15px rgba(0, 0, 0, 0.6);
#             }}

#             #slide {{
#                 width: 90%;
#                 max-width: 500px;
#                 margin: 20px auto;
#                 font-size: 16px;
#             }}

#             #openseadragon1 {{
#                 width: 90%;
#                 max-width: 1200px;
#                 height: 600px;
#                 margin-top: 30px;
#             }}
#         </style>
#     </head>
#     <body>
#         <div class="header"> BMA and PBS Slide Viewer</div>
#         <label for="slide" style="text-align: center; display: block; font-size: 18px; margin-top: 20px;"> Select a Slide:</label>
#         <select id="slide" onchange="initializeViewer()" class="searchable-dropdown">
#             {slide_options}
#         </select>
#         <div id="openseadragon1"></div>

#         <script>
#             $(document).ready(function() {{
#                 // Initialize Select2 on the dropdown
#                 $('.searchable-dropdown').select2({{
#                     placeholder: "Search for a slide...",
#                     allowClear: true,
#                     width: 'resolve' // Ensures proper width of the dropdown
#                 }});
#             }});

#             let viewer;

#             // Initialize OpenSeadragon viewer
#             function initializeViewer() {{
#                 const slide = document.getElementById("slide").value;
#                 fetch(`/get_dimensions?slide=${{encodeURIComponent(slide)}}`)
#                     .then(response => response.json())
#                     .then(dimensions => {{
#                         const width = dimensions.width;
#                         const height = dimensions.height;

#                         if (viewer) {{
#                             viewer.destroy();
#                         }}

#                         viewer = OpenSeadragon({{
#                             id: "openseadragon1",
#                             prefixUrl: "https://cdnjs.cloudflare.com/ajax/libs/openseadragon/2.4.2/images/",
#                             tileSources: {{
#                                 width: width,
#                                 height: height,
#                                 tileSize: 512,
#                                 maxLevel: 18,
#                                 getTileUrl: function(level, x, y) {{
#                                     return `/tile_api?slide=${{encodeURIComponent(slide)}}&level=${{level}}&x=${{x}}&y=${{y}}`;
#                                 }}
#                             }},
#                             showNavigator: true,
#                             navigatorPosition: "BOTTOM_RIGHT",
#                             minZoomLevel: 0.5,
#                             zoomPerScroll: 1.5,
#                         }});
#                     }})
#                     .catch(error => console.error("Error fetching slide data:", error));
#             }}
#         </script>
#     </body>
#     </html>
#     """

#     return render_template_string(template)


@app.route("/", methods=["GET"])
def index():
    root_dir = "/media/hdd2/neo/SameDayDzsave"
    results_dir = "/media/hdd2/neo/SameDayLLBMAResults"

    print("Finding slide options...")
    # find all the h5 files in the root_dir
    slide_h5_names = [
        slide_name for slide_name in os.listdir(root_dir) if slide_name.endswith(".h5")
    ]

    print("Sorting slide options...")
    # sort the slide names by datetime
    slide_h5_names.sort(key=get_slide_datetime)

    print("Creating slide options...")
    slide_h5_paths = [
        os.path.join(root_dir, slide_name) for slide_name in slide_h5_names
    ]

    error_slide_h5_paths = []

    for slide_h5_path in slide_h5_paths:
        slide_h5_name = os.path.basename(slide_h5_path)
        slide_h5_basename = slide_h5_name.split(".h5")[0]

        if os.path.exists(
            os.path.join(
                results_dir,
                f"ERROR_{slide_h5_basename}",
            )
        ):
            error_slide_h5_paths.append(slide_h5_path)

    slide_options = "".join(
        [f'<option value="{slide}"> {slide}</option>' for slide in error_slide_h5_paths]
    )

    template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title> H5 Slide Viewer (Error Slides) </title>
        <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Roboto:wght@300;400;700&display=swap" rel="stylesheet">
        <link href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/css/select2.min.css" rel="stylesheet" />
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/js/select2.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/openseadragon/2.4.2/openseadragon.min.js"></script>
        <style>
            /* General Styles */
            body {{
                font-family: 'Roboto', sans-serif;
                margin: 0;
                padding: 0;
                background-color: #e8f4fc; /* Pastel light blue background */
                color: #003b66;
                display: flex;
                flex-direction: column;
                align-items: center;
            }}

            .header {{
                text-align: center;
                padding: 20px;
                background: #b8dff7; /* Lighter blue */
                color: #003b66;
                font-family: 'Orbitron', sans-serif;
                font-size: 28px;
                letter-spacing: 2px;
                width: 100%;
                box-shadow: 0 8px 15px rgba(0, 0, 0, 0.2);
            }}

            #slide {{
                width: 90%;
                max-width: 500px;
                margin: 20px auto;
                font-size: 16px;
                border-radius: 5px;
                border: 1px solid #b8dff7;
                padding: 8px;
                background: #ffffff;
                color: #003b66;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}

            #openseadragon1 {{
                width: 90%;
                max-width: 1200px;
                height: 600px;
                margin-top: 30px;
                background-color: #ffffff;
                border: 2px solid #b8dff7;
                border-radius: 10px;
                box-shadow: 0 8px 15px rgba(0, 0, 0, 0.1);
            }}
        </style>
    </head>
    <body>
        <div class="header"> BMA and PBS Slide Viewer (Error Slides) </div>
        <label for="slide" style="text-align: center; display: block; font-size: 18px; margin-top: 20px;"> Select a Slide:</label>
        <select id="slide" onchange="initializeViewer()" class="searchable-dropdown">
            {slide_options}
        </select>
        <div id="openseadragon1"></div>

        <script>
            $(document).ready(function() {{
                // Initialize Select2 on the dropdown
                $('.searchable-dropdown').select2({{
                    placeholder: "Search for a slide...",
                    allowClear: true,
                    width: 'resolve' // Ensures proper width of the dropdown
                }});
            }});

            let viewer;

            // Initialize OpenSeadragon viewer
            function initializeViewer() {{
                const slide = document.getElementById("slide").value;
                fetch(`/get_dimensions?slide=${{encodeURIComponent(slide)}}`)
                    .then(response => response.json())
                    .then(dimensions => {{
                        const width = dimensions.width;
                        const height = dimensions.height;

                        if (viewer) {{
                            viewer.destroy();
                        }}

                        viewer = OpenSeadragon({{
                            id: "openseadragon1",
                            prefixUrl: "https://cdnjs.cloudflare.com/ajax/libs/openseadragon/2.4.2/images/",
                            tileSources: {{
                                width: width,
                                height: height,
                                tileSize: 512,
                                maxLevel: 18,
                                getTileUrl: function(level, x, y) {{
                                    return `/tile_api?slide=${{encodeURIComponent(slide)}}&level=${{level}}&x=${{x}}&y=${{y}}`;
                                }}
                            }},
                            showNavigator: true,
                            navigatorPosition: "BOTTOM_RIGHT",
                            minZoomLevel: 0.5,
                            zoomPerScroll: 1.5,
                        }});
                    }})
                    .catch(error => console.error("Error fetching slide data:", error));
            }}
        </script>
    </body>
    </html>
    """

    return render_template_string(template)


@app.route("/get_dimensions", methods=["GET"])
def get_dimensions_api():
    slide = request.args.get("slide")
    width, height = get_dimensions(slide)
    return {"width": width, "height": height}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7992)
