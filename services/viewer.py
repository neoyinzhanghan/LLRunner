import os
import io
import h5py
import pandas as pd
from flask import Flask, send_file, request, render_template_string
from LLBMA.tiling.dzsave_h5 import retrieve_tile_h5

app = Flask(__name__)


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
    level = int(request.args.get("level"))
    row = int(request.args.get("x"))  # Note: x corresponds to row
    col = int(request.args.get("y"))  # Note: y corresponds to col

    tile = retrieve_tile_h5(slide, level, row, col)  # Retrieve the JPEG image file
    img_io = io.BytesIO()
    tile.save(img_io, format="JPEG")
    img_io.seek(0)
    return send_file(img_io, mimetype="image/jpeg")


@app.route("/", methods=["GET"])
def index():

    root_dir = "/media/hdd2/neo/SameDayDzsave"

    # find all the h5 files in the root_dir
    slide_h5_names = [
        slide_name for slide_name in os.listdir(root_dir) if slide_name.endswith(".h5")
    ]

    # sort the slide names by datetime
    slide_h5_names.sort(key=get_slide_datetime)

    slide_h5_paths = [
        os.path.join(root_dir, slide_name) for slide_name in slide_h5_names
    ]

    slide_options = "".join(
        [f'<option value="{slide}"> {slide}</option>' for slide in slide_h5_paths]
    )

    # template is index.html file, load the template and render it

    template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title> H5 Slide Viewer</title>
        <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Roboto:wght@300;400;700&display=swap" rel="stylesheet">
        <script src="https://cdnjs.cloudflare.com/ajax/libs/openseadragon/2.4.2/openseadragon.min.js"></script>
        <style>
            /* General Styles */
            body {{
                font-family: 'Roboto', sans-serif;
                margin: 0;
                padding: 0;
                background-color: #e8f4fc; /* Light theme default */
                color: #333;
                transition: background-color 0.5s, color 0.5s;
                display: flex;
                flex-direction: column;
                align-items: center;
                overflow-x: hidden;
            }}
            /* Add other styles here */
        </style>
    </head>
    <body class="light-theme">
        <div class="header"> BMA and PBS Slide Viewer</div>
        <button class="theme-toggle" onclick="toggleTheme()"> 🌙 Switch to Dark Theme</button>
        <label for="slide" style="text-align: center; display: block; font-size: 18px; margin-top: 20px;"> Select a Slide:</label>
        <select id="slide" onchange="initializeViewer()">
            {slide_options}
        </select>
        <div id="openseadragon1"></div>
        <div class="footer">
            Developed by the <a href="https://www.hemepath.ai/" target="_blank">Goldgof HemeAI Lab</a>
        </div>
        <script>
            let viewer;
            let isDarkTheme = false;

            function toggleTheme() {{
                isDarkTheme = !isDarkTheme;
                document.body.className = isDarkTheme ? "dark-theme" : "light-theme";
                document.querySelector('.theme-toggle').textContent = isDarkTheme ? '🔆 Switch to Light Theme' : '🌙 Switch to Dark Theme';
            }}

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
                        }});

                        document.getElementById('openseadragon1').classList.add('show');
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
    app.run(host="0.0.0.0", port=5000)
