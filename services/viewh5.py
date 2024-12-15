if __name__ == "__main__":

    import os
    import io
    import h5py
    import argparse
    import pandas as pd
    from flask import Flask, send_file, request, render_template_string
    from LLBMA.tiling.dzsave_h5 import retrieve_tile_h5

    parser = argparse.ArgumentParser(description="Run the H5 Slide Viewer")
    parser.add_argument(
        "--slide_h5_path",
        type=str,
        default="",
        help="Path to the directory containing the H5 slide files",
    )

    args = parser.parse_args()

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
        # Ensure the provided slide path is valid
        if not os.path.exists(args.slide_h5_path) or not args.slide_h5_path.endswith(
            ".h5"
        ):
            return f"Error: Invalid slide path {args.slide_h5_path}", 400

        template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>H5 Slide Viewer</title>
            <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Roboto:wght@300;400;700&display=swap" rel="stylesheet">
            <script src="https://cdnjs.cloudflare.com/ajax/libs/openseadragon/2.4.2/openseadragon.min.js"></script>
            <style>
                /* General Styles */
                body {{
                    font-family: 'Roboto', sans-serif;
                    margin: 0;
                    padding: 0;
                    background-color: #0a1e34;
                    color: #d3e3f1;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                }}

                /* Header Style */
                .header {{
                    text-align: center;
                    padding: 20px;
                    background: linear-gradient(to right, #162b44, #0a1e34);
                    color: #00eaff;
                    font-family: 'Orbitron', sans-serif;
                    font-size: 28px;
                }}

                /* Slide Path Display */
                .slide-path {{
                    margin: 10px 0 20px;
                    font-size: 16px;
                    color: #d3e3f1;
                    text-align: center;
                    word-wrap: break-word;
                    width: 90%;
                    max-width: 1000px;
                    border: 1px solid #0e6ba8;
                    padding: 10px;
                    border-radius: 5px;
                    background-color: #162b44;
                }}

                /* OpenSeadragon Viewer */
                #openseadragon1 {{
                    width: 90%;
                    max-width: 1200px;
                    height: 600px;
                    border: 5px solid #0e6ba8;
                    border-radius: 15px;
                    margin-top: 30px;
                }}
            </style>
        </head>
        <body>
            <div class="header">H5 Slide Viewer</div>
            <div class="slide-path">Current Slide: {args.slide_h5_path}</div>
            <div id="openseadragon1"></div>

            <script>
                // Initialize OpenSeadragon viewer with the provided slide
                const slide = "{args.slide_h5_path}";

                fetch(`/get_dimensions?slide=${{encodeURIComponent(slide)}}`)
                    .then(response => response.json())
                    .then(dimensions => {{
                        const width = dimensions.width;
                        const height = dimensions.height;

                        OpenSeadragon({{
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
                    }})
                    .catch(error => console.error("Error fetching slide data:", error));
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
