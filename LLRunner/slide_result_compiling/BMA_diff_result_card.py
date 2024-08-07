import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from PIL import Image, ImageDraw, ImageFont
from LLRunner.read.BMAResult import BMAResult, BMAResultSSH
from LLRunner.config import available_machines, ssh_config
from PIL import Image


def match_reported_and_grouped_differential(reported_diff_dict, grouped_diff_dict):
    """First assert that the keys in grouped_diff_dict is a subset of the keys in reported_diff_dict. Then, for each key in grouped_diff_dict, if the value is None, set it to 0."""

    assert set(grouped_diff_dict.keys()).issubset(
        set(reported_diff_dict.keys())
    ), f"Keys in grouped_diff_dict is not a subset of keys in reported_diff_dict. Reported: {set(reported_diff_dict.keys())}, Grouped: {set(grouped_diff_dict.keys())}"

    # then for each key in reported_diff_dict, if the value is None, set it to 0
    reported_diff_dict = {
        k: v if v is not None else -0.1 for k, v in reported_diff_dict.items()
    }

    # then for each key in grouped_diff_dict, if the value is None, set it to 0
    grouped_diff_dict = {
        k: v if v is not None else -0.1 for k, v in grouped_diff_dict.items()
    }

    # need to multiply by 100 to get the percentage <<< due to how the differential is computed
    grouped_diff_dict = {k: v * 100 for k, v in grouped_diff_dict.items()}

    # only keep the keys that are in grouped_diff_dict
    reported_diff_dict = {
        k: v for k, v in reported_diff_dict.items() if k in grouped_diff_dict
    }

    return reported_diff_dict, grouped_diff_dict


def plot_differential_comparison_image_light_futuristic(
    reported_diff_dict,
    grouped_diff_dict,
    title="Comparison of Reported and Grouped Differential",
):
    """
    Create a side-by-side bar plot comparison between the reported and grouped differential dictionaries,
    and return the plot as a PIL Image object with a light medical futuristic theme.
    """

    # Prepare data for plotting
    categories = list(reported_diff_dict.keys())
    reported_values = list(reported_diff_dict.values())
    grouped_values = list(grouped_diff_dict.values())

    # Create a DataFrame for Seaborn
    df = pd.DataFrame(
        {
            "Category": categories * 2,
            "Value": reported_values + grouped_values,
            "Type": ["Reported"] * len(reported_values)
            + ["Grouped"] * len(grouped_values),
        }
    )

    # Apply a light medical futuristic theme
    sns.set_theme(style="whitegrid")
    sns.set_context("talk")

    plt.figure(figsize=(14, 8))
    ax = sns.barplot(x="Category", y="Value", hue="Type", data=df, palette="coolwarm")

    # Add a title with a futuristic font style
    plt.title(
        title,
        fontsize=18,
        weight="bold",
        color="#007ACC",
    )

    # Style the axes and labels
    ax.set_xlabel("Cell Type", fontsize=14, weight="bold", color="#007ACC")
    ax.set_ylabel("Proportion", fontsize=14, weight="bold", color="#007ACC")
    plt.xticks(rotation=45, ha="right", fontsize=12, weight="bold", color="#007ACC")
    plt.yticks(fontsize=12, weight="bold", color="#007ACC")

    # Customize the legend
    plt.legend(
        title="Type",
        fontsize=12,
        title_fontsize=14,
        loc="upper right",
        facecolor="white",
        edgecolor="black",
    )

    # Set the background color
    ax.set_facecolor("#F0F0F0")
    plt.gcf().set_facecolor("#FFFFFF")
    plt.grid(True, color="#D0D0D0")

    plt.tight_layout()

    # Save the plot to a BytesIO object and return it as a PIL Image
    from io import BytesIO

    image_stream = BytesIO()
    plt.savefig(image_stream, format="png")
    plt.close()  # Close the plot to free memory
    image_stream.seek(0)  # Rewind the stream to the beginning
    return Image.open(image_stream)


# def create_result_card_stacked_with_simple_theme(
#     differential_comparison_image,
#     confidence_heatmap,
#     total_processing_time,
#     storage_consumption,
#     Dx,
#     sub_Dx,
#     wsi_name,
#     datetime_processed,
#     pipeline,
#     num_regions,
#     num_cells,
# ):
#     """
#     Create a result card image that consolidates the confidence heatmap, differential comparison image, and other metadata.
#     The images are resized to fit within a specific width and stacked on top of each other.
#     Text is displayed with a simple, clean theme matching the plot font.
#     """

#     # Set a target width and maintain aspect ratio for resizing
#     target_width = 1000
#     confidence_heatmap_resized = confidence_heatmap.resize(
#         (
#             target_width,
#             int(confidence_heatmap.height * target_width / confidence_heatmap.width),
#         )
#     )
#     differential_comparison_image_resized = differential_comparison_image.resize(
#         (
#             target_width,
#             int(
#                 differential_comparison_image.height
#                 * target_width
#                 / differential_comparison_image.width
#             ),
#         )
#     )

#     # Define the size of the card
#     card_width = target_width + 40  # 40 for margin
#     card_height = (
#         confidence_heatmap_resized.height
#         + differential_comparison_image_resized.height
#         + 350
#     )  # Extra space for text
#     card = Image.new(
#         "RGB", (card_width, card_height), color="white"
#     )  # White background

#     draw = ImageDraw.Draw(card)

#     # Use a simple, clean font
#     font = ImageFont.load_default()
#     line_height = 32  # Adjust line height for better readability

#     # Define positions for the images and text
#     margin = 20

#     # Paste the confidence heatmap on the top
#     card.paste(confidence_heatmap_resized, (margin, margin))

#     # Paste the differential comparison image below the heatmap
#     card.paste(
#         differential_comparison_image_resized,
#         (margin, confidence_heatmap_resized.height + margin + 20),
#     )

#     # Write the metadata below the images with matching font
#     text_y = (
#         confidence_heatmap_resized.height
#         + differential_comparison_image_resized.height
#         + 2 * margin
#         + 20
#     )
#     text_color = "black"  # Simple black text

#     draw.text((margin, text_y), f"WSI Name: {wsi_name}", font=font, fill=text_color)
#     text_y += line_height
#     draw.text((margin, text_y), f"Pipeline: {pipeline}", font=font, fill=text_color)
#     text_y += line_height
#     draw.text(
#         (margin, text_y),
#         f"Date Processed: {datetime_processed}",
#         font=font,
#         fill=text_color,
#     )
#     text_y += line_height
#     draw.text(
#         (margin, text_y),
#         f"Diagnosis: {Dx}, Sub Diagnosis: {sub_Dx}",
#         font=font,
#         fill=text_color,
#     )
#     text_y += line_height
#     draw.text(
#         (margin, text_y),
#         f"Total Processing Time: {total_processing_time} seconds",
#         font=font,
#         fill=text_color,
#     )
#     text_y += line_height
#     draw.text(
#         (margin, text_y),
#         f"Storage Consumption: {storage_consumption}",
#         font=font,
#         fill=text_color,
#     )
#     text_y += line_height
#     draw.text(
#         (margin, text_y),
#         f"Number of Regions: {num_regions}",
#         font=font,
#         fill=text_color,
#     )
#     text_y += line_height
#     draw.text(
#         (margin, text_y), f"Number of Cells: {num_cells}", font=font, fill=text_color
#     )

#     return card


def create_result_card_stacked_with_simple_theme(
    differential_comparison_image,
    confidence_heatmap,
    total_processing_time,
    storage_consumption,
    Dx,
    sub_Dx,
    wsi_name,
    datetime_processed,
    pipeline,
    num_regions,
    num_cells,
):
    """
    Create a result card image that consolidates the confidence heatmap, differential comparison image, and other metadata.
    The images are resized to fit within a specific width and stacked on top of each other.
    Text is displayed with a simple, clean theme matching the plot font.
    """

    # Set a target width and maintain aspect ratio for resizing
    target_width = 1000
    confidence_heatmap_resized = confidence_heatmap.resize(
        (
            target_width,
            int(confidence_heatmap.height * target_width / confidence_heatmap.width),
        )
    )
    differential_comparison_image_resized = differential_comparison_image.resize(
        (
            target_width,
            int(
                differential_comparison_image.height
                * target_width
                / differential_comparison_image.width
            ),
        )
    )

    # Define the size of the card
    card_width = target_width + 40  # 40 for margin
    card_height = (
        confidence_heatmap_resized.height
        + differential_comparison_image_resized.height
        + 380
    )  # Extra space for text
    card = Image.new(
        "RGB", (card_width, card_height), color="white"
    )  # White background

    draw = ImageDraw.Draw(card)

    # Load a generic font with customizable size
    font_size = 32
    try:
        font = ImageFont.truetype("DejaVuSans.ttf", font_size)
    except OSError:
        print("DejaVuSans.ttf not found. Using default PIL font with custom size.")
        font = ImageFont.truetype("LiberationSans-Regular.ttf", font_size)

    line_height = font_size + 5  # Adjust line height (add a small margin)

    # Define positions for the images and text
    margin = 20

    # Paste the confidence heatmap on the top
    card.paste(confidence_heatmap_resized, (margin, margin))

    # Paste the differential comparison image below the heatmap
    card.paste(
        differential_comparison_image_resized,
        (margin, confidence_heatmap_resized.height + margin + 20),
    )

    # Write the metadata below the images with matching font
    text_y = (
        confidence_heatmap_resized.height
        + differential_comparison_image_resized.height
        + 2 * margin
        + 20
    )
    text_color = "black"  # Simple black text

    draw.text((margin, text_y), f"WSI Name: {wsi_name}", font=font, fill=text_color)
    text_y += line_height
    draw.text((margin, text_y), f"Pipeline: {pipeline}", font=font, fill=text_color)
    text_y += line_height
    draw.text(
        (margin, text_y),
        f"Date Processed: {datetime_processed}",
        font=font,
        fill=text_color,
    )
    text_y += line_height
    draw.text(
        (margin, text_y),
        f"Diagnosis: {Dx}, Sub Diagnosis: {sub_Dx}",
        font=font,
        fill=text_color,
    )
    text_y += line_height
    draw.text(
        (margin, text_y),
        f"Total Processing Time: {total_processing_time} seconds",
        font=font,
        fill=text_color,
    )
    text_y += line_height
    draw.text(
        (margin, text_y),
        f"Storage Consumption: {storage_consumption}",
        font=font,
        fill=text_color,
    )
    text_y += line_height
    draw.text(
        (margin, text_y),
        f"Number of Regions: {num_regions}",
        font=font,
        fill=text_color,
    )
    text_y += line_height
    draw.text(
        (margin, text_y), f"Number of Cells: {num_cells}", font=font, fill=text_color
    )

    return card


def get_mini_result_card(remote_result_dir, machine):
    """Get the mini result card for the BMA-diff pipeline."""

    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines."

    username = ssh_config[machine]["username"]
    hostname = ssh_config[machine]["hostname"]

    bma_result = BMAResultSSH(
        hostname=hostname,
        username=username,
        remote_result_dir=remote_result_dir,
        max_retries=5,  # Optional: set the max retries for rsync, defaults to 3
        backoff_factor=2,  # Optional: set the backoff factor for rsync, defaults to 2
    )

    reported_diff_dict = bma_result.get_reported_differential()
    grouped_diff_dict = bma_result.get_grouped_differential()

    # now for both dict, set None values to 0
    reported_diff_dict = {
        k: v if v is not None else 0 for k, v in reported_diff_dict.items()
    }
    grouped_diff_dict = {
        k: v if v is not None else 0 for k, v in grouped_diff_dict.items()
    }

    reported_diff_dict, grouped_diff_dict = match_reported_and_grouped_differential(
        reported_diff_dict, grouped_diff_dict
    )

    num_regions = bma_result.get_num_regions()
    num_cells = bma_result.get_num_cells()

    result_card = create_result_card_stacked_with_simple_theme(
        differential_comparison_image=plot_differential_comparison_image_light_futuristic(
            reported_diff_dict, grouped_diff_dict, title=bma_result.get_wsi_name()
        ),
        confidence_heatmap=bma_result.get_confidence_heatmap(),
        total_processing_time=bma_result.get_runtime_breakdown()["total_time"],
        storage_consumption=bma_result.get_storage_consumption(),
        Dx=bma_result.get_Dx_and_sub_Dx()[0],
        sub_Dx=bma_result.get_Dx_and_sub_Dx()[1],
        wsi_name=bma_result.get_wsi_name(),
        datetime_processed=bma_result.get_datetime_processed(),
        pipeline=bma_result.get_pipeline(),
        num_regions=num_regions,
        num_cells=num_cells,
    )

    return result_card


if __name__ == "__main__":
    # Example usage
    remote_result_dir = "/media/hdd3/neo/results_dir/BMA-diff_2024-07-31 23:06:08"
    machine = "glv2"
    result_card = get_mini_result_card(remote_result_dir, machine)
    result_card.save("result_card.png")  # Save the result card to a file
