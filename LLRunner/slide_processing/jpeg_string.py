from PIL import Image
import io


def image_to_jpeg_string(image):
    # Create an in-memory bytes buffer
    buffer = io.BytesIO()

    # Save the image in JPEG format to the buffer
    image.save(buffer, format="JPEG")

    # Get the byte data from the buffer
    jpeg_string = buffer.getvalue()

    return jpeg_string


image_path = "/Users/neo/Downloads/upsampled_more_sharpened_definition_image.jpg"

# Open the image
image = Image.open(image_path)

# Convert the image to a JPEG string
jpeg_string = image_to_jpeg_string(image)

print(jpeg_string[:10])  # b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01'
