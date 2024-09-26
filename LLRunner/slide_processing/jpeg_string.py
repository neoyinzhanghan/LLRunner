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


def jpeg_string_to_image(jpeg_string):
    # Create a BytesIO object from the JPEG string (byte data)
    buffer = io.BytesIO(jpeg_string)

    # Open the image from the buffer
    image = Image.open(buffer)

    return image


image_path = "/Users/neo/Downloads/upsampled_more_sharpened_definition_image.jpg"

# Open the image
image = Image.open(image_path)

# Convert the image to a JPEG string
jpeg_string = image_to_jpeg_string(image)

print(jpeg_string)  # b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01'


# Convert the JPEG string back to an image
image_from_jpeg_string = jpeg_string_to_image(jpeg_string)


# display the image
image_from_jpeg_string.show()
