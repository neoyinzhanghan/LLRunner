import cv2
from PIL import Image, ImageDraw

# Open the image
img = Image.open("/Users/neo/Documents/painting_references/painting_rev.jpg")

# Calculate the target size based on the 2:3 aspect ratio
width, height = img.size
new_width = width
new_height = int(new_width * 3 / 2)

# If the image height is greater than the new height, crop it
if height > new_height:
    top = (height - new_height) // 2
    bottom = top + new_height
    img = img.crop((0, top, width, bottom))

# Save the cropped image
cropped_image_path = "/Users/neo/Documents/painting_references/painting_rev_cropped.jpg"
img.save(cropped_image_path)

# Draw an 8x12 grid on the image
draw = ImageDraw.Draw(img)

# Calculate grid dimensions
cols, rows = 24, 36
col_width = new_width // cols
row_height = new_height // rows

# Draw vertical lines
for i in range(1, cols):
    x = i * col_width
    draw.line([(x, 0), (x, new_height)], fill="black", width=1)

# Open the image as cv2 image
img = cv2.imread(cropped_image_path)

# apply a pyramid mean shift filtering to the image
img = cv2.pyrMeanShiftFiltering(img, 9, 9)

# convert from BGR to RGB color space
img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

# convert img to PIL image
img = Image.fromarray(img)

img.show()

# # Draw horizontal lines
# for i in range(1, rows):
#     y = i * row_height
#     draw.line([(0, y), (new_width, y)], fill="black", width=1)

# # Save the image with grid
# img_with_grid_path = "/Users/neo/Documents/painting_references/painting_rev_cropped_with_grid.jpg"
# img.save(img_with_grid_path)

# # Display the image with the grid
# img.show()






