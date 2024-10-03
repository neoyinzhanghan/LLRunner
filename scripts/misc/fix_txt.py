# Define the function to add a new line before every occurrence of 'list(data_path'
def add_new_line_before_list(file_path, output_file_path):
    # Read the file content
    with open(file_path, 'r') as file:
        content = file.read()

    # Add a new line before every 'list(data_path'
    fixed_content = content.replace('list(data_path', '\nlist(data_path')

    # Write the fixed content to a new file
    with open(output_file_path, 'w') as file:
        file.write(fixed_content)

    print(f"Fixed content saved to: {output_file_path}")

# File paths
input_file = '/Users/neo/Downloads/predict_test_2 3/results.txt'
output_file = '/Users/neo/Downloads/predict_test_2 3/fixed_results.txt'

# Run the function
add_new_line_before_list(input_file, output_file)
