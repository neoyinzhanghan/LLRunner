from get_service_summary import get_number_of_regions_and_cells

test_result_dir = "/media/hdd2/neo/test_slide_result_dir"

(
    num_focus_regions_passed,
    num_unannotated_focus_regions,
    num_cells_passed,
    num_removed_cells,
) = get_number_of_regions_and_cells(test_result_dir)

print(f"Number of focus regions passed: {num_focus_regions_passed}")
print(f"Number of unannotated focus regions: {num_unannotated_focus_regions}")
print(f"Number of cells passed: {num_cells_passed}")
print(f"Number of cells removed: {num_removed_cells}")

# if the number of cells passed is less than 200, print "ERROR: Number of cells passed is less than 200"
if num_cells_passed < 200:
    print("ERROR: Number of cells passed is less than 200")
