############################
### Clinical Data Confid ###
############################

slide_scanning_tracker_path = "/media/hdd3/neo/clinical_text_data/SST.xlsx"
slide_scanning_tracker_sheet_name = "Sheet1"
status_results_path = "/media/hdd3/neo/clinical_text_data/status_results.csv"
bma_info_path = (
    "/media/hdd3/neo/clinical_text_data/hemeParser11_bm_diff_202404032109.csv"
)

############################
### Biology Assumptions ####
############################

cellnames = [
    "B1",
    "B2",
    "E1",
    "E4",
    "ER1",
    "ER2",
    "ER3",
    "ER4",
    "ER5",
    "ER6",
    "L2",
    "L4",
    "M1",
    "M2",
    "M3",
    "M4",
    "M5",
    "M6",
    "MO2",
    "PL2",
    "PL3",
    "U1",
    "U4",
]

differential_group_dict = {
    "blasts": ["M1"], #, "M2", "ER1"],
    "blast-equivalents": [],
    "promyelocytes": ["M2"],
    "myelocytes": ["M3"],
    "metamyelocytes": ["M4"],
    "neutrophils/bands": ["M5", "M6"],
    "monocytes": ["MO2"],
    "eosinophils": ["E1", "E4"],
    "erythroid precursors": ["ER1", "ER2", "ER3", "ER4"],
    "lymphocytes": ["L2"],
    "plasma cells": ["L4"],
}
BMA_final_classes = list(differential_group_dict.keys())

omitted_classes = ["B1", "B2"]
removed_classes = ["U1", "PL2", "PL3", "ER5", "ER6", "U4"]
