import pandas as pd
import os
import sys

original_datasets_path = os.path.join("../../contents")
augmented_datasets_path = os.path.join("../../contents/augmented-specifics")
input_filename = "historical_stock_prices.csv"  # "test_shuffle.csv"  # #
input_file_path = os.path.join(original_datasets_path, input_filename)
iterations = 3

if len(sys.argv) > 1:
    iterations = int(sys.argv[1])

output_filename = input_filename.split(".")[0] + "_" + str(iterations) + "x." + input_filename.split(".")[1]

output_file_path = os.path.join(augmented_datasets_path, output_filename)

""" Augment File """
hsp = pd.read_csv(input_file_path)
dataframes = [hsp]
i = 1
while i <= iterations:
    pd2 = pd.DataFrame.copy(hsp)
    dataframes.append(pd2)
    i += 1

conc = pd.concat(dataframes, ignore_index=True)
conc.to_csv(output_file_path, index=False, sep=",", line_terminator="\n")