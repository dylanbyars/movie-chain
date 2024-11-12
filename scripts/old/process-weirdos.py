import csv
import re
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py <weirdos_csv_file> <problems_output_file>")
        sys.exit(1)

    weirdos_file = sys.argv[1]
    problems_file = sys.argv[2]

    problematic_substrings = []

    # Read the weird rows and extract problematic substrings
    with open(weirdos_file, mode='r', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            for cell in row:
                # Check for problematic quotes inside the cell
                if '"' in cell:
                    problematic_parts = re.split(r'"', cell)
                    for part in problematic_parts:
                        if '"' in part or len(part.strip()) > 0:
                            problematic_substrings.append(part)
                            logging.info(f"Found problematic substring: {part}")

    # Write the problematic substrings to an output file for analysis
    with open(problems_file, mode='w', newline='') as f:
        for substring in problematic_substrings:
            f.write(substring + '\n')

    logging.info(f"Problematic substrings saved as {problems_file}")
