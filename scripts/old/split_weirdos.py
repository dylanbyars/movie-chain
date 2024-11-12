import csv
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python script.py <input_csv_file> <output_csv_file> <weirdos_csv_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    weirdos_file = sys.argv[3]

    # Now read the CSV to see how quotes are handled and split out problematic rows
    with open(input_file, mode='r', newline='') as f:
        reader = csv.reader(f)
        cleaned_rows = []
        weird_rows = []
        for row in reader:
            logging.info(f"Processing row with ID: {row[0]}")
            has_problematic_quote = any('"' in cell for cell in row)
            if has_problematic_quote:
                logging.warning(f"Problematic quotes detected in row with ID: {row[0]}")
                weird_rows.append(row)
            else:
                cleaned_rows.append(row)

    # Write cleaned rows to a new CSV file
    with open(output_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        for cleaned_row in cleaned_rows:
            writer.writerow(cleaned_row)

    # Write weird rows to a separate CSV file for further inspection
    with open(weirdos_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        for weird_row in weird_rows:
            writer.writerow(weird_row)

    logging.info(f"Cleaned CSV saved as {output_file}")
    logging.info(f"Weird rows saved as {weirdos_file}")

