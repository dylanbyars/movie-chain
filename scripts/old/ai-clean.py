import csv
import requests
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to send rows with problematic quotes to an AI for cleaning
def clean_with_ai(row):
    logging.info(f"Sending row to AI for cleaning: Row ID: {row[0]}")
    prompt = f"Please clean and reformat the following CSV row to properly align with the schema, removing any problematic internal quotes: {row}"
    response = requests.post(
        "http://localhost:11434/api/generate",
        headers={
            "Content-Type": "application/json"
        },
        json={
            "model": "llama3",
            "prompt": prompt,
            "stream": False
        }
    )
    if response.status_code == 200:
        cleaned_row = response.json().get('response', '').strip()
        logging.info(f"Cleaned row received for Row ID {row[0]}")
        return cleaned_row
    else:
        logging.error(f"Error in AI response: {response.status_code}, {response.text}")
        raise Exception(f"Error in AI response: {response.status_code}, {response.text}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py <input_csv_file> <output_csv_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Now read the CSV to see how quotes are handled and send problematic rows to the AI
    with open(input_file, mode='r', newline='') as f:
        reader = csv.reader(f)
        cleaned_rows = []
        for row in reader:
            logging.info(f"Processing row with ID: {row[0]}")
            has_problematic_quote = any('"' in cell for cell in row)
            if has_problematic_quote:
                logging.warning(f"Problematic quotes detected in row with ID: {row[0]}")
                cleaned_row = clean_with_ai(row)
                cleaned_rows.append(cleaned_row)
            else:
                cleaned_rows.append(','.join(row))

    # Write cleaned rows to a new CSV file
    with open(output_file, mode='w', newline='') as f:
        for cleaned_row in cleaned_rows:
            f.write(cleaned_row + '\n')

    logging.info(f"Cleaned CSV saved as {output_file}")

