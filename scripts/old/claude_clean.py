#!/usr/bin/env python3
import csv
import argparse
from pathlib import Path
import logging
from typing import List, Dict, Iterator
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CSVNormalizer:
    def __init__(self, desired_headers: List[str], chunk_size: int = 10000):
        self.desired_headers = desired_headers
        self.chunk_size = chunk_size

    def read_chunks(self, file_path: Path) -> Iterator[List[Dict]]:
        """Read the CSV file in chunks."""
        current_chunk = []
        
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            # Try to detect the dialect
            sample = f.read(1024)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                logging.warning("Could not detect CSV dialect, using default.")
                dialect = csv.excel

            # Try to detect if there's a header
            has_header = csv.Sniffer().has_header(sample)
            reader = csv.DictReader(f) if has_header else csv.DictReader(f, fieldnames=self.desired_headers)

            for row in reader:
                current_chunk.append(row)
                if len(current_chunk) >= self.chunk_size:
                    yield current_chunk
                    current_chunk = []
            
            if current_chunk:
                yield current_chunk

    def normalize_chunk(self, chunk: List[Dict]) -> List[Dict]:
        """Normalize a chunk of data to match desired headers."""
        normalized = []
        for row in chunk:
            normalized_row = {}
            for header in self.desired_headers:
                # Look for the header in a case-insensitive way
                matching_key = next(
                    (k for k in row.keys() if k and k.lower().strip() == header.lower()),
                    None
                )
                normalized_row[header] = row.get(matching_key, '').strip() if matching_key else ''
            normalized.append(normalized_row)
        return normalized

    def process_file(self, input_path: Path, output_path: Path):
        """Process the entire file chunk by chunk."""
        total_rows = 0
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.desired_headers)
            writer.writeheader()

            for i, chunk in enumerate(self.read_chunks(input_path)):
                normalized_chunk = self.normalize_chunk(chunk)
                writer.writerows(normalized_chunk)
                total_rows += len(normalized_chunk)
                logging.info(f"Processed chunk {i+1}: {len(normalized_chunk)} rows (Total: {total_rows})")

def main():
    parser = argparse.ArgumentParser(description='Normalize a malformed CSV file')
    parser.add_argument('input_file', type=str, help='Input CSV file path')
    parser.add_argument('output_file', type=str, help='Output CSV file path')
    parser.add_argument('--headers', type=str, required=True, 
                      help='Desired headers (comma-separated)')
    parser.add_argument('--chunk-size', type=int, default=10000,
                      help='Number of rows to process at once (default: 10000)')
    
    args = parser.parse_args()
    
    desired_headers = [h.strip() for h in args.headers.split(',')]
    input_path = Path(args.input_file)
    output_path = Path(args.output_file)
    
    if not input_path.exists():
        logging.error(f"Input file {input_path} does not exist")
        sys.exit(1)
        
    normalizer = CSVNormalizer(desired_headers, args.chunk_size)
    
    try:
        normalizer.process_file(input_path, output_path)
        logging.info(f"Successfully normalized CSV file: {output_path}")
    except Exception as e:
        logging.error(f"Error processing file: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
