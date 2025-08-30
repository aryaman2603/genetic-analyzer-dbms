import os
import csv
from Bio import SeqIO  # The key library for parsing FASTA files

# --- Configuration ---
# The name of your large FASTA file inside the 'data' folder
INPUT_FASTA_PATH = os.path.join('data', 'sequence.fasta') 
# The name of the clean CSV file we will create
OUTPUT_CSV_PATH = os.path.join('data', 'processed_sequences.csv') 
# The maximum number of characters to keep from each sequence
SEQUENCE_LENGTH_LIMIT = 100000

def process_fasta_file():
    """
    Parses a large FASTA file, generates an integer ID, truncates 
    sequences, and writes the results to a new, robustly-quoted CSV file.
    """
    try:
        print(f"Opening input FASTA file: {INPUT_FASTA_PATH}")
        with open(INPUT_FASTA_PATH, mode='r') as infile, \
             open(OUTPUT_CSV_PATH, mode='w', newline='') as outfile:

            # Create a CSV writer that quotes ALL fields for consistency
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            
            # Write the header row that matches the database schema
            writer.writerow(['genome_id', 'description', 'sequence'])
            
            print("Processing FASTA records...")
            genome_id_counter = 0
            
            # SeqIO.parse is a memory-efficient generator.
            for record in SeqIO.parse(infile, "fasta"):
                # Increment our integer ID for each new record
                genome_id_counter += 1
                
                # Extract the full sequence as a string
                sequence_str = str(record.seq)
                
                # Truncate the sequence to the defined limit
                truncated_sequence = sequence_str[:SEQUENCE_LENGTH_LIMIT]
                
                # Write the processed data to the CSV file
                # Use our new integer ID and the full FASTA description
                writer.writerow([genome_id_counter, record.description, truncated_sequence])
                
                print(f"Processed {genome_id_counter} records...", end='\r')

            print(f"\n✅ Successfully processed {genome_id_counter} records.")
            print(f"Clean CSV created at: {OUTPUT_CSV_PATH}")

    except FileNotFoundError:
        print(f"❌ Error: Input file not found at '{INPUT_FASTA_PATH}'")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == '__main__':
    process_fasta_file()