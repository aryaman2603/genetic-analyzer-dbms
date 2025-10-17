import db_utils
import analysis_functions
from psycopg2.extras import execute_batch # Import the high-performance tool

def setup_patterns():
    """
    Ensures some sample patterns exist in the 'patterns' table for testing.
    """
    print("\n--- Step 1: Setting up Sample Patterns ---")
    conn = db_utils.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO patterns (pattern_name, regex_pattern, description) VALUES
                ('Bacterial TATA Box', 'TATA[AT]A[AT]', 'A common promoter sequence (TATA box) found in bacteria.'),
                ('EcoRI Restriction Site', 'GAATTC', 'The specific recognition site for the EcoRI restriction enzyme.'),
                ('Shine-Dalgarno Sequence', 'AGGAGG', 'A ribosomal binding site in bacterial mRNA.')
                ON CONFLICT (pattern_name) DO NOTHING;
            """)
            print("Checked for sample patterns in 'patterns' table.")
            
            conn.commit()
    finally:
        db_utils.release_connection(conn)

def search_and_log_patterns(genome_id_to_search: int):
    """Searches for all known patterns in a given sequence and logs any matches."""
    print(f"\n--- Step 2: Searching for Patterns in Genome ID: {genome_id_to_search} ---")
    conn = db_utils.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sequence FROM genomes WHERE genome_id = %s;", (genome_id_to_search,))
            result = cur.fetchone()
            if not result:
                print(f"Genome ID {genome_id_to_search} not found.")
                return
            sequence = result[0]

            cur.execute("SELECT pattern_id, regex_pattern, pattern_name FROM patterns;")
            all_patterns = cur.fetchall()

            for pattern_id, regex_pattern, pattern_name in all_patterns:
                matches = analysis_functions.find_patterns_regex(sequence, regex_pattern)
                if matches:
                    print(f"Found {len(matches)} match(es) for pattern '{pattern_name}'")
                    for match in matches:
                        cur.execute("""
                            INSERT INTO search_results (genome_id, pattern_id, match_start, match_end, matched_sequence)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (genome_id_to_search, pattern_id, match['start'], match['end'], match['matched_sequence']))
            conn.commit()
            print("✅ Pattern search and logging complete.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred during pattern search: {e}")
    finally:
        db_utils.release_connection(conn)

def compare_and_log_mutations(ref_genome_id: int, comp_genome_id: int):
    """
    Fetches two genomes, compares them, and logs mutations using a single,
    high-performance batch insert operation.
    """
    print(f"\n--- Step 3: Comparing Genome {ref_genome_id} (Ref) with {comp_genome_id} ---")
    conn = db_utils.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sequence FROM genomes WHERE genome_id = %s;", (ref_genome_id,))
            ref_sequence = cur.fetchone()[0]
            cur.execute("SELECT sequence FROM genomes WHERE genome_id = %s;", (comp_genome_id,))
            comp_sequence = cur.fetchone()[0]

            mutations = analysis_functions.detect_mutations_simple(ref_sequence, comp_sequence)
            
            if not mutations:
                print("No mutations found between the two genomes.")
                return

            print(f"Found {len(mutations)} mutation(s). Preparing high-performance batch insert.")
            
            # --- START OF EFFICIENT BATCH INSERT ---
            # 1. Prepare all the data in a list of tuples
            mutations_to_log = []
            for mutation in mutations:
                mutations_to_log.append((
                    comp_genome_id, 
                    mutation['type'], 
                    mutation['position'], 
                    mutation['original'], 
                    mutation['mutated']
                ))

            # 2. Define the SQL template
            sql_template = """
                INSERT INTO mutations (genome_id, mutation_type, position, original_sequence, mutated_sequence)
                VALUES (%s, %s, %s, %s, %s)
            """

            # 3. Execute the batch operation once
            execute_batch(cur, sql_template, mutations_to_log)
            # --- END OF EFFICIENT BATCH INSERT ---

            conn.commit()
            print("✅ Transaction successful. All mutations logged efficiently.")

    except Exception as e:
        print(f"❌ Transaction failed. Rolling back changes. Error: {e}")
        if conn:
            conn.rollback()
    finally:
        db_utils.release_connection(conn)

def main():
    """Main function to run the focused analysis workflow."""
    try:
        db_utils.init_connection_pool()
        #setup_patterns()
        search_and_log_patterns(genome_id_to_search=3)
        compare_and_log_mutations(ref_genome_id=1, comp_genome_id=3)

        print("\n--- Analysis Pipeline Finished ---")
        print("You can now view the results in your Supabase dashboard.")

    except Exception as e:
        print(f"\n A critical error occurred in the main script: {e}")
    finally:
        db_utils.close_connection_pool()

if __name__ == '__main__':
    main()