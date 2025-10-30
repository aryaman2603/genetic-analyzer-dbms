import streamlit as st
import pandas as pd
import db_utils
import analysis_functions
from psycopg2.extras import execute_batch # Import the high-performance tool

# --- Page Configuration ---
st.set_page_config(
    page_title="Genetic Sequence Analyzer",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Database Connection ---
try:
    db_utils.init_connection_pool()
except Exception as e:
    st.error(f"Failed to connect to the database. Please check your .env file and connection. Error: {e}")
    st.stop()

# --- Caching Functions ---
@st.cache_data
def load_genome_list():
    """Fetches all genome IDs and descriptions from the database."""
    conn = db_utils.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT genome_id, description FROM genomes ORDER BY genome_id;")
            genomes = cur.fetchall()
            return [(f"{desc} (ID: {gid})", gid) for gid, desc in genomes]
    finally:
        db_utils.release_connection(conn)

@st.cache_data
def load_saved_patterns():
    """Fetches all saved patterns from the database."""
    conn = db_utils.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pattern_name, regex_pattern FROM patterns ORDER BY pattern_name;")
            patterns = cur.fetchall()
            return patterns
    finally:
        db_utils.release_connection(conn)

# --- Main Application UI ---
st.title("🧬 Genetic Sequence Analyzer")
st.markdown("An interactive tool to search for patterns and compare genomic variations using a live Supabase database.")

st.sidebar.title("Navigation")
tool_choice = st.sidebar.radio("Choose an Analysis Tool", ["Pattern Search", "Variation Comparison"])
st.sidebar.markdown("---")


# --- Tool 1: Pattern Search ---
if tool_choice == "Pattern Search":
    st.header("🔬 Pattern Search Tool")
    
    genome_list = load_genome_list()
    saved_patterns = load_saved_patterns()

    col1, col2 = st.columns(2)

    with col1:
        selected_genome_desc, selected_genome_id = st.selectbox(
            "1. Select a Genome to Search",
            options=genome_list,
            format_func=lambda x: x[0]
        )

    with col2:
        use_saved = st.checkbox("Use a saved pattern?")
        if use_saved:
            selected_pattern_name, selected_pattern_regex = st.selectbox(
                "2. Select a Saved Pattern",
                options=saved_patterns,
                format_func=lambda x: f"{x[0]} ({x[1]})"
            )
            pattern_to_search = selected_pattern_regex
        else:
            pattern_to_search = st.text_input("2. Or Enter a Custom Regex Pattern", value="GAATTC")

    if st.button("Search for Pattern", type="primary"):
        if not pattern_to_search:
            st.warning("Please enter a pattern to search for.")
        else:
            with st.spinner(f"Searching for '{pattern_to_search}' in genome {selected_genome_id}..."):
                conn = db_utils.get_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT sequence FROM genomes WHERE genome_id = %s;", (selected_genome_id,))
                        sequence = cur.fetchone()[0]
                    matches = analysis_functions.find_patterns_regex(sequence, pattern_to_search)
                    st.success(f"Found {len(matches)} match(es)!")
                    if matches:
                        df = pd.DataFrame(matches)
                        df.index += 1
                        df.rename(columns={'start': 'Start Position', 'end': 'End Position', 'matched_sequence': 'Matched Sequence'}, inplace=True)
                        st.dataframe(df)
                except Exception as e:
                    st.error(f"An error occurred during search: {e}")
                finally:
                    db_utils.release_connection(conn)


# --- Tool 2: Variation Comparison ---
elif tool_choice == "Variation Comparison":
    st.header("🔄 Variation Comparison Tool")
    
    # Use Streamlit's session state to hold all persistent data
    if 'variations' not in st.session_state:
        st.session_state.variations = None
        st.session_state.ref_seq_len = 0
        st.session_state.comp_seq_len = 0
        st.session_state.comp_genome_id_to_log = None

    genome_list = load_genome_list()
    col1, col2 = st.columns(2)
    with col1:
        ref_genome_desc, ref_genome_id = st.selectbox("1. Select Reference Genome", options=genome_list, format_func=lambda x: x[0])
    with col2:
        comp_genome_desc, comp_genome_id = st.selectbox("2. Select Comparison Genome", options=genome_list, format_func=lambda x: x[0], index=1)
    
    if st.button("Compare Genomes", type="primary"):
        st.session_state.variations = None # Clear previous results
        if ref_genome_id == comp_genome_id:
            st.warning("Please select two different genomes to compare.")
        else:
            with st.spinner(f"Comparing genome {ref_genome_id} with {comp_genome_id}..."):
                conn = db_utils.get_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT sequence FROM genomes WHERE genome_id = %s;", (ref_genome_id,))
                        ref_sequence = cur.fetchone()[0]
                        cur.execute("SELECT sequence FROM genomes WHERE genome_id = %s;", (comp_genome_id,))
                        comp_sequence = cur.fetchone()[0]
                    
                    # Store results AND other needed data in session state
                    st.session_state.variations = analysis_functions.detect_mutations_simple(ref_sequence, comp_sequence)
                    # *** FIX 1: Save the lengths and ID to session state ***
                    st.session_state.ref_seq_len = len(ref_sequence)
                    st.session_state.comp_seq_len = len(comp_sequence)
                    st.session_state.comp_genome_id_to_log = comp_genome_id

                except Exception as e:
                    st.error(f"An error occurred during comparison: {e}")
                finally:
                    db_utils.release_connection(conn)

    # --- Display Results and Logging Button ---
    if st.session_state.variations is not None:
        variations = st.session_state.variations
        st.success(f"Comparison complete! Found {len(variations)} variations.")
        
        # *** FIX 2: Use the saved lengths from session state ***
        seq_len = min(st.session_state.ref_seq_len, st.session_state.comp_seq_len)
        
        variation_rate = (len(variations) / seq_len) * 100 if seq_len > 0 else 0
        st.metric(label="Variation Rate", value=f"{variation_rate:.2f}%")

        if variations:
            st.markdown("### Preview of Variations Found:")
            df = pd.DataFrame(variations)
            st.dataframe(df.head(100))

            st.markdown("---")
            if st.button("Log these variations to the database"):
                with st.spinner(f"Logging {len(variations)} variations using high-performance batch insert..."):
                    conn = db_utils.get_connection()
                    try:
                        with conn.cursor() as cur:
                            # *** FIX 3: Use the saved ID from session state ***
                            comp_genome_id = st.session_state.comp_genome_id_to_log
                            mutations_to_log = [(comp_genome_id, v['type'], v['position'], v['original'], v['mutated']) for v in variations]
                            sql_template = "INSERT INTO mutations (genome_id, mutation_type, position, original_sequence, mutated_sequence) VALUES (%s, %s, %s, %s, %s)"
                            
                            execute_batch(cur, sql_template, mutations_to_log)
                            
                            conn.commit()
                            st.success("✅ Transaction successful. All variations logged to the database!")
                            st.balloons()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Transaction failed. Rolling back. Error: {e}")
                    finally:
                        db_utils.release_connection(conn)


    
