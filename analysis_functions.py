import re
from typing import List, Dict, Union

def find_patterns_regex(sequence: str, regex_pattern: str) -> List[Dict[str, Union[int, str]]]:
    """
    Finds all occurrences of a regex pattern in a sequence.

    This function uses `re.finditer` for memory efficiency, which is ideal
    for searching large sequences without loading all matches into memory at once.

    Args:
        sequence: The genetic sequence string to search within.
        regex_pattern: The regular expression to match.

    Returns:
        A list of dictionaries. Each dictionary represents a match and
        contains the 'start' index, 'end' index, and the 'matched_sequence'.
        Returns an empty list if no matches are found or if the regex is invalid.
    """
    matches = []
    try:
        for match in re.finditer(regex_pattern, sequence):
            matches.append({
                'start': match.start(),
                'end': match.end(),
                'matched_sequence': match.group(0)
            })
    except re.error as e:
        print(f"An error occurred with the regex pattern '{regex_pattern}': {e}")
    return matches

def detect_mutations_simple(reference_sequence: str, comparison_sequence: str) -> List[Dict[str, Union[int, str]]]:
    """
    Performs a simple character-by-character comparison to find mutations.

    This basic method is effective for identifying substitutions in aligned
    sequences and large insertions/deletions at the end. It does not perform
    complex sequence alignment, so it's best used for comparing sequences
    that are expected to be highly similar.

    Args:
        reference_sequence: The original sequence to use as a baseline.
        comparison_sequence: The sequence to compare against the reference.

    Returns:
        A list of dictionaries, where each dictionary describes a detected
        mutation with its 'type', 'position', 'original' bases, and 'mutated' bases.
    """
    mutations = []
    ref_len, comp_len = len(reference_sequence), len(comparison_sequence)
    min_len = min(ref_len, comp_len)

    # 1. Check for substitutions in the overlapping part of the sequences
    for i in range(min_len):
        if reference_sequence[i] != comparison_sequence[i]:
            mutations.append({
                'type': 'substitution',
                'position': i,
                'original': reference_sequence[i],
                'mutated': comparison_sequence[i]
            })

    # 2. Check for a large insertion or deletion at the end
    if comp_len > ref_len:
        mutations.append({
            'type': 'insertion',
            'position': min_len,
            'original': '',
            'mutated': comparison_sequence[min_len:]
        })
    elif ref_len > comp_len:
        mutations.append({
            'type': 'deletion',
            'position': min_len,
            'original': reference_sequence[min_len:],
            'mutated': ''
        })
        
    return mutations

def calculate_gc_content(sequence: str) -> float:
    """
    Calculates the GC content (percentage of Guanine and Cytosine) of a sequence.

    GC content is a key characteristic of a genome and is often used in
    bioinformatics for various analyses.

    Args:
        sequence: The genetic sequence string.

    Returns:
        The GC content as a percentage, or 0.0 if the sequence is empty.
    """
    if not sequence:
        return 0.0
    
    # Ensure sequence is uppercase for case-insensitive counting
    sequence = sequence.upper()
    
    g_count = sequence.count('G')
    c_count = sequence.count('C')
    
    gc_percentage = ((g_count + c_count) / len(sequence)) * 100
    
    return round(gc_percentage, 2)