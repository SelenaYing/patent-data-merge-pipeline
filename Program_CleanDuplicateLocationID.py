# %%
import pandas as pd

# Input and output file paths
input_file = '/Users/selenaying/g_location_not_disambiguated.tsv'
output_file = '/Users/selenaying/g_location_clean_deduplicated.tsv'

# Set to keep track of unique (raw_city, location_id) pairs
seen_pairs = set()

# Flag to control writing header only once
header_written = False

# Open output file in write mode
with open(output_file, 'w', encoding='utf-8') as out_file:
    # Read the input file in chunks
    for chunk in pd.read_csv(input_file, sep='\t', chunksize=500000, dtype=str):
        # Drop duplicates within this chunk
        chunk = chunk.drop_duplicates(subset=['raw_city', 'location_id'])

        # Filter out rows already seen
        is_new = chunk.apply(
            lambda row: (row['raw_city'], row['location_id']) not in seen_pairs, axis=1
        )
        new_rows = chunk[is_new]

        # Update seen set
        seen_pairs.update((row['raw_city'], row['location_id']) for _, row in new_rows.iterrows())

        # Write to file
        new_rows.to_csv(out_file, sep='\t', index=False, header=not header_written, mode='a')
        header_written = True



