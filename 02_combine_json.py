import json
import pandas as pd
import os
import glob

def extract_data(json_file):
    """Extract data from a single JSON file into a DataFrame."""
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Extract the fields we want from each entry
    entries = []
    for entry in data:
        item = {
            'title': entry.get('title', ''),
            'nid': entry.get('nid', ''),
            'type': entry.get('type', ''),  # 'winner' or 'finalist'
            'changed': entry.get('changed', ''),
            'path_alias': entry.get('path_alias', ''),
            'citation': entry.get('field_abbr_citation', {}).get('und', [{}])[0].get('safe_value', '') if entry.get('field_abbr_citation') else '',
            'category_tid': entry.get('field_category', {}).get('und', [{}])[0].get('tid', '') if entry.get('field_category') else '',
            'year_tid': entry.get('field_year', {}).get('und', [{}])[0].get('tid', '') if entry.get('field_year') else '',
            'publication': entry.get('field_publication', {}).get('und', [{}])[0].get('safe_value', '') if entry.get('field_publication') else '',
            'publisher': entry.get('field_publisher', {}).get('und', [{}])[0].get('safe_value', '') if entry.get('field_publisher') else '',
        }
        entries.append(item)
    
    return pd.DataFrame(entries)

def process_all_files():
    """Process all JSON files in both winners and finalists directories."""
    # Load years data for joining and convert tid to string
    years_df = pd.read_csv('years.csv')
    years_df['tid'] = years_df['tid'].astype(str)
    
    # Load awards data for joining and convert tid to string
    awards_df = pd.read_csv('csv/awards.csv')
    awards_df['tid'] = awards_df['tid'].astype(str)
    
    # Create paths to all JSON files
    winner_files = glob.glob('json/winners_by_year/*.json')
    finalist_files = glob.glob('json/finalists_by_year/*.json')
    
    # Process winners
    all_winners = []
    for file in winner_files:
        print(f"Processing winner file: {file}")
        df = extract_data(file)
        
        # Add the filename year as a fallback
        filename_year = os.path.basename(file).split('_')[2]
        df['filename_year'] = filename_year
        
        # Convert all columns to string type
        df = df.astype(str)
        
        all_winners.append(df)
    
    # Process finalists
    all_finalists = []
    for file in finalist_files:
        print(f"Processing finalist file: {file}")
        df = extract_data(file)
        
        # Add the filename year as a fallback
        filename_year = os.path.basename(file).split('_')[2]
        df['filename_year'] = filename_year
        
        # Convert all columns to string type
        df = df.astype(str)
        
        all_finalists.append(df)
    
    # Combine and save winners
    if all_winners:
        combined_winners = pd.concat(all_winners, ignore_index=True)
        
        # Join with years data (both sides are now strings)
        combined_winners = pd.merge(
            combined_winners,
            years_df[['tid', 'name']],
            left_on='year_tid',
            right_on='tid',
            how='left',
            suffixes=('', '_year')
        )
        
        # Rename the name column from the join to year
        combined_winners = combined_winners.rename(columns={'name': 'year'})
        
        # Join with awards data to get category names
        combined_winners = pd.merge(
            combined_winners,
            awards_df[['tid', 'name']],
            left_on='category_tid',
            right_on='tid',
            how='left',
            suffixes=('', '_category')
        )
        
        # Rename the name column from the awards join to category
        combined_winners = combined_winners.rename(columns={'name': 'category'})
        
        # Use filename_year as fallback if year is missing after join
        combined_winners['year'] = combined_winners['year'].fillna(combined_winners['filename_year'])
        
        # Save to CSV
        if not os.path.exists('csv'):
            os.makedirs('csv')
        combined_winners.to_csv('csv/all_winners.csv', index=False)
        print(f"Saved {len(combined_winners)} winners to csv/all_winners.csv")
    
    # Combine and save finalists
    if all_finalists:
        combined_finalists = pd.concat(all_finalists, ignore_index=True)
        
        # Join with years data (both sides are now strings)
        combined_finalists = pd.merge(
            combined_finalists,
            years_df[['tid', 'name']],
            left_on='year_tid',
            right_on='tid',
            how='left',
            suffixes=('', '_year')
        )
        
        # Rename the name column from the join to year
        combined_finalists = combined_finalists.rename(columns={'name': 'year'})
        
        # Join with awards data to get category names
        combined_finalists = pd.merge(
            combined_finalists,
            awards_df[['tid', 'name']],
            left_on='category_tid',
            right_on='tid',
            how='left',
            suffixes=('', '_category')
        )
        
        # Rename the name column from the awards join to category
        combined_finalists = combined_finalists.rename(columns={'name': 'category'})
        
        # Use filename_year as fallback if year is missing after join
        combined_finalists['year'] = combined_finalists['year'].fillna(combined_finalists['filename_year'])
        
        # Save to CSV
        if not os.path.exists('csv'):
            os.makedirs('csv')
        combined_finalists.to_csv('csv/all_finalists.csv', index=False)
        print(f"Saved {len(combined_finalists)} finalists to csv/all_finalists.csv")
    
    # Optionally combine both winners and finalists into one dataset
    all_entries = []
    if all_winners:
        all_entries.append(combined_winners)
    if all_finalists:
        all_entries.append(combined_finalists)
    
    if all_entries:
        combined_all = pd.concat(all_entries, ignore_index=True)
        combined_all.to_csv('csv/all_entries.csv', index=False)
        print(f"Saved {len(combined_all)} total entries to csv/all_entries.csv")

if __name__ == "__main__":
    process_all_files()