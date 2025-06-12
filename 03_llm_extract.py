import os
import re
import json
import argparse
import multiprocessing
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from tqdm import tqdm

# Import Google Generative AI library properly
try:
    import google.generativeai as genai
    from google.generativeai import types
except ImportError:
    print("Error importing Google Generative AI. Make sure it's installed with:")
    print("pip install -U google-generativeai")
    genai = None
    types = None

def extract_entities(args):
    api_name, model_name, row_index, row = args
    
    # Create output directory structure
    output_base_dir = "entity_extraction"
    api_dir = os.path.join(output_base_dir, api_name)
    model_dir = os.path.join(api_dir, model_name)
    os.makedirs(model_dir, exist_ok=True)
    
    # Format the row index with leading zeros (padded to 3 digits)
    padded_index = str(row_index).zfill(3)
    output_file = os.path.join(model_dir, f"entity_extract_{padded_index}.json")
    
    # Check if a valid file already exists
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r') as f:
                existing_content = json.load(f)
            required_keys = ["row_index", "creators_entities", "other_entities"]
            if all(key in existing_content for key in required_keys):
                print(f"Skipping {output_file} - valid result already exists")
                return
        except (json.JSONDecodeError, FileNotFoundError):
            pass

    try:
        # Process based on API type
        if api_name == "google":
            result = process_google_entity_extraction(model_name, row_index, row)
        else:
            print(f"Unsupported API: {api_name}")
            return

        # Save result to file
        if result:
            with open(output_file, "w") as f:
                json.dump(result, f, indent=4)
            print(f"Saved result to {output_file}")
    except Exception as e:
        print(f"Error in extract_entities for row {row_index}: {str(e)}")

def process_google_entity_extraction(model_name, row_index, row):
    # Configure the Google GenAI client
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

    prompt = (
        f"Extract entities from this text: {row['prompt']}\n\n"
        "# Role and Objective\n"
        "You are a professional data analyst specializing in entity extraction from text.\n"
        "Your task is to extract entities from the given text following the instructions I give you.\n"
        "You will be given a row index and a 'citation' column which describes a work of journalism that was a finalist or winner of the Pulitzer Prize.\n"
        "\n"
        "Do the following steps:\n"
        "* Examine the text in the citation category. Using ONLY the text in the citation field, and NOT any outside information you may posses in your memory\n"
        "** Extract the names of any entities who appear to be responsible in whole or in-part for creating the work being awarded. This could include a person or persons, one or more news organizations, a funding organization, a non-profit.  For example, if \"Agence France Press\" is credited with the work or the \"Center on Crisis Reporting\" is credited with supporting the work or \"Fred Jones\" is credited with authoring a work, you should extract those. For this part of the task, Do NOT include any entities that are related to the content of the journalism work. For example, if the citation says the awarded work was about the impeachment of President Clinton by Congress, you would NOT extract Clinton or Congress. Extract all relevant entries in a single, comma separated list and return it in the 'creator_entities' json key as described below. If there are no relevant entities -- as will probably be the case for most of them -- just put NA.\n"
        "** Extract the names of any other entities that appear in the citation that you did not include under creator_entities.  For example, if the citation says the awarded work was about the impeachment of President Clinton by Congress, you would extract Clinton or Congress and include it here. Return it as a comma separated list in the 'other_entities' json key as described below. If there are no relevant entities here, just put NA.\n"
        "\n"
        "## Output format\n"
        "-- The output should be in valid JSON format using this schema:\n"
        "{\n"
        '    "row_index": "row index here",\n'
        '    "creators_entities": "list of creator entities here",\n'
        '    "other_entities": "list of other entities here"\n'
        "}\n"
        "--Do not exclude any of the keys I've provided to you in the schema, and don't add any new keys.\n"
    )

    try:
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(prompt)
        response_text = response.text

        # Clean up response text if it's in a Markdown code block
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned)
            cleaned = re.sub(r"\n?```$", "", cleaned)
        try:
            result = json.loads(cleaned)
            result["row_index"] = row_index
            return result
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for row {row_index}: {e}")
            print("Raw response:", response_text)
            return None

    except Exception as e:
        print(f"Error processing row {row_index} with Google GenAI: {e}")
        return None
        return None


def bind_json_to_csv(bind_folder, output_csv):
    import glob

    json_files = glob.glob(os.path.join(bind_folder, "*.json"))
    rows = []
    for jf in json_files:
        with open(jf, "r") as f:
            try:
                rows.append(json.load(f))
            except Exception as e:
                print(f"Error reading {jf}: {e}")
    if not rows:
        print("No valid JSON files found to bind.")
        return
    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False)
    print(f"Bound {len(rows)} JSON files to {output_csv}")



def main():
    parser = argparse.ArgumentParser(description="Extract entities from text using various LLM APIs.")
    parser.add_argument("--api", type=str, required=False, choices=["google", "openai", "meta"], 
                        help="The API to use (google, openai, or meta).")
    parser.add_argument("--model", type=str, required=False, 
                        help="The name of the model to use (e.g., gemini-1.5-pro, gemini-2.0-flash).")
    parser.add_argument("--limit", type=int, default=None, 
                        help="Limit the number of rows to process from the DataFrame.")
    parser.add_argument("--workers", type=int, default=1, 
                        help="Number of parallel workers. Defaults to 1 to avoid multiprocessing issues.")
    parser.add_argument("--bind", action="store_true", help="Bind all JSON files in the specified model folder and export as CSV.")
    parser.add_argument("--bind_folder", type=str, default="entity_extraction/gemini-2.5-flash-preview-04-17", help="Folder to bind JSON files from.")
    parser.add_argument("--output_csv", type=str, default="entity_extraction/bound_output.csv", help="Output CSV file path for binding.")
    args = parser.parse_args()

    if args.bind:
        bind_json_to_csv(args.bind_folder, args.output_csv)
        return
    
    # Set number of workers
    num_workers = args.workers
    
    # Load data directly from Google Sheets
    try:
        sheet_url = "https://docs.google.com/spreadsheets/d/1XxOPWj7SP9CaFct3oqeWpvX_0jtu9R-gjCZd_FRP8DM/export?format=csv&gid=1719538348"
        df = pd.read_csv(sheet_url)
        # Keep only 'citation' column and reset index
        df = df.reset_index()[['index', 'citation']]
        df = df.rename(columns={'index': 'row_index'})
        print(f"Successfully loaded {len(df)} rows from Google Sheet")
        
        # Check if the required column exists
        if 'citation' not in df.columns:
            print(f"Error: The required column 'citation' is missing from the Google Sheet")
            print(f"The available columns are: {', '.join(df.columns)}")
            return
            
        # Create a 'prompt' column from the 'citation' column if it doesn't exist
        if 'prompt' not in df.columns:
            df['prompt'] = df['citation']
            print("Created 'prompt' column from 'citation' column")
            
    except Exception as e:
        print(f"Error loading data from Google Sheet: {e}")
        return
    
    # Limit the DataFrame rows if a limit is provided
    if args.limit:
        df_limited = df.head(args.limit)
        print(f"Processing {len(df_limited)} out of {len(df)} rows (limit={args.limit})")
    else:
        df_limited = df
        print(f"Processing all {len(df)} rows")
    
    # Create a list of arguments for parallel processing
    process_args = [(args.api, args.model, idx, row) for idx, row in df_limited.iterrows()]
    
        # ...
    if num_workers > 1:
        print(f"Processing with {num_workers} workers")
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            list(tqdm(executor.map(extract_entities, process_args), total=len(process_args), desc="Extracting entities"))
    else:
        print("Processing rows sequentially (multiprocessing disabled)")
        for proc_arg in tqdm(process_args, desc="Extracting entities"):
            extract_entities(proc_arg)

    print(f"Processing complete. Results saved in entity_extraction/{args.api}/{args.model}/")

if __name__ == "__main__":
    main()





