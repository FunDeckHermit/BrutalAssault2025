# runs in crontab with */5 * * * * /bin/python3 /people/fun/brutalassaultscraper/scrape.py -d /srv/gossafileshare/brutalassault/schedule_by_stage.json

import requests
from datetime import datetime
import json
import argparse # Import argparse for command-line argument parsing

def fetch_and_extract_schedule_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None
    except ValueError as e: # Includes JSONDecodeError
        print(f"Error decoding JSON: {e}")
        return None

def timestamp_to_iso8601(timestamp_ms):
    """Converts a Unix timestamp in milliseconds to ISO 8601 format."""
    if timestamp_ms is None:
        return None
    try:
        timestamp_s = timestamp_ms / 1000.0
        dt = datetime.fromtimestamp(timestamp_s)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError) as e:
        print(f"Error converting timestamp {timestamp_ms}: {e}")
        return None

def extract_stage_name(stage_info):
    """Extracts the stage name, preferring English if available."""
    if not stage_info:
        return None
    localized_stages = stage_info.get('localized', [])
    # Try to find the English name first
    en_name_entry = next((s for s in localized_stages if s.get('language') == 'EN'), None)
    if en_name_entry:
        return en_name_entry.get('name')
    # Fallback to the first available name
    if localized_stages:
        return localized_stages[0].get('name')
    return None

def extract_schedule_info(schedule_data):
    if not schedule_data or 'schedules' not in schedule_data:
        print("Invalid data format received.")
        return {}

    # Dictionary to hold stages, keyed by stage name
    stages_dict = {}

    for item in schedule_data['schedules']:
        try:
            schedule_id = item.get('id')
            date_from_ms = item.get('date_from')
            date_to_ms = item.get('date_to')
            
            stage_info = item.get('stage')
            stage_name = extract_stage_name(stage_info)
            
            # If we can't determine a stage name, skip this item or use a default key
            if not stage_name:
                stage_name = "Unknown Stage"
                print(f"Warning: Could not determine stage name for schedule ID {schedule_id}. Using '{stage_name}'.")

            artist_info = item.get('artist', {})
            artist_name = artist_info.get('name') if artist_info else None
            
            # Safely get the English localized artist info
            en_artist_info = None
            if artist_info and 'localized' in artist_info:
                 en_artist_info = next((loc_item for loc_item in artist_info['localized'] if loc_item.get('language') == 'EN'), None)

            # Check for critical data before processing
            if schedule_id is not None and date_from_ms is not None and artist_name:
                iso_date_from = timestamp_to_iso8601(date_from_ms)
                iso_date_to = timestamp_to_iso8601(date_to_ms)
                
                # Get genre from English localized info if available
                genre = en_artist_info.get('genre') if en_artist_info else None 
                
                # Create the entry for this performance
                performance_entry = {
                    'id': schedule_id,
                    'date_from_iso8601': iso_date_from,
                    'date_to_iso8601': iso_date_to,
                    'artist_name': artist_name,
                    'genre': genre # Include genre, even if it's None
                }

                # Add to the appropriate stage array in the dictionary
                if stage_name not in stages_dict:
                    stages_dict[stage_name] = []
                stages_dict[stage_name].append(performance_entry)
                
            else:
                # Silently skip items with missing critical data
                # print(f"Skipping item with missing data: id={schedule_id}, date_from={date_from_ms}, artist_name={artist_name}")
                pass
                
        except Exception as e:
            print(f"Error processing schedule item ID {item.get('id', 'Unknown')}: {e}")
            continue # Continue processing other items

    return stages_dict

def main():
    # Create the parser
    parser = argparse.ArgumentParser(description="Fetch and process festival schedule data.")
    
    # Add the destination argument with a default value
    parser.add_argument(
        '-d', '--destination',
        type=str,
        default='schedule_by_stage.json',
        help='Destination filename for the output JSON. (default: schedule_by_stage.json)'
    )
    
    # Parse the arguments
    args = parser.parse_args()
    
    # Use the destination argument
    output_filename = args.destination

    url = "https://admin.best4fest.app/api/v3/ba2025/schedule"

    print("Fetching schedule data...")
    raw_data = fetch_and_extract_schedule_data(url)

    if raw_data:
        print("Extracting information...")
        stages_data = extract_schedule_info(raw_data)

        if stages_data:
            result_json = stages_data
            
            print(f"\nExtracted data for {len(stages_data)} stages.")
            print("Information in JSON format (showing structure):")
            print("-" * 50)
            
            # Pretty print the structure for clarity (optional, can be removed)
            # print(json.dumps(result_json, indent=2, ensure_ascii=False))

            # Save to the specified file
            # output_filename is now determined by the command-line argument
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(result_json, f, indent=2, ensure_ascii=False)
            print(f"\nData saved to '{output_filename}'")

        else:
            print("No valid schedule information extracted.")
            result_json = {}
            
    else:
        print("Failed to retrieve or parse schedule data.")
        result_json = {"error": "Failed to retrieve or parse schedule data."}

if __name__ == "__main__":
    main()