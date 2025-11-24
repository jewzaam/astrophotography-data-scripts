#!/usr/bin/env python3
"""
Extract profile data from NINA profile files and merge into database-config.json.

This script:
- Reads NINA profile files (.profile)
- Extracts profile_id, profile_name, and filters
- Merges into database-config.json without deleting existing fields
- Creates config file if it doesn't exist
- Creates default filter_order and filter_configuration entries
"""

import hashlib
import json
import os
import sys
from datetime import date
import xmltodict
import yaml

import common

CONFIG_FILE = "database-config.json"

# Broadband filters (in priority order)
BROADBAND_FILTERS = ["UVIR", "L", "R", "G", "B"]

# Narrowband filters (in priority order - O, S, H)
NARROWBAND_FILTERS = ["O", "S", "H"]


def create_default_config():
    """Create a default empty configuration structure."""
    return {
        "locations": [],
        "profiles": [],
        "scheduler": {
            "project_defaults": {
                "state": 1,
                "minimumtime": 30,
                "minimumaltitude": 0,
                "usecustomhorizon": 1,
                "horizonoffset": 0,
                "meridianwindow": 0,
                "filterswitchfrequency": 0,
                "enablegrader": 0
            },
            "target_defaults": {
                "active": 1,
                "epochcode": 2,
                "roi": 100
            }
        }
    }


def load_config():
    """Load existing config or create default if it doesn't exist."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        print(f"Config file {CONFIG_FILE} does not exist. Creating new one.")
        return create_default_config()


def save_config(config):
    """Save config to file."""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)
    print(f"Saved configuration to {CONFIG_FILE}")


def extract_filters_from_profile(xml_data):
    """Extract filter names from profile XML data."""
    filters = []
    if "FilterWheelSettings" in xml_data["Profile"]:
        filter_settings = xml_data["Profile"]["FilterWheelSettings"]
        if "FilterWheelFilters" in filter_settings and "a:FilterInfo" in filter_settings["FilterWheelFilters"]:
            infos = filter_settings["FilterWheelFilters"]["a:FilterInfo"]
            if type(infos) is not list:
                infos = [infos]
            
            for info in infos:
                f = common.normalize_filterName(info['a:_name'])
                # skip any "DARK" filter
                if f.upper().startswith("DARK"):
                    continue
                # skip any "BLANK" filter
                if f.upper().startswith("BLANK"):
                    continue
                filters.append(f)
    return filters


def create_filter_order(filters):
    """
    Create filter order based on filter wheel with special case for narrowband.
    """
    ordered = ",".join(filters)
    
    # Special case: if we have L,R,G,B,S,H,O, reorder to L,R,G,B,O,S,H
    if ordered == "L,R,G,B,S,H,O":
        ordered = "L,R,G,B,O,S,H"
    
    return ordered


def find_active_filter_configuration(filter_configurations):
    """Find the active filter configuration (valid_to is null, or most recent if multiple)."""
    if not filter_configurations:
        return None
    
    # Find configurations with valid_to = null
    active_configs = [cfg for cfg in filter_configurations if cfg.get("valid_to") is None]
    
    if not active_configs:
        # No active config, return the most recent one (highest valid_from)
        return max(filter_configurations, key=lambda x: x.get("valid_from", ""))
    
    if len(active_configs) == 1:
        return active_configs[0]
    
    # Multiple active configs - return the one with the latest valid_from
    return max(active_configs, key=lambda x: x.get("valid_from", ""))


def create_filter_configuration(filters, previous_config=None):
    """Create a filter configuration entry, carrying forward data from previous config if available."""
    today = date.today().isoformat()
    
    # Create a map of previous filter data by name
    previous_filters = {}
    if previous_config:
        for f in previous_config.get("filters", []):
            previous_filters[f.get("name")] = {
                "astrobin_id": f.get("astrobin_id", ""),
                "astrobin_name": f.get("astrobin_name", "")
            }
    
    filter_objects = []
    for f in filters:
        if f in previous_filters:
            # Carry forward existing data
            filter_objects.append({
                "name": f,
                "astrobin_id": previous_filters[f]["astrobin_id"],
                "astrobin_name": previous_filters[f]["astrobin_name"]
            })
        else:
            # New filter - empty data
            filter_objects.append({
                "name": f,
                "astrobin_id": "",
                "astrobin_name": ""
            })
    
    return {
        "valid_from": today,
        "valid_to": None,
        "filters": filter_objects
    }


def find_profile_index(config, profile_id):
    """Find the index of a profile in the config, or return None if not found."""
    for i, profile in enumerate(config.get("profiles", [])):
        if profile.get("profile_id") == profile_id:
            return i
    return None


def hash_profile(profile):
    """Create a hash of a profile dict for comparison."""
    # Create a copy and sort keys for consistent hashing
    profile_copy = json.loads(json.dumps(profile, sort_keys=True))
    profile_str = json.dumps(profile_copy, sort_keys=True)
    return hashlib.md5(profile_str.encode('utf-8')).hexdigest()


def reorder_profile_dict(profile):
    """Reorder profile dict to ensure profile_id and profile_name are first."""
    # Extract profile_id and profile_name first
    ordered_profile = {
        "profile_id": profile["profile_id"],
        "profile_name": profile["profile_name"]
    }
    
    # Add all other fields in their current order
    for key, value in profile.items():
        if key not in ["profile_id", "profile_name"]:
            ordered_profile[key] = value
    
    return ordered_profile


def merge_profile_data(config, profile_id, profile_name, filters):
    """Merge profile data into config without deleting existing fields."""
    profile_index = find_profile_index(config, profile_id)
    
    # Create filter order
    filter_order = create_filter_order(filters)
    
    if profile_index is not None:
        # Profile exists - merge data
        existing_profile = config["profiles"][profile_index]
        
        # Hash before changes
        hash_before = hash_profile(existing_profile)
        
        # Update profile_name - profile file is the authority, always overwrite
        existing_profile["profile_name"] = profile_name
        
        # Update filter_order
        existing_profile["filter_order"] = filter_order
        
        # Handle filter configurations
        if "filter_configurations" not in existing_profile:
            existing_profile["filter_configurations"] = []
        
        if len(existing_profile["filter_configurations"]) == 0:
            # No existing config - create new one
            existing_profile["filter_configurations"].append(create_filter_configuration(filters))
        else:
            # Check if filters have changed (set comparison - order doesn't matter)
            active_config = find_active_filter_configuration(existing_profile["filter_configurations"])
            if active_config:
                existing_filter_names = {f.get("name") for f in active_config.get("filters", [])}
                new_filter_names = set(filters)
                
                if existing_filter_names != new_filter_names:
                    # Filters changed - end-date current config and create new one
                    today = date.today().isoformat()
                    active_config["valid_to"] = today
                    existing_profile["filter_configurations"].append(create_filter_configuration(filters, active_config))
            else:
                # No active config found - create new one
                existing_profile["filter_configurations"].append(create_filter_configuration(filters))
        
        # Ensure other fields exist (don't overwrite if they do)
        if "exposure_templates" not in existing_profile:
            existing_profile["exposure_templates"] = []
        
        if "filter_groups" not in existing_profile:
            existing_profile["filter_groups"] = []
        
        # Hash after changes
        hash_after = hash_profile(existing_profile)
        
        if hash_before != hash_after:
            # Reorder to ensure profile_id and profile_name are first
            config["profiles"][profile_index] = reorder_profile_dict(existing_profile)
            print(f"Updated profile: {profile_name} ({profile_id})")
    else:
        # Profile doesn't exist - create new entry
        new_profile = {
            "profile_id": profile_id,
            "profile_name": profile_name,
            "filter_order": filter_order,
            "filter_configurations": [create_filter_configuration(filters)],
            "exposure_templates": [],
            "filter_groups": []
        }
        config.setdefault("profiles", []).append(new_profile)
        print(f"Added new profile: {profile_name} ({profile_id})")


def extract_profile(profile_path):
    """Extract profile data from a NINA profile file."""
    if not os.path.exists(profile_path):
        print(f"Error: Profile file not found: {profile_path}")
        return None
    
    if not profile_path.endswith(".profile"):
        print(f"Error: File must be a .profile file: {profile_path}")
        return None
    
    with open(profile_path, "r", encoding="utf-8") as fd:
        try:
            xml_dump = json.dumps(xmltodict.parse(fd.read()), indent=4)
            xml_data = yaml.safe_load(xml_dump)
            
            profile_id = xml_data["Profile"]["Id"]
            profile_name = xml_data["Profile"]["Name"]
            filters = extract_filters_from_profile(xml_data)
            
            return {
                "profile_id": profile_id,
                "profile_name": profile_name,
                "filters": filters
            }
        except Exception as e:
            print(f"Error processing profile file '{profile_path}': {e}")
            import traceback
            traceback.print_exc()
            return None


def find_profile_files(directory):
    """Recursively find all .profile files in a directory tree."""
    profile_files = []
    for root, _, f_names in os.walk(directory):
        for f in f_names:
            if f.endswith(".profile"):
                profile_files.append(os.path.join(root, f))
    return profile_files


def validate_unique_profile_ids(profile_files):
    """Check that all profile files have unique profile IDs. Returns dict mapping profile_id to file paths."""
    profile_ids = {}
    duplicates = []
    
    for profile_path in profile_files:
        profile_data = extract_profile(profile_path)
        if profile_data:
            profile_id = profile_data["profile_id"]
            if profile_id in profile_ids:
                duplicates.append((profile_id, profile_ids[profile_id], profile_path))
            else:
                profile_ids[profile_id] = profile_path
    
    if duplicates:
        print("Error: Duplicate profile IDs found:")
        for profile_id, path1, path2 in duplicates:
            print(f"  Profile ID {profile_id} found in:")
            print(f"    {path1}")
            print(f"    {path2}")
        sys.exit(1)
    
    return profile_ids


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: extract-profile-to-config.py <profile_directory>")
        print("")
        print("Recursively finds all .profile files in the directory tree and merges into database-config.json")
        sys.exit(1)
    
    profile_dir = sys.argv[1]
    
    if not os.path.isdir(profile_dir):
        print(f"Error: Directory not found: {profile_dir}")
        sys.exit(1)
    
    # Find all profile files
    profile_files = find_profile_files(profile_dir)
    
    if not profile_files:
        print(f"No .profile files found in {profile_dir}")
        sys.exit(0)
    
    print(f"Found {len(profile_files)} profile file(s)")
    
    # Validate unique profile IDs
    validate_unique_profile_ids(profile_files)
    
    # Load existing config
    config = load_config()
    
    # Process each profile file
    for profile_path in profile_files:
        profile_data = extract_profile(profile_path)
        if profile_data:
            merge_profile_data(
                config,
                profile_data["profile_id"],
                profile_data["profile_name"],
                profile_data["filters"]
            )
    
    # Save updated config
    save_config(config)


if __name__ == "__main__":
    main()

