import json
import csv
import io
from typing import Any, Optional, List


def json_to_csv(json_input: str | dict) -> str:
    """
    Convert JSON to flattened CSV format.

    Args:
        json_input: JSON string or dict. If the JSON has a 'results' key containing
                   a list, it will be extracted. Otherwise, the entire structure
                   will be wrapped in a list for processing.

    Returns:
        CSV string with headers and flattened rows
    """
    # Parse JSON if it's a string
    if isinstance(json_input, str):
        data = json.loads(json_input)
    else:
        data = json_input

    if isinstance(data, dict) and "results" in data:
        records = data["results"]
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    flattened_records = [_flatten_dict(record) for record in records]

    if not flattened_records:
        return ""

    # Get all unique keys across all records (for consistent column ordering)
    all_keys = []
    seen = set()
    for record in flattened_records:
        for key in record.keys():
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_keys, lineterminator="\n")
    writer.writeheader()
    writer.writerows(flattened_records)

    return output.getvalue()


def _flatten_dict(
    d: dict[str, Any], parent_key: str = "", sep: str = "_"
) -> dict[str, Any]:
    """
    Flatten a nested dictionary by joining keys with separator.

    Args:
        d: Dictionary to flatten
        parent_key: Key from parent level (for recursion)
        sep: Separator to use between nested keys

    Returns:
        Flattened dictionary with no nested structures
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            # Recursively flatten nested dicts
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to comma-separated strings
            items.append((new_key, str(v)))
        else:
            items.append((new_key, v))

    return dict(items)


def json_to_csv_filtered(
    json_input: str | dict,
    fields: Optional[List[str]] = None,
    exclude_fields: Optional[List[str]] = None,
) -> str:
    """
    Convert JSON to CSV with optional field filtering.

    Args:
        json_input: JSON string or dict
        fields: Include only these fields (None = all)
        exclude_fields: Exclude these fields

    Returns:
        CSV string with selected fields only
    """
    # Parse JSON
    if isinstance(json_input, str):
        data = json.loads(json_input)
    else:
        data = json_input

    # Extract records
    if isinstance(data, dict) and "results" in data:
        records = data["results"]
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    # Flatten records
    flattened = [_flatten_dict(record) for record in records]

    # Apply field filtering with flexible matching
    if fields:
        filtered = []
        for record in flattened:
            filtered_record = {}
            for field in fields:
                # Try exact match first
                if field in record:
                    filtered_record[field] = record[field]
                else:
                    # Try suffix match (e.g., "expiration_date" matches "details_expiration_date")
                    for key, value in record.items():
                        if key.endswith(f"_{field}") or key.endswith(f".{field}"):
                            # Use the original field name for output
                            filtered_record[field] = value
                            break
            if filtered_record:  # Only include records with at least one matching field
                filtered.append(filtered_record)
        flattened = filtered
    elif exclude_fields:
        flattened = [
            {k: v for k, v in record.items() if k not in exclude_fields}
            for record in flattened
        ]

    # Convert to CSV
    if not flattened:
        return ""

    # Get all unique keys across all records (for consistent column ordering)
    all_keys = []
    seen = set()
    for record in flattened:
        for key in record.keys():
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_keys, lineterminator="\n")
    writer.writeheader()
    writer.writerows(flattened)

    return output.getvalue()


def json_to_compact(json_input: str | dict, fields: Optional[List[str]] = None) -> str:
    """
    Convert JSON to minimal compact format.
    Best for single-record responses.

    Args:
        json_input: JSON string or dict
        fields: Include only these fields

    Returns:
        Compact JSON string (e.g., '{"close": 185.92, "volume": 52165200}')
    """
    if isinstance(json_input, str):
        data = json.loads(json_input)
    else:
        data = json_input

    # Extract single record
    if isinstance(data, dict) and "results" in data:
        record = data["results"][0] if data["results"] else {}
    elif isinstance(data, list):
        record = data[0] if data else {}
    else:
        record = data

    # Flatten
    flattened = _flatten_dict(record)

    # Apply field filtering with flexible matching
    if fields:
        filtered = {}
        for field in fields:
            # Try exact match first
            if field in flattened:
                filtered[field] = flattened[field]
            else:
                # Try suffix match
                for key, value in flattened.items():
                    if key.endswith(f"_{field}") or key.endswith(f".{field}"):
                        filtered[field] = value
                        break
        flattened = filtered

    return json.dumps(flattened, separators=(",", ":"))


def json_to_json_filtered(
    json_input: str | dict,
    fields: Optional[List[str]] = None,
    preserve_structure: bool = False,
) -> str:
    """
    Convert to JSON with optional field filtering.

    Args:
        json_input: JSON string or dict
        fields: Include only these fields
        preserve_structure: Keep nested structure (don't flatten)

    Returns:
        JSON string
    """
    if isinstance(json_input, str):
        data = json.loads(json_input)
    else:
        data = json_input

    if isinstance(data, dict) and "results" in data:
        records = data["results"]
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    if not preserve_structure:
        records = [_flatten_dict(record) for record in records]

    # Apply field filtering with flexible matching
    if fields:
        filtered_records = []
        for record in records:
            filtered_record = {}
            for field in fields:
                # Try exact match first
                if field in record:
                    filtered_record[field] = record[field]
                else:
                    # Try suffix match
                    for key, value in record.items():
                        if key.endswith(f"_{field}") or key.endswith(f".{field}"):
                            filtered_record[field] = value
                            break
            if filtered_record:
                filtered_records.append(filtered_record)
        records = filtered_records

    return json.dumps(records, indent=2)
