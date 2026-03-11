import json
import csv
import io
from typing import Any, Optional, List


def strip_response_metadata(json_text: str, exclude_keys: set) -> str:
    """Strip metadata keys from a JSON response string.

    Parses the JSON, removes top-level keys in exclude_keys, and re-serializes.
    """
    data = json.loads(json_text)
    if isinstance(data, dict):
        for key in exclude_keys:
            data.pop(key, None)
    return json.dumps(data)


def extract_records(data: str | dict | list) -> list[dict]:
    """Extract and flatten records from raw JSON input.

    Takes raw JSON input (string or parsed), extracts the records list
    (handling 'results', 'last', list, and single-object cases), flattens
    each record via _flatten_dict, and returns a list of flat dicts.

    Args:
        data: JSON string, dict, or list.

    Returns:
        List of flattened dictionaries.
    """
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return []

    if isinstance(data, dict) and "results" in data:
        results_value = data["results"]
        if isinstance(results_value, list):
            records = results_value
        elif isinstance(results_value, dict):
            records = [results_value]
        else:
            records = [results_value]
    elif isinstance(data, dict) and "last" in data:
        records = [data["last"]] if isinstance(data["last"], dict) else [data]
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    flattened_records = []
    for record in records:
        if isinstance(record, dict):
            flattened_records.append(_flatten_dict(record))
        else:
            flattened_records.append({"value": str(record)})

    return flattened_records


def json_to_csv(json_input: str | dict | list) -> str:
    """
    Convert JSON to flattened CSV format.

    Args:
        json_input: JSON string or dict. If the JSON has a 'results' key containing
                   a list, it will be extracted. Otherwise, the entire structure
                   will be wrapped in a list for processing.

    Returns:
        CSV string with headers and flattened rows
    """
    flattened_records = extract_records(json_input)

    if not flattened_records:
        return ""

    # Get all unique keys across all records (for consistent column ordering)
    all_keys = []
    seen = set()
    for record in flattened_records:
        if isinstance(record, dict):
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
        try:
            data = json.loads(json_input)
        except json.JSONDecodeError:
            return ""
    else:
        data = json_input

    # Extract records
    if isinstance(data, dict) and "results" in data:
        results_value = data["results"]
        if isinstance(results_value, list):
            records = results_value
        elif isinstance(results_value, dict):
            records = [results_value]
        else:
            records = [results_value]
    elif isinstance(data, dict) and "last" in data:
        records = [data["last"]] if isinstance(data["last"], dict) else [data]
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    # Flatten records
    flattened = []
    for record in records:
        if isinstance(record, dict):
            flattened.append(_flatten_dict(record))
        else:
            flattened.append({"value": str(record)})

    # Apply field filtering
    if fields:
        flattened = [
            {k: v for k, v in record.items() if k in fields} for record in flattened
        ]
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
        try:
            data = json.loads(json_input)
        except json.JSONDecodeError:
            return "{}"
    else:
        data = json_input

    # Extract single record
    if isinstance(data, dict) and "results" in data:
        results = data["results"]
        if isinstance(results, list):
            record = results[0] if results else {}
        else:
            record = results
    elif isinstance(data, dict) and "last" in data:
        record = data["last"] if isinstance(data["last"], dict) else {}
    elif isinstance(data, list):
        record = data[0] if data else {}
    else:
        record = data

    # Flatten
    if isinstance(record, dict):
        flattened = _flatten_dict(record)
    else:
        flattened = {"value": str(record)}

    # Apply field filtering
    if fields:
        flattened = {k: v for k, v in flattened.items() if k in fields}

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
        try:
            data = json.loads(json_input)
        except json.JSONDecodeError:
            return "[]"
    else:
        data = json_input

    if isinstance(data, dict) and "results" in data:
        results_value = data["results"]
        if isinstance(results_value, list):
            records = results_value
        elif isinstance(results_value, dict):
            records = [results_value]
        else:
            records = [results_value]
    elif isinstance(data, dict) and "last" in data:
        records = [data["last"]] if isinstance(data["last"], dict) else [data]
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    if not preserve_structure:
        flattened = []
        for record in records:
            if isinstance(record, dict):
                flattened.append(_flatten_dict(record))
            else:
                flattened.append({"value": str(record)})
        records = flattened

    if fields:
        records = [
            {k: v for k, v in record.items() if k in fields} for record in records
        ]

    return json.dumps(records, indent=2)
