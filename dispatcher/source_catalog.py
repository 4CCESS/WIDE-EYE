import json
from typing import List, Dict, Set


def load_sources(filepath: str = "dispatcher/sources.json") -> List[Dict]:
    """
    Load the source catalog from a JSON file.

    Args:
        filepath (str): Path to the sources.json file.

    Returns:
        List[Dict]: A list of source entries, each being a dictionary with keys:
            - id (str): Unique identifier for the source.
            - name (str): Human-readable name.
            - url (str): URL or feed endpoint.
            - categories (List[str]): List of category strings.
            - locations (List[str]): List of location strings.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        sources = json.load(f)
    return sources


def list_available_categories(sources: List[Dict]) -> List[str]:
    """
    Extract and return a sorted list of all unique categories
    present in the source catalog.

    Args:
        sources (List[Dict]): List of source entries.

    Returns:
        List[str]: Sorted list of unique category names.
    """
    categories: Set[str] = set()
    for source in sources:
        # Each category entry may itself be a comma-separated string.
        for entry in source.get('categories', []):
            # Split on commas and strip whitespace
            for cat in entry.split(','):
                cleaned = cat.strip()
                if cleaned:
                    categories.add(cleaned)
    return sorted(categories)


def list_available_locations(sources: List[Dict]) -> List[str]:
    """
    Extract and return a sorted list of all unique locations
    present in the source catalog.

    Args:
        sources (List[Dict]): List of source entries.

    Returns:
        List[str]: Sorted list of unique location names.
    """
    locations: Set[str] = set()
    for source in sources:
        # Each location entry may itself be a comma-separated string.
        for entry in source.get('locations', []):
            # Split on commas and strip whitespace
            for loc in entry.split(','):
                cleaned = loc.strip()
                if cleaned:
                    locations.add(cleaned)
    return sorted(locations)


def match_sources(task_categories: List[str], task_locations: List[str], sources: List[Dict]) -> List[Dict]:
    """
    Match user-specified categories and locations against
    the source catalog and return the list of sources that
    have at least one matching category AND at least one matching location.

    Args:
        task_categories (List[str]): List of categories specified in the task.
        task_locations (List[str]): List of locations specified in the task.
        sources (List[Dict]): The source catalog.

    Returns:
        List[Dict]: Subset of sources matching the criteria.
    """
    # Normalize user input: strip whitespace and lowercase
    cat_set = {c.strip().lower() for c in task_categories if c.strip()}
    loc_set = {l.strip().lower() for l in task_locations if l.strip()}

    matched: List[Dict] = []
    for source in sources:
        # Normalize source tags
        src_cats = set()
        for entry in source.get('categories', []):
            for cat in entry.split(','):
                cleaned = cat.strip().lower()
                if cleaned:
                    src_cats.add(cleaned)

        src_locs = set()
        for entry in source.get('locations', []):
            for loc in entry.split(','):
                cleaned = loc.strip().lower()
                if cleaned:
                    src_locs.add(cleaned)

        # Check for intersection on both dimensions
        if src_cats & cat_set and src_locs & loc_set:
            matched.append(source)
    return matched
