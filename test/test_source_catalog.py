import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest

from dispatcher.source_catalog import (
    list_available_categories,
    list_available_locations,
    match_sources
)

# We'll manually define a small fake catalog
FAKE_SOURCES = [
    {
        "id": "source1",
        "name": "Source One",
        "url": "http://example.com/one",
        "categories": ["general", "cybersecurity"],
        "locations": ["international", "europe"]
    },
    {
        "id": "source2",
        "name": "Source Two",
        "url": "http://example.com/two",
        "categories": ["emergency management"],
        "locations": ["united states", "california"]
    },
    {
        "id": "source3",
        "name": "Source Three",
        "url": "http://example.com/three",
        "categories": ["health", "general"],
        "locations": ["asia", "international"]
    }
]

def test_list_available_categories():
    cats = list_available_categories(FAKE_SOURCES)
    assert "general" in cats
    assert "cybersecurity" in cats
    assert "health" in cats
    assert "emergency management" in cats
    assert len(cats) == 4

def test_list_available_locations():
    locs = list_available_locations(FAKE_SOURCES)
    assert "international" in locs
    assert "europe" in locs
    assert "united states" in locs
    assert "california" in locs
    assert "asia" in locs
    assert len(locs) == 5

def test_match_sources_basic():
    # Match on category and location
    matched = match_sources(
        task_categories=["general"],
        task_locations=["international"],
        sources=FAKE_SOURCES
    )
    ids = [s["id"] for s in matched]
    assert "source1" in ids
    assert "source3" in ids
    assert len(ids) == 2

def test_match_sources_case_insensitive():
    matched = match_sources(
        task_categories=["EMERGENCY MANAGEMENT"],
        task_locations=["United States"],
        sources=FAKE_SOURCES
    )
    ids = [s["id"] for s in matched]
    assert "source2" in ids
    assert len(ids) == 1

def test_match_sources_no_match():
    matched = match_sources(
        task_categories=["nonexistent category"],
        task_locations=["nowhere"],
        sources=FAKE_SOURCES
    )
    assert matched == []
