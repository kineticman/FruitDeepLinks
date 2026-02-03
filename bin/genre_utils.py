#!/usr/bin/env python3
"""
genre_utils.py - Shared genre normalization utilities

Use this to clean genres before inserting into database.
Ensures consistent capitalization and filters out non-sports categories.
"""

from typing import List

# Genre normalization mapping
GENRE_FIXES = {
    'Mma': 'MMA',
    'mma': 'MMA',
}

# Non-sports genres to filter out
NON_SPORTS_GENRES = {
    'Bus./Financial',
    'Consumer',
    'Sports',  # Too generic
}

def normalize_genres(genres: List[str]) -> List[str]:
    """
    Normalize and filter a list of genres.
    
    - Fixes capitalization (Mma -> MMA)
    - Removes non-sports categories (Consumer, Bus./Financial)
    - Removes generic "Sports" genre
    - Removes duplicates while preserving order
    
    Returns: Cleaned list of genres
    """
    if not genres:
        return []
    
    normalized = []
    seen = set()
    
    for genre in genres:
        # Skip empty/null genres
        if not genre:
            continue
        
        # Filter out non-sports genres
        if genre in NON_SPORTS_GENRES:
            continue
        
        # Apply capitalization fixes
        fixed = GENRE_FIXES.get(genre, genre)
        
        # Remove duplicates (case-sensitive after fixes)
        if fixed not in seen:
            seen.add(fixed)
            normalized.append(fixed)
    
    return normalized
