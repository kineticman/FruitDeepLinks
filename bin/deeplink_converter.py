#!/usr/bin/env python3
"""
deeplink_converter.py - Convert app scheme deeplinks to HTTP format for Android/Fire TV compatibility

This module provides functions to convert platform-specific deeplink schemes (like aiv://, sportscenter://)
to HTTP URLs that work better on Android and Fire TV devices.

Each provider has its own conversion function that can be updated as we learn more about their schemas.
"""

import re
from typing import Optional


def generate_http_deeplink(punchout_url: str, provider: str = None) -> Optional[str]:
    """
    Convert app scheme deeplinks to HTTP format for Android/Fire TV.
    
    Args:
        punchout_url: Original deeplink from Apple TV (e.g. "aiv://aiv/detail?gti=...")
        provider: Optional provider hint (e.g. "Amazon Prime Video")
    
    Returns:
        HTTP-formatted deeplink or None if conversion not possible
    """
    if not punchout_url:
        return None
    
    # Try each converter in order
    converters = [
        convert_amazon_prime,
        convert_espn,
        convert_peacock,
        convert_paramount,
        convert_max,
        convert_apple_tv,
        convert_dazn,
        convert_fox_sports,
        # Add new converters here as we learn schemas
    ]
    
    for converter in converters:
        result = converter(punchout_url)
        if result and result != punchout_url:
            return result
    
    # No conversion available - return None to indicate we should use original
    return None


def convert_amazon_prime(punchout_url: str) -> Optional[str]:
    """
    Convert Amazon Prime Video deeplinks to HTTP format.
    
    Input:  aiv://aiv/detail?gti=amzn1.dv.gti.XXX&action=watch&type=live&...
    Output: https://app.primevideo.com/detail?gti=amzn1.dv.gti.XXX
    
    Reference: Tested by ADB developer on Fire TV
    """
    if not punchout_url.startswith("aiv://"):
        return None
    
    # Extract GTI (Global Title Identifier)
    match = re.search(r'gti=([^&]+)', punchout_url)
    if match:
        gti = match.group(1)
        return f"https://app.primevideo.com/detail?gti={gti}"
    
    return None


def convert_espn(punchout_url: str) -> Optional[str]:
    """
    Convert ESPN+ (SportsCenter) deeplinks to HTTP format.
    
    Input:  sportscenter://x-callback-url/showWatchStream?playID=XXX&x-source=AppleUMC
    Output: https://www.espn.com/watch/player/_/id/XXX
    
    Note: May need adjustment based on Fire TV testing
    """
    if not punchout_url.startswith("sportscenter://"):
        return None
    
    # Extract playID
    match = re.search(r'playID=([^&]+)', punchout_url)
    if match:
        play_id = match.group(1)
        return f"https://www.espn.com/watch/player/_/id/{play_id}"
    
    return None


def convert_peacock(punchout_url: str) -> Optional[str]:
    """
    Convert Peacock deeplinks to HTTP format.
    
    Input:  peacock://event/XXX
    Output: https://www.peacocktv.com/watch/playback/event/XXX
    
    Note: Schema may vary - needs Fire TV testing
    """
    if not punchout_url.startswith("peacock://"):
        return None
    
    # Extract event ID
    if punchout_url.startswith("peacock://event/"):
        event_id = punchout_url.replace("peacock://event/", "")
        return f"https://www.peacocktv.com/watch/playback/event/{event_id}"
    
    return None


def convert_paramount(punchout_url: str) -> Optional[str]:
    """
    Convert Paramount+ deeplinks to HTTP format.
    
    Input:  TBD - schema unknown
    Output: TBD
    
    TODO: Add conversion once we learn the schema
    """
    if not punchout_url.startswith("paramount"):
        return None
    
    # TODO: Implement when schema is discovered
    return None


def convert_max(punchout_url: str) -> Optional[str]:
    """
    Convert Max (HBO Max) deeplinks to HTTP format.
    
    Input:  TBD - schema unknown
    Output: TBD
    
    TODO: Add conversion once we learn the schema
    """
    if not punchout_url.startswith("max://"):
        return None
    
    # TODO: Implement when schema is discovered
    return None


def convert_apple_tv(punchout_url: str) -> Optional[str]:
    """
    Convert Apple TV deeplinks to HTTP format.
    
    Input:  appletv://...
    Output: https://tv.apple.com/...
    
    Note: Apple TV app may work with both schemes on Fire TV
    """
    if not punchout_url.startswith("appletv://"):
        return None
    
    # Apple TV deeplinks may work as-is on Fire TV
    # Return None to use original scheme, or convert if needed
    return None


def convert_dazn(punchout_url: str) -> Optional[str]:
    """
    Convert DAZN deeplinks to HTTP format.
    
    Input:  TBD - schema unknown
    Output: TBD
    
    TODO: Add conversion once we learn the schema
    """
    if not "dazn" in punchout_url.lower():
        return None
    
    # TODO: Implement when schema is discovered
    return None


def convert_fox_sports(punchout_url: str) -> Optional[str]:
    """
    Convert FOX Sports deeplinks to HTTP format.
    
    Input:  TBD - schema unknown
    Output: TBD
    
    TODO: Add conversion once we learn the schema
    """
    if not "fox" in punchout_url.lower():
        return None
    
    # TODO: Implement when schema is discovered
    return None


# Add more converter functions here as we learn new schemas
# Template:
#
# def convert_SERVICE_NAME(punchout_url: str) -> Optional[str]:
#     """
#     Convert SERVICE deeplinks to HTTP format.
#     
#     Input:  scheme://...
#     Output: https://...
#     """
#     if not punchout_url.startswith("scheme://"):
#         return None
#     
#     # Conversion logic here
#     return converted_url


if __name__ == "__main__":
    # Test conversions
    test_cases = [
        ("aiv://aiv/detail?gti=amzn1.dv.gti.10fd272d-309e-427a-87b6-6289003e2ccb&action=watch&type=live", 
         "https://app.primevideo.com/detail?gti=amzn1.dv.gti.10fd272d-309e-427a-87b6-6289003e2ccb"),
        
        ("sportscenter://x-callback-url/showWatchStream?playID=3be751ec-31ee-466d-9d5a-59645ee401aa&x-source=AppleUMC",
         "https://www.espn.com/watch/player/_/id/3be751ec-31ee-466d-9d5a-59645ee401aa"),
        
        ("peacock://event/12345",
         "https://www.peacocktv.com/watch/playback/event/12345"),
    ]
    
    print("Testing deeplink conversions:")
    for original, expected in test_cases:
        result = generate_http_deeplink(original)
        status = "✓" if result == expected else "✗"
        print(f"\n{status} Original: {original}")
        print(f"  Expected: {expected}")
        print(f"  Got:      {result}")
