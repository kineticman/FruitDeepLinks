#!/usr/bin/env python3
"""
adb_provider_mapper.py - Map logical services to ADB provider codes

For ADB/ADBTuner backward compatibility, we need to map multiple logical services
back to their original provider codes. For example, ESPN Linear and ESPN+ both
map back to 'sportscenter' for ADB purposes.
"""

from typing import Dict, List

# Mapping of logical_service -> ADB provider code
# Multiple logical services can map to the same ADB provider
ADB_PROVIDER_MAP = {
    # ESPN services -> sportscenter
    'espn_linear': 'sportscenter',
    'espn_plus': 'sportscenter',
    'sportscenter': 'sportscenter',  # Keep original mapping
    'sportsonespn': 'sportscenter',  # Legacy ESPN+ code
    
    # Everything else maps to itself
    # (We only need to specify exceptions to the identity mapping)
}


def get_adb_provider_code(logical_service: str) -> str:
    """
    Get the ADB provider code for a logical service.
    
    Args:
        logical_service: The logical service code (e.g., 'espn_linear', 'espn_plus')
    
    Returns:
        The ADB provider code (e.g., 'sportscenter')
    """
    return ADB_PROVIDER_MAP.get(logical_service, logical_service)


def get_logical_services_for_adb_provider(adb_provider: str) -> List[str]:
    """
    Get all logical services that map to an ADB provider.
    
    Args:
        adb_provider: The ADB provider code (e.g., 'sportscenter')
    
    Returns:
        List of logical service codes that map to this provider
    """
    # Build reverse mapping
    services = []
    for logical, adb in ADB_PROVIDER_MAP.items():
        if adb == adb_provider:
            services.append(logical)
    
    # Also include the provider itself if not already in the map
    if adb_provider not in services:
        services.append(adb_provider)
    
    return services


def should_combine_for_adb(adb_provider: str) -> bool:
    """
    Check if this ADB provider combines multiple logical services.
    
    Args:
        adb_provider: The ADB provider code
    
    Returns:
        True if multiple logical services map to this provider
    """
    return len(get_logical_services_for_adb_provider(adb_provider)) > 1


if __name__ == '__main__':
    """Test the mapper"""
    print("ADB Provider Mapping Test")
    print("=" * 60)
    
    test_cases = [
        'espn_linear',
        'espn_plus',
        'sportscenter',
        'peacock',
        'pplus',
    ]
    
    for service in test_cases:
        adb_code = get_adb_provider_code(service)
        print(f"{service:20s} -> {adb_code}")
    
    print()
    print("Reverse mapping (sportscenter):")
    print(get_logical_services_for_adb_provider('sportscenter'))
    print()
    print("Should combine?")
    print(f"  sportscenter: {should_combine_for_adb('sportscenter')}")
    print(f"  peacock: {should_combine_for_adb('peacock')}")
