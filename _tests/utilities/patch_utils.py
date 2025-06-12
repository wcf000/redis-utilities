"""
Utility functions for patching dependencies in tests
"""

import sys
from unittest.mock import patch
from .mock_supabase import MockSupabaseAuthService, mock_get_supabase_client

def patch_supabase_for_tests():
    """
    Apply patches for Supabase dependencies in tests
    Returns a tuple of (patch_app, patch_client) to be used in test context managers
    """
    # Create patches for Supabase imports
    patch_app = patch('app.core.third_party_integrations.supabase_home.app.SupabaseAuthService', 
                     return_value=MockSupabaseAuthService())
    
    patch_client = patch('app.core.third_party_integrations.supabase_home.client.get_supabase_client', 
                        side_effect=mock_get_supabase_client)
    
    return patch_app, patch_client