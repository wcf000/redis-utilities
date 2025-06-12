"""
Mock implementations of Supabase services for testing
"""

import logging
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class MockSupabaseClient:
    """Mock Supabase client for testing"""
    
    def __init__(self):
        self.users = {
            "test_user_id": {
                "id": "test_user_id",
                "email": "test@example.com",
                "role": "user"
            },
            "admin_user_id": {
                "id": "admin_user_id",
                "email": "admin@example.com",
                "role": "admin"
            }
        }
        self.valid_tokens = {
            "valid_token": "test_user_id",
            "admin_token": "admin_user_id",
            "expired_token": None
        }
    
    async def auth(self):
        """Return auth interface"""
        return self
    
    async def get_user(self, token: str) -> Optional[Dict[str, Any]]:
        """Get user by token"""
        user_id = self.valid_tokens.get(token)
        if not user_id:
            return None
        return self.users.get(user_id)


class MockSupabaseAuthService:
    """Mock implementation of SupabaseAuthService"""
    
    def __init__(self, client=None):
        self.client = client or MockSupabaseClient()
    
    def verify_jwt(self, token: str) -> bool:
        """Verify JWT token"""
        return token in self.client.valid_tokens and self.client.valid_tokens[token] is not None
    
    def get_user_id(self, token: str) -> Optional[str]:
        """Get user ID from token"""
        return self.client.valid_tokens.get(token)
    
    async def get_current_user(self) -> Optional[Dict[str, Any]]:
        """Get current user (placeholder)"""
        return self.client.users["test_user_id"]


async def mock_get_supabase_client():
    """Return mock Supabase client for testing"""
    return MockSupabaseClient()