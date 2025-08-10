"""
API Integrations package for bs_Dashboard
"""
from .base.client import BaseAPIClient
from .base.credentials import CredentialsManager
from .base.data_processor import BaseDataProcessor

# Import platform-specific clients
# These will be added as we implement them