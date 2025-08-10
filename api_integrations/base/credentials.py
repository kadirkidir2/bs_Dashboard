"""
Credentials management system for API integrations
"""
import json
import os
import base64
from typing import Dict, Any, Optional, List
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("api.credentials")

class CredentialsManager:
    """Manages API credentials with encryption"""
    
    def __init__(self, storage_path: str, secret_key: Optional[str] = None):
        """
        Initialize the credentials manager
        
        Args:
            storage_path: Path to store encrypted credentials
            secret_key: Secret key for encryption (if None, will use environment variable)
        """
        self.storage_path = storage_path
        
        # Create storage directory if it doesn't exist
        os.makedirs(os.path.dirname(storage_path), exist_ok=True)
        
        # Get or generate encryption key
        if secret_key:
            self.secret_key = secret_key
        else:
            self.secret_key = os.environ.get('API_CREDENTIALS_KEY', 'default_secret_key_change_in_production')
            
        # Initialize encryption
        self._init_encryption()
        
    def _init_encryption(self):
        """Initialize encryption with the secret key"""
        # Generate a key from the secret
        salt = b'bs_dashboard_salt'  # In production, this should be stored securely
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(self.secret_key.encode()))
        self.cipher = Fernet(key)
        
    def save_credentials(self, platform: str, credentials: Dict[str, Any]) -> bool:
        """
        Save credentials for a platform
        
        Args:
            platform: Platform name (e.g., 'shopify', 'meta')
            credentials: Dictionary of credentials
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load existing credentials
            all_credentials = self.load_all_credentials()
            
            # Update with new credentials
            all_credentials[platform] = credentials
            
            # Encrypt and save
            encrypted_data = self.cipher.encrypt(json.dumps(all_credentials).encode())
            with open(self.storage_path, 'wb') as f:
                f.write(encrypted_data)
                
            logger.info(f"Credentials saved for platform: {platform}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save credentials for {platform}: {str(e)}")
            return False
            
    def load_credentials(self, platform: str) -> Optional[Dict[str, Any]]:
        """
        Load credentials for a specific platform
        
        Args:
            platform: Platform name
            
        Returns:
            Dictionary of credentials or None if not found
        """
        all_credentials = self.load_all_credentials()
        return all_credentials.get(platform)
        
    def load_all_credentials(self) -> Dict[str, Dict[str, Any]]:
        """
        Load all stored credentials
        
        Returns:
            Dictionary of all credentials by platform
        """
        if not os.path.exists(self.storage_path):
            return {}
            
        try:
            with open(self.storage_path, 'rb') as f:
                encrypted_data = f.read()
                
            if not encrypted_data:
                return {}
                
            decrypted_data = self.cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode())
            
        except Exception as e:
            logger.error(f"Failed to load credentials: {str(e)}")
            return {}
            
    def delete_credentials(self, platform: str) -> bool:
        """
        Delete credentials for a platform
        
        Args:
            platform: Platform name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            all_credentials = self.load_all_credentials()
            
            if platform in all_credentials:
                del all_credentials[platform]
                
                # Encrypt and save
                encrypted_data = self.cipher.encrypt(json.dumps(all_credentials).encode())
                with open(self.storage_path, 'wb') as f:
                    f.write(encrypted_data)
                    
                logger.info(f"Credentials deleted for platform: {platform}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to delete credentials for {platform}: {str(e)}")
            return False
            
    def list_platforms(self) -> List[str]:
        """
        List all platforms with saved credentials
        
        Returns:
            List of platform names
        """
        return list(self.load_all_credentials().keys())