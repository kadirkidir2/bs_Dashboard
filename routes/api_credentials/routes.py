"""
API routes for managing API credentials
"""
import os
import json
from flask import Blueprint, request, jsonify
from api_integrations.base.credentials import CredentialsManager
from api_integrations.shopify import ShopifyClient
from api_integrations.meta import MetaClient
from api_integrations.google import GoogleAnalyticsClient
from api_integrations.tiktok import TikTokAdsClient
from api_integrations.twitter import TwitterClient

# Initialize blueprint
bp = Blueprint('api_credentials', __name__)

# Initialize credentials manager
CREDENTIALS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                               "credentials", "api_credentials.enc")
os.makedirs(os.path.dirname(CREDENTIALS_PATH), exist_ok=True)

# Use environment variable for secret key or default to a secure value
SECRET_KEY = os.environ.get("API_CREDENTIALS_KEY", "change_this_in_production_environment")
credentials_manager = CredentialsManager(CREDENTIALS_PATH, SECRET_KEY)

@bp.route('/save', methods=['POST'])
def save_credentials():
    """Save API credentials"""
    try:
        data = request.json
        
        if not data or 'platform' not in data or 'credentials' not in data:
            return jsonify({"success": False, "message": "Invalid request data"}), 400
        
        platform = data['platform']
        credentials = data['credentials']
        
        # Save credentials
        success = credentials_manager.save_credentials(platform, credentials)
        
        if success:
            return jsonify({"success": True, "message": f"{platform} credentials saved successfully"})
        else:
            return jsonify({"success": False, "message": f"Failed to save {platform} credentials"}), 500
            
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

@bp.route('/test', methods=['POST'])
def test_credentials():
    """Test API credentials"""
    try:
        data = request.json
        
        if not data or 'platform' not in data or 'credentials' not in data:
            return jsonify({"success": False, "message": "Invalid request data"}), 400
        
        platform = data['platform']
        credentials = data['credentials']
        
        # Test credentials based on platform
        if platform == 'shopify':
            client = ShopifyClient(credentials)
            valid = client.validate_credentials()
            
        elif platform == 'meta':
            client = MetaClient(credentials)
            valid = client.validate_credentials()
            
        elif platform == 'google_analytics':
            client = GoogleAnalyticsClient(credentials)
            valid = client.validate_credentials()
            
        elif platform == 'tiktok_ads':
            client = TikTokAdsClient(credentials)
            valid = client.validate_credentials()
            
        elif platform == 'twitter':
            client = TwitterClient(credentials)
            valid = client.validate_credentials()
            
        else:
            return jsonify({"success": False, "message": f"Unsupported platform: {platform}"}), 400
        
        if valid:
            return jsonify({"success": True, "message": f"{platform} credentials are valid"})
        else:
            return jsonify({"success": False, "message": f"{platform} credentials are invalid"}), 400
            
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

@bp.route('/list', methods=['GET'])
def list_platforms():
    """List platforms with saved credentials"""
    try:
        platforms = credentials_manager.list_platforms()
        return jsonify({"success": True, "platforms": platforms})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

@bp.route('/delete', methods=['POST'])
def delete_credentials():
    """Delete API credentials"""
    try:
        data = request.json
        
        if not data or 'platform' not in data:
            return jsonify({"success": False, "message": "Invalid request data"}), 400
        
        platform = data['platform']
        
        # Delete credentials
        success = credentials_manager.delete_credentials(platform)
        
        if success:
            return jsonify({"success": True, "message": f"{platform} credentials deleted successfully"})
        else:
            return jsonify({"success": False, "message": f"No {platform} credentials found"}), 404
            
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500