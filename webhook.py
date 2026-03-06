"""
Webhook client for firing events to downstream systems.
"""
from typing import Dict, Any
import httpx
import time
import logging

logger = logging.getLogger(__name__)


class WebhookClient:
    """Handles webhook delivery to external systems."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize webhook client with configuration.
        
        Args:
            config: Webhook configuration
        """
        self.config = config
        self.crm_endpoint = config.get("crm_endpoint", "")
        self.email_endpoint = config.get("email_endpoint", "")
        self.timeout = config.get("timeout", 10)
        self.max_retries = config.get("max_retries", 2)
    
    def _fire_webhook(self, endpoint: str, payload: Dict[str, Any]) -> bool:
        """
        Fire a single webhook with retry logic.
        
        Args:
            endpoint: Webhook endpoint URL
            payload: Payload to send
            
        Returns:
            True if successful, False otherwise
        """
        if not endpoint:
            return True
        
        backoff = 1.0
        
        for attempt in range(self.max_retries):
            try:
                response = httpx.post(endpoint, json=payload, timeout=self.timeout)
                
                if response.status_code in (200, 201, 202, 204):
                    return True
                elif response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", backoff))
                    logger.warning(f"Rate limited on {endpoint}, waiting {retry_after}s")
                    time.sleep(retry_after)
                    backoff = min(backoff * 2, 32)
                    continue
                elif response.status_code >= 500:
                    logger.warning(
                        f"Server error {response.status_code} on {endpoint} (attempt {attempt + 1}/{self.max_retries})"
                    )
                    if attempt < self.max_retries - 1:
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 32)
                    continue
                else:
                    logger.error(f"Unexpected status {response.status_code} for {endpoint}")
                    return False
            
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                logger.warning(
                    f"Network error on {endpoint}: {str(e)} (attempt {attempt + 1}/{self.max_retries})"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 32)
                continue
        
        logger.error(f"Failed to fire webhook to {endpoint} after {self.max_retries} retries")
        return False
    
    def fire(self, payload: Dict[str, Any]) -> bool:
        """
        Fire webhook with payload to configured endpoints.
        
        Args:
            payload: Data to send in webhook
            
        Returns:
            True if successful, False otherwise
        """
        crm_success = self._fire_webhook(self.crm_endpoint, payload)
        email_success = self._fire_webhook(self.email_endpoint, payload)
        
        return crm_success and email_success