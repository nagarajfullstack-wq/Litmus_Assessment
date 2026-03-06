"""
Data enrichment service for firmographic and contact data.
"""
from typing import Dict, Any, Optional
import httpx
import time
import logging


logger = logging.getLogger(__name__)


class Enricher:
    """Handles data enrichment for firms."""

    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        """
        Initialize enricher with API configuration.

        Args:
            base_url: Base URL for enrichment API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries

    def _make_request(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request with exponential backoff retry logic.

        Args:
            endpoint: API endpoint URL

        Returns:
            Response JSON or None if all retries exhausted
        """
        backoff = 1.0
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = httpx.get(endpoint, timeout=self.timeout)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", backoff))
                    logger.warning(
                        f"Rate limited on {endpoint}, waiting {retry_after}s (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(retry_after)
                    backoff = min(backoff * 2, 32)
                    continue
                elif response.status_code == 500:
                    logger.warning(
                        f"Server error on {endpoint} (attempt {attempt + 1}/{self.max_retries})"
                    )
                    if attempt < self.max_retries - 1:
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 32)
                    continue
                else:
                    logger.error(f"Unexpected status {response.status_code} for {endpoint}")
                    return None

            except (httpx.TimeoutException, httpx.ConnectError) as e:
                last_error = e
                logger.warning(
                    f"Network error on {endpoint}: {str(e)} (attempt {attempt + 1}/{self.max_retries})"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 32)
                continue

        logger.error(f"Failed to fetch {endpoint} after {self.max_retries} retries")
        return None

    def fetch_firmographic(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch firmographic data for a firm.

        Args:
            firm_id: Unique identifier for the firm

        Returns:
            Firmographic data or None if unavailable
        """
        endpoint = f"{self.base_url}/firms/{firm_id}/firmographic"
        data = self._make_request(endpoint)

        if data is None:
            return None

        normalized = {
            "num_lawyers": data.get("num_lawyers") or data.get("lawyer_count"),
            "practice_areas": data.get("practice_areas", []),
            "annual_revenue": data.get("annual_revenue"),
            "founded_year": data.get("founded_year"),
        }

        return {k: v for k, v in normalized.items() if v is not None}

    def fetch_contact(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch contact information for a firm.

        Args:
            firm_id: Unique identifier for the firm

        Returns:
            Contact data or None if unavailable
        """
        endpoint = f"{self.base_url}/firms/{firm_id}/contact"
        data = self._make_request(endpoint)

        if data is None:
            return None

        contact = {
            "name": data.get("contact_name"),
            "email": data.get("email"),
            "title": data.get("title"),
            "linkedin_url": data.get("linkedin_url"),
        }

        return {k: v for k, v in contact.items() if v is not None}