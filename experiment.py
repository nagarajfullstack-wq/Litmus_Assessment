"""
Experiment assignment system for A/B testing.
"""
from typing import Dict, Any
import hashlib


class ExperimentAssigner:
    """Assigns leads to experiment variants."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize experiment assigner with configuration.

        Args:
            config: Experiment configuration
        """
        self.config = config
        self.variants = list(config.get("experiments", {}).get("email_variants", {}).keys())
        if not self.variants:
            self.variants = ["variant_a", "variant_b"]

    def assign_variant(self, lead_id: str) -> str:
        """
        Assign a lead to an experiment variant.

        Args:
            lead_id: Unique lead identifier

        Returns:
            Experiment variant identifier (e.g. "variant_a" or "variant_b")
        """
        hash_obj = hashlib.md5(lead_id.encode())
        hash_int = int(hash_obj.hexdigest(), 16)
        variant_index = hash_int % len(self.variants)
        return self.variants[variant_index]
