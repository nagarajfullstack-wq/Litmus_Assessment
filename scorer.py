"""
ICP scoring system for evaluating firm fit.
"""
from typing import Dict, Any


class ICPScorer:
    """Scores firms against ideal customer profile criteria."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize scorer with ICP configuration.
        
        Args:
            config: ICP scoring configuration
        """
        self.config = config.get("icp_criteria", {})
        self.firm_size_weight = 0.4
        self.practice_areas_weight = 0.35
        self.geography_weight = 0.25
    
    def _score_firm_size(self, firm: Dict[str, Any]) -> float:
        """
        Score firm based on employee count within ideal range.
        
        Args:
            firm: Firm data with num_lawyers
            
        Returns:
            Score between 0.0 and 1.0
        """
        size_config = self.config.get("firm_size", {})
        min_lawyers = size_config.get("min_lawyers", 50)
        max_lawyers = size_config.get("max_lawyers", 500)
        
        num_lawyers = firm.get("num_lawyers")
        if num_lawyers is None:
            return 0.5
        
        if num_lawyers < min_lawyers:
            return num_lawyers / min_lawyers * 0.5
        elif num_lawyers <= max_lawyers:
            return 1.0
        else:
            return max(0.0, 1.0 - (num_lawyers - max_lawyers) / (max_lawyers * 0.5))
    
    def _score_practice_areas(self, firm: Dict[str, Any]) -> float:
        """
        Score firm based on practice area overlap with preferences.
        
        Args:
            firm: Firm data with practice_areas
            
        Returns:
            Score between 0.0 and 1.0
        """
        pa_config = self.config.get("practice_areas", {})
        preferred = set(pa_config.get("preferred", []))
        
        if not preferred:
            return 0.5
        
        firm_areas = set(firm.get("practice_areas", []))
        
        if not firm_areas:
            return 0.0
        
        overlap = len(firm_areas & preferred)
        return min(1.0, overlap / len(preferred))
    
    def _score_geography(self, firm: Dict[str, Any]) -> float:
        """
        Score firm based on geographic presence in preferred regions.
        
        Args:
            firm: Firm data with country field
            
        Returns:
            Score between 0.0 and 1.0
        """
        geo_config = self.config.get("geography", {})
        preferred_regions = set(geo_config.get("preferred_regions", []))
        
        if not preferred_regions:
            return 0.5
        
        country = firm.get("country")
        if country in preferred_regions:
            return 1.0
        else:
            return 0.0
    
    def score(self, firm: Dict[str, Any]) -> float:
        """
        Calculate ICP score for a firm.
        
        Args:
            firm: Firm data with enriched information
            
        Returns:
            ICP score between 0.0 and 1.0
        """
        size_score = self._score_firm_size(firm)
        area_score = self._score_practice_areas(firm)
        geo_score = self._score_geography(firm)
        
        total_score = (
            size_score * self.firm_size_weight
            + area_score * self.practice_areas_weight
            + geo_score * self.geography_weight
        )
        
        return round(total_score, 3)