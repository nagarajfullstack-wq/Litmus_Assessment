"""
Main pipeline orchestrator for GTM data processing.
"""
from typing import Any, Dict, List, Optional, Set
import yaml
import httpx
import logging
from difflib import SequenceMatcher
from enricher import Enricher
from scorer import ICPScorer
from router import LeadRouter
from experiment import ExperimentAssigner
from webhook import WebhookClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the complete GTM data pipeline."""
    
    def __init__(self, config_path: str):
        """
        Initialize pipeline with configuration.
        
        Args:
            config_path: Path to YAML configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        api_config = self.config.get("apis", {})
        enrichment_config = api_config.get("enrichment", {})
        webhooks_config = api_config.get("webhooks", {})
        
        self.enricher = Enricher(
            base_url=enrichment_config.get("base_url", "http://localhost:8000"),
            timeout=enrichment_config.get("timeout", 30),
            max_retries=enrichment_config.get("max_retries", 3)
        )
        
        self.scorer = ICPScorer(self.config)
        self.router = LeadRouter(self.config)
        self.experiment_assigner = ExperimentAssigner(self.config)
        self.webhook_client = WebhookClient(webhooks_config)
        
        self.base_url = enrichment_config.get("base_url", "http://localhost:8000")
        self.timeout = enrichment_config.get("timeout", 30)
    
    def _fetch_all_firms(self) -> List[Dict[str, Any]]:
        """
        Fetch all firms from the API with pagination.
        
        Returns:
            List of firm records
        """
        firms = []
        page = 1
        
        while True:
            try:
                response = httpx.get(
                    f"{self.base_url}/firms?page={page}",
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    batch = data.get("firms", [])
                    
                    if not batch:
                        break
                    
                    firms.extend(batch)
                    page += 1
                else:
                    logger.warning(f"Failed to fetch page {page}: status {response.status_code}")
                    break
            
            except Exception as e:
                logger.error(f"Error fetching firms page {page}: {str(e)}")
                break
        
        logger.info(f"Fetched {len(firms)} firms from API")
        return firms
    
    def _is_duplicate(self, firm1: Dict[str, Any], firm2: Dict[str, Any]) -> bool:
        """
        Check if two firms are duplicates based on domain and name similarity.
        
        Args:
            firm1: First firm record
            firm2: Second firm record
            
        Returns:
            True if firms are duplicates
        """
        domain1 = firm1.get("domain", "").lower()
        domain2 = firm2.get("domain", "").lower()
        
        if domain1 and domain1 == domain2:
            return True
        
        name1 = firm1.get("name", "").lower()
        name2 = firm2.get("name", "").lower()
        
        if name1 and name2:
            similarity = SequenceMatcher(None, name1, name2).ratio()
            if similarity > 0.85:
                return True
        
        return False
    
    def _deduplicate_firms(self, firms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate firms from the list.
        
        Args:
            firms: List of firm records
            
        Returns:
            Deduplicated list of firms
        """
        seen_domains: Set[str] = set()
        deduplicated = []
        
        for firm in firms:
            domain = firm.get("domain", "").lower()
            
            is_dup = False
            if domain and domain in seen_domains:
                is_dup = True
            else:
                for existing in deduplicated:
                    if self._is_duplicate(firm, existing):
                        is_dup = True
                        break
            
            if not is_dup:
                deduplicated.append(firm)
                if domain:
                    seen_domains.add(domain)
        
        logger.info(f"Deduplicated {len(firms)} firms to {len(deduplicated)} unique firms")
        return deduplicated
    
    def _enrich_firm(self, firm: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a firm with additional data.
        
        Args:
            firm: Base firm record
            
        Returns:
            Enriched firm record
        """
        firm_id = firm.get("id")
        
        firmographic = self.enricher.fetch_firmographic(firm_id)
        if firmographic:
            firm.update(firmographic)
        
        contact = self.enricher.fetch_contact(firm_id)
        if contact:
            firm["contact"] = contact
        
        return firm
    
    def _process_leads(self, firms: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process firms through scoring, routing, and assignment.
        
        Args:
            firms: List of enriched firm records
            
        Returns:
            Processing results
        """
        results = {
            "high_priority": [],
            "nurture": [],
            "disqualified": [],
            "total_processed": 0,
            "webhooks_fired": 0,
            "webhooks_failed": 0
        }
        
        for firm in firms:
            results["total_processed"] += 1
            
            score = self.scorer.score(firm)
            firm["icp_score"] = score
            
            route = self.router.route(firm, score)
            firm["route"] = route
            
            variant = self.experiment_assigner.assign_variant(firm.get("id"))
            firm["experiment_variant"] = variant
            
            if route != "disqualified":
                payload = {
                    "firm_id": firm.get("id"),
                    "firm_name": firm.get("name"),
                    "icp_score": score,
                    "route": route,
                    "experiment_variant": variant,
                    "contact": firm.get("contact"),
                    "practice_areas": firm.get("practice_areas", []),
                    "num_lawyers": firm.get("num_lawyers")
                }
                
                if self.webhook_client.fire(payload):
                    results["webhooks_fired"] += 1
                else:
                    results["webhooks_failed"] += 1
            
            results[route].append(firm)
        
        return results
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete GTM data pipeline.
        
        Returns:
            Pipeline results
        """
        logger.info("Starting GTM pipeline...")
        
        firms = self._fetch_all_firms()
        logger.info(f"Fetched {len(firms)} firms")
        
        firms = self._deduplicate_firms(firms)
        logger.info(f"Processing {len(firms)} unique firms")
        
        enriched_firms = []
        for i, firm in enumerate(firms):
            logger.info(f"Enriching firm {i+1}/{len(firms)}: {firm.get('name')}")
            enriched = self._enrich_firm(firm)
            enriched_firms.append(enriched)
        
        logger.info(f"Enriched {len(enriched_firms)} firms")
        
        results = self._process_leads(enriched_firms)
        
        logger.info(f"Pipeline complete. Results:")
        logger.info(f"  High Priority: {len(results['high_priority'])}")
        logger.info(f"  Nurture: {len(results['nurture'])}")
        logger.info(f"  Disqualified: {len(results['disqualified'])}")
        logger.info(f"  Total Processed: {results['total_processed']}")
        logger.info(f"  Webhooks Fired: {results['webhooks_fired']}")
        logger.info(f"  Webhooks Failed: {results['webhooks_failed']}")
        
        return results


def run_pipeline(config_path: str) -> Any:
    """
    Run the complete GTM data pipeline.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Pipeline results (structure is yours to define)
    """
    orchestrator = PipelineOrchestrator(config_path)
    return orchestrator.run()


if __name__ == "__main__":
    import sys
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    results = run_pipeline(config_path)
    print(f"\nPipeline Results Summary:")
    print(f"  High Priority Leads: {len(results['high_priority'])}")
    print(f"  Nurture Leads: {len(results['nurture'])}")
    print(f"  Disqualified: {len(results['disqualified'])}")
    print(f"  Total Processed: {results['total_processed']}")
    print(f"  Webhooks Successfully Fired: {results['webhooks_fired']}")
    print(f"  Webhooks Failed: {results['webhooks_failed']}")
