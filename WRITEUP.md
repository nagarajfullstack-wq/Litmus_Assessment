# GTM Data Pipeline - Write-up

Simple end-to-end pipeline that fetches law firms, enriches with data, scores against ICP criteria, routes leads, and fires webhooks to CRM/email systems.

## Architecture

6 modules handling: data ingestion → enrichment → scoring → routing → webhook delivery

**enricher.py**: Fetches firm data with exponential backoff retry (1s → 2s → 4s... 32s max). Handles rate limits (429), timeouts, schema inconsistencies (num_lawyers vs lawyer_count).

**scorer.py**: Weighted ICP scoring (Firm Size 40%, Practice Areas 35%, Geography 25%). Missing data scored conservatively.

**router.py**: Routes by score threshold: high_priority ≥0.7, nurture ≥0.4, disqualified <0.4.

**experiment.py**: Deterministic MD5 hash-based A/B assignment. Same firm → same variant across runs.

**webhook.py**: Dual-endpoint delivery (CRM + email) with exponential backoff retries. Respects Retry-After header.

**pipeline.py**: Orchestrates everything. Fetches all firms with pagination, deduplicates (domain match + name similarity 0.85 threshold), enriches, scores, routes, fires webhooks.

## Key Design Decisions

**Sequential vs Concurrent**: Sequential processing for simplicity. ~3-10 min for 50 firms acceptable; could use asyncio for 10x speedup.

**Deduplication Strategy**: Two-phase (exact domain + SequenceMatcher 0.85 similarity) catches duplicates like "Baker & Sterling" vs "Baker Sterling LLP". Reduces 55 to ~50 firms.

**Deterministic Assignment**: MD5 hash of firm_id ensures reproducibility across runs. No external state needed.

**Graceful Degradation**: Pipeline continues on failures, returns partial results. Maximizes lead output; ops investigates failures post-run.

## Running It

```bash
./setup.sh
source venv/bin/activate

# Terminal 1
python mock_server.py

# Terminal 2
python pipeline.py config.yaml
```

Expected: 55 firms fetched → ~50 deduplicated → enriched → scored (0.0-1.0) → routed to high_priority/nurture/disqualified → webhooks fired.

## With More Time

- Async HTTP with semaphore for rate limiting (10x faster)
- Database persistence (PostgreSQL) for historical analysis + caching
- Unit tests + Prometheus monitoring
- ML-based scoring (train on conversion data, optimize thresholds)
- Batch webhook delivery (consolidate 50 calls to 2)
- Fuzzy matching for phonetic duplicate detection
