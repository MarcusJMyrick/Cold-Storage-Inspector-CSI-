# Cold Storage Inspector (CSI)

**AI-powered data warehouse cost optimization through intelligent cold data archival**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

## Problem

Organizations maintain petabyte-scale data warehouses (Snowflake, BigQuery, Databricks) where **60-90% of data is cold** (not queried in 30+ days) but remains in premium hot storage due to **archival paralysis**—fear of breaking undiscovered downstream dependencies.

Cold data costs **10x more** than necessary, but the risk of breaking production queries prevents archival.

## Solution

CSI solves archival paralysis by:

1. **Analyzing 90 days of query logs** to build partition-level access heatmaps
2. **Detecting brittle query patterns** using static SQL analysis
3. **Simulating cost savings** with quantified risk assessments
4. **Generating actionable policies** with confidence scores

## Quick Start

### Installation

```bash
pip install cold-storage-inspector[snowflake]
# or
pip install cold-storage-inspector[bigquery]
# or
pip install cold-storage-inspector[all]
```

### Basic Usage

```bash
# Generate a savings report
csi inspect snowflake "account:user@warehouse/database?role=ANALYST"

# Validate an archival policy
csi validate policy.json

# Monitor queries in real-time
csi monitor --warehouse snowflake --watch
```

## Architecture

CSI is built in 8 layers:

1. **Data Acquisition Plane** - Extract query logs from warehouses
2. **SQL Static Analysis Engine** - Parse AST, detect brittle patterns
3. **Access Heatmap Engine** - Build partition access frequency maps
4. **Cost Simulation Engine** - Recompute bills with archival scenarios
5. **Policy Engine** - Generate archival recommendations
6. **CLI Application** - Zero-install-cost entry point
7. **SaaS Web Platform** - Continuous monitoring (coming soon)
8. **Integrations Layer** - dbt, Airflow, Terraform (coming soon)

## Core Concepts

### QueryRecord

Canonical representation of a single query execution:

```python
from csi.models import QueryRecord

record = QueryRecord(
    warehouse_type="SNOWFLAKE",
    query_text="SELECT * FROM sales WHERE date = '2024-01-01'",
    start_time=datetime(2024, 1, 1, 10, 0, 0),
    bytes_scanned=1024 * 1024 * 100,  # 100 MB
    status="SUCCESS"
)
```

### PartitionHeatMap

Time-series heatmap of partition access:

```python
from csi.heatmap import build_heatmap

heatmap = build_heatmap(
    queries=query_records,
    table_metadata=table_meta,
    lookback_days=90
)

print(f"Coldness score: {heatmap['2024-01-01'].coldness_score}")
```

### ArchivalPolicy

Actionable recommendation with confidence scores:

```python
from csi.policy import generate_recommendations

policies = generate_recommendations(
    heatmap=heatmap,
    brittle_scores=brittle_scores,
    cost_simulation=simulation
)

for policy in policies:
    print(f"{policy.table_id}: ${policy.estimated_monthly_savings_usd:.2f}/mo")
    print(f"  Confidence: {policy.confidence_score:.2%}")
    print(f"  Risk: {policy.risk_score:.2%}")
```

## Examples

### Identify Cold Partitions

```python
import asyncio
from csi.connectors import SnowflakeConnector
from csi.heatmap import build_heatmap
from csi.policy import generate_recommendations

async def main():
    connector = SnowflakeConnector(connection_string="...")
    
    # Extract 90 days of queries
    queries = []
    async for query in connector.extract_query_logs(lookback_days=90):
        queries.append(query)
    
    # Build heatmap
    heatmap = build_heatmap(queries, table_metadata)
    
    # Generate recommendations
    recommendations = generate_recommendations(heatmap)
    
    # Print top 10 savings opportunities
    for rec in recommendations[:10]:
        print(f"{rec.table_id}: ${rec.estimated_monthly_savings_usd:,.2f}/mo")

asyncio.run(main())
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Status

🚧 **Alpha** - Active development. APIs may change.

## Support

- 📖 [Documentation](https://docs.coldstorageinspector.com) (coming soon)
- 💬 [Discussions](https://github.com/yourorg/cold-storage-inspector/discussions)
- 🐛 [Issues](https://github.com/yourorg/cold-storage-inspector/issues)


