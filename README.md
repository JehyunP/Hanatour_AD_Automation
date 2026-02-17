# Hanatour_AD_Automation
Real-Time Travel Product Optimization & EP Feed Automation with Apache Airflow

## Background
Hanatour agencies using Naver Price Comparison Ads were receiving compliance warnings due to inconsistencies between:
- Advertisement metadata (title, price, departure date)
- Actual landing page product data
Since Naver enforces strict feed accuracy policies, even minor discrepancies can trigger penalties.

To address this issue, I designed and implemented an automated end-to-end pipeline that:
- Fetches real-time overseas travel product data
- Identifies competitive Top-N products
- Generates optimized advertisement titles
- Transforms structured data into EP (Engine Page) schema
- Uploads final feeds to data into SFTP (Cafe24 Naver)
- Orchestrates the entire workflow using Apache Airflow

The system was designed to ensure:
- Real-time price consistency
- Scalable Top-N product selection across large datasets
- Layered data architecture enabling historical tracking and rollback
- Stable 24/7 execution under constrained EC2 resources

## System Architecture

### Infrastructure
- AWS EC2 (t3-medium) : Primary execution environment
- AWS S3 : Data Lake (Bronze, Silver, Gold)
- Apache Airflow : Workflow orchestration
- SFTP : EP upload destination
- Slack Webhook : Execution monitoring & alerting
- Docker : Environment isolation

To prevent OOM issues:
- Airflow Web UI was excluded from runtime instance
- Memory conscious design across pipeline stages

## End-To-End Workflow
### 1. Area metadata collection (Bronze Layer)
- Fetch overseas travel Area information (category, country, city, codes)
- Store raw Json data into S3 (Bronze Bucket)
- preserve raw data for reproducibility and traceability

### 2. Product List Collection & Filtering (Silver Layer)
- Fetch large-scale product lists (10,000+ items per region)
- Select Top-N competitive products based on:
  - Lowest price
  - Ealiest departure date (when prices are equal)
- Fetch detailed product information using selected product codes
- Store structured dataset partitioned by date in S3 (Silver Bucket)

### 3. Advertisement Logic & EP Transformation (Gold Layer)
- Load latest silver dataset
- Apply multiple title generation strategies (N different logics)
- Generate advertisement-ready titles
- Transform dataset into EP schema format
- Remove duplicates (based on title and landing URL)
- Store finalized dataset in S3 (Gold bucket)

### 4. Final Feed Delivery
- Convert Dataset into TSV format
- Save as www/title.txt
- Upload to Cafe24 Naver server via SFTP

## Scheduling & Monitoring
- Execution hourly (1 hour before scheduled update window)
- Slack notifications for success / failure
- Centralized logging
- All intermediate datasets stored in S3
- Continuous automated execution

## Engineering Challenges & Solutions

### 1. Scaling from single product to Top-N optimization
Selecting a single cheapest product was straightforward.
However, scaling to Top-N selection across tens of thousands of records required algorithmic redesign.

**Initial Idea**
> - Store all products in list/dictionay
> - Sort and Select Top-N
>
> Problem
> - Large memory footprint
> - Repeated sorting overhead
> - Not scalable in EC2-constrained environment

**Final Solution**
> Implemented a **Max Heap (heapq with negative values)**
> - Maintain fixed-size N heap
> - Compare incoming products
> - Replace when more competitive
> - Use `SEEN` set for deduplication
>
> Benefits:
> - O(log N) Maintenance
> - Memory-efficient
> - Deterministic ranking logic
> - Suitable for streaming like process
>
> This significantly improved scalability and reduced memory pressure

### 2, Performance Bottlenect (8-10 Hours -> 20 minutes)
**Problem**
> Detailed product fetch process required:
> ```
> 13,000+ products × N detail fetches
> ```
> Sequential execution caused:
> - 8-10 hour runtime
> - Risk of missing advertisement update windows

**Optimization**
> - Identified workload as IO-bound
> - Applied `ThreadPoolExecutor`
> - Parallelized detail data collection

**Result**
> - Runtime reduces to 18-20 minutes
> - ~95% performance improvement
> - Stable within update constraints
> 
> This was the largest performance gain in the project


## Data Lake Design
**Bronze Bucket** : Raw Area metadata
**Silver Bucket** : Ranked & structured product dataset, prior mapping dataset
**Gold Bucket** : EP-ready advertisement feed

Partitioning Strategy:
- Data-based partitions
- Enables historical comparison
- Simplifies rollback and debugging


## Project Structure
```
dag/
    Airflow DAG definitions

plugins/configs/
    Mapping logic & advertisement title rules (sensitive configuration)

plugins/util/
    Data collection, Preprocessing, Transformation modules

docker/
    Docker build & environment setup

.env
    Environment variables (excluded from repo)
```

## Impact
This project envolved beyond automation and became a scalable advertisement data platform.

### 1. Extensible Multi-Agency Framework
- EP Schema and title-generation logic are modularized
- Different travel agencies can plug in their own title strategies
- Mapping-based configuration enables immediate adoption without core pipeline modification
- Design for horizontal service expansion across region and agencies

### 2. High-Throughput Parallel Processing (t3-medium Optimized)
- Supports parallel logic application for N=30 products per product code
- Capable of generating advertisement-ready titles for up to 30,000+ product records
- Completes execution within 30 minutes of EC2 t3-medium
- Optimized for memory-constrained environments

> This demonstrates production-grade workload optimization under limited infrastructure.

### 3. Operational Observability
- Slack notifiactions for each execution (success/failure)
- Transparent monitoring accessible to non-technical operators
- Immediate detection of pipeline or feed-generation failure

> Reduced operational dependency on engineers.

### 4. Advertisement Workflow Efficiency
- Eliminated manual feed generation
- Enabled real-time advertisement updates
- Supports multi-directional campaign experimentation (different title strategies)
- Increased responsiveness to price changes and departure updates

> Significantly reduced operational overhead in marketing workflow.

### 5. Dynamic Title & Image Override System
- Silver-layer mapping configuration allows:
  - Custom title overrides per product ID
  - Image replacement per product ID
- Enables prioritized product exposure
- Allows rapid campaign pivot without structural code changes

> This design supports strategic marketing interventions without redeployment.


## Future Improvements
The current system provides a stable foundation for further expansion.

### 1. Priority-Based Product Scoring
enhance data collection pharase by incorporating:
- Coupon availability
- Customer ratings
- Review count
- Promotional flags

> Instead of ranking purely by price and departure date,
> introduce a weighted scoring model for smarter ad competitiveness.


### 2. Infrastructure Utilization Optimization
Since EC2 is not fully utilized outside update windows:
- Introduce additional background workloads
- Example:
  - Dynamic CPC adjustment for search ads
  - Bid optimization based on exposure ranking
    
> This would transform the system from feed automation to full ad-operations automation.

### 3. Advanced Title Logic Expansion
- Incorporate more product attributes into title generation
- Expand combinational logic strategies
- Enable A/B testing between title strategies
- Introduce performance-driven title scoring

> This would enable data-driven advertisement optimization.

### 4. Feed Data Integrity Validation
Add validation layer before SFTP upload:
- Verify landing page availability (HTTP status check)
- Validate image URL accessibility
- Detect broken resources
- Prevent invalid feed submissions
  
> This would further reduce compliance risk.


