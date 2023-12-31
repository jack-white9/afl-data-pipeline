# AFL Game Data

## Overview

This repo contains a data pipeline that runs on AWS to ingest and transform AFL game data on a weekly basis.

## Architecture

![architecture](https://raw.githubusercontent.com/jack-white9/afl-tipping-pipeline/main/docs/architecture.png)

### Ingestion

The raw data is pulled from the [Squiggle API](https://api.squiggle.com.au/) and ingested into S3 in CSV format by `ingestion/Ingestion.py`.

### ETL

The data is extracted from S3 and transformed using AWS Glue. The data is then loaded into an S3 data lake in Parquet format.

## Getting Started

### Installation

1. Install dependencies

```shell
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Create Lambda `ingestion/config.yaml` file

```yaml
region: #<REGION>

function_name: run_ingestion
handler: Ingestion.lambda_handler
description: Run ingestion on AFL data from Squiggle
runtime: python3.11

aws_access_key_id: #<AWS ACCESS KEY ID>
aws_secret_access_key: #<AWS SECRET ACCESS KEY>

environment_variables:
  ACCESS_KEY_ID: #<AWS ACCESS KEY ID>
  SECRET_ACCESS_KEY: #<AWS SECRET ACCESS KEY>

build:
  source_directories: lib
```

3. Deploy Lambda to AWS

```shell
lambda deploy
```

4. Deploy Glue job to AWS from `etl/main.py`
