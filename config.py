"""
Configuration file for Apache Jira Scraper
"""

import os
from pathlib import Path

# Base configuration
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
LOGS_DIR = BASE_DIR / "logs"

# Create directories if they don't exist
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, CHECKPOINT_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Jira configuration
JIRA_BASE_URL = "https://issues.apache.org/jira"
JIRA_API_URL = f"{JIRA_BASE_URL}/rest/api/2"

# Projects to scrape (can be modified)
PROJECTS = [
    "KAFKA",  # Apache Kafka
    "SPARK",  # Apache Spark
    "HADOOP"  # Apache Hadoop
]

# Scraping configuration
MAX_RESULTS_PER_PAGE = 50  # Jira API max is 100, using 50 for safety
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 5
RETRY_BACKOFF_FACTOR = 2  # Exponential backoff: 1, 2, 4, 8, 16 seconds

# Rate limiting (requests per minute)
RATE_LIMIT_CALLS = 50
RATE_LIMIT_PERIOD = 60  # seconds

# Processing configuration
BATCH_SIZE = 100  # Number of issues to process before saving checkpoint
MAX_COMMENT_LENGTH = 10000  # Truncate very long comments
MAX_DESCRIPTION_LENGTH = 20000  # Truncate very long descriptions

# Checkpoint configuration
SAVE_CHECKPOINT_EVERY = 50  # Save checkpoint every N issues
CHECKPOINT_FILE_PATTERN = "checkpoint_{project}_{timestamp}.json"

# Output configuration
OUTPUT_FILE_PATTERN = "{project}_issues.jsonl"
FINAL_OUTPUT_FILE = "apache_jira_corpus.jsonl"

# Logging configuration
LOG_FILE = LOGS_DIR / "scraper.log"
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# HTTP Headers
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Fields to extract from Jira API
JIRA_FIELDS = [
    "summary",
    "description",
    "status",
    "priority",
    "issuetype",
    "project",
    "reporter",
    "assignee",
    "created",
    "updated",
    "resolutiondate",
    "labels",
    "components",
    "versions",
    "fixVersions",
    "comment"
]

# LLM Training Task Types
TASK_TYPES = [
    "summarization",
    "classification",
    "question_answering",
    "issue_resolution",
    "priority_prediction"
]