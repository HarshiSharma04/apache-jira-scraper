"""
Utility functions for the scraper
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import config


def setup_logging(log_file: Path = config.LOG_FILE, 
                  log_level: str = config.LOG_LEVEL) -> logging.Logger:
    """
    Set up logging configuration
    
    Args:
        log_file: Path to log file
        log_level: Logging level
        
    Returns:
        Configured logger
    """
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=config.LOG_FORMAT,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def save_checkpoint(project: str, data: Dict[str, Any]) -> Path:
    """
    Save checkpoint data to disk
    
    Args:
        project: Project name
        data: Checkpoint data to save
        
    Returns:
        Path to saved checkpoint file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = config.CHECKPOINT_FILE_PATTERN.format(
        project=project,
        timestamp=timestamp
    )
    filepath = config.CHECKPOINT_DIR / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return filepath


def load_checkpoint(project: str) -> Optional[Dict[str, Any]]:
    """
    Load the most recent checkpoint for a project
    
    Args:
        project: Project name
        
    Returns:
        Checkpoint data or None if no checkpoint exists
    """
    checkpoint_files = list(config.CHECKPOINT_DIR.glob(f"checkpoint_{project}_*.json"))
    
    if not checkpoint_files:
        return None
    
    # Get most recent checkpoint
    latest_checkpoint = max(checkpoint_files, key=lambda p: p.stat().st_mtime)
    
    with open(latest_checkpoint, 'r', encoding='utf-8') as f:
        return json.load(f)


def clean_text(text: Optional[str], max_length: Optional[int] = None) -> str:
    """
    Clean and normalize text data
    
    Args:
        text: Text to clean
        max_length: Maximum length to truncate to
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    
    # Remove excessive whitespace
    text = " ".join(text.split())
    
    # Truncate if needed
    if max_length and len(text) > max_length:
        text = text[:max_length] + "..."
    
    return text


def extract_user_info(user_obj: Optional[Dict[str, Any]]) -> str:
    """
    Extract user information from Jira user object
    
    Args:
        user_obj: Jira user object
        
    Returns:
        User display name or 'Unknown'
    """
    if not user_obj:
        return "Unknown"
    
    return user_obj.get("displayName", user_obj.get("name", "Unknown"))


def format_timestamp(timestamp: Optional[str]) -> str:
    """
    Format Jira timestamp to ISO format
    
    Args:
        timestamp: Jira timestamp string
        
    Returns:
        Formatted timestamp or empty string
    """
    if not timestamp:
        return ""
    
    try:
        # Jira timestamps are already in ISO format
        return timestamp
    except Exception:
        return ""


def calculate_sleep_time(retry_count: int, 
                        backoff_factor: int = config.RETRY_BACKOFF_FACTOR) -> float:
    """
    Calculate sleep time for exponential backoff
    
    Args:
        retry_count: Current retry attempt number
        backoff_factor: Backoff multiplication factor
        
    Returns:
        Sleep time in seconds
    """
    return min(backoff_factor ** retry_count, 60)  # Cap at 60 seconds


def merge_jsonl_files(input_files: list, output_file: Path) -> int:
    """
    Merge multiple JSONL files into one
    
    Args:
        input_files: List of input file paths
        output_file: Output file path
        
    Returns:
        Number of lines written
    """
    count = 0
    
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for input_file in input_files:
            if not input_file.exists():
                continue
                
            with open(input_file, 'r', encoding='utf-8') as infile:
                for line in infile:
                    outfile.write(line)
                    count += 1
    
    return count


def get_file_size_mb(filepath: Path) -> float:
    """
    Get file size in megabytes
    
    Args:
        filepath: Path to file
        
    Returns:
        File size in MB
    """
    if not filepath.exists():
        return 0.0
    
    return filepath.stat().st_size / (1024 * 1024)


def validate_json_structure(data: Dict[str, Any], required_fields: list) -> bool:
    """
    Validate that a JSON object has required fields
    
    Args:
        data: JSON data to validate
        required_fields: List of required field names
        
    Returns:
        True if valid, False otherwise
    """
    return all(field in data for field in required_fields)


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename to remove invalid characters
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename
    """
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename