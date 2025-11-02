"""
Jira Scraper Module - Handles all data extraction from Apache Jira
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from tqdm import tqdm

import config
import utils


class JiraScraper:
    """
    Main scraper class for Apache Jira
    """
    
    def __init__(self, project: str):
        """
        Initialize the scraper
        
        Args:
            project: Jira project key (e.g., 'KAFKA')
        """
        self.project = project
        self.logger = logging.getLogger(f"{__name__}.{project}")
        self.session = self._create_session()
        self.issues_scraped = 0
        self.checkpoint_data = self._load_or_create_checkpoint()
        
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry logic
        
        Returns:
            Configured requests session
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=config.MAX_RETRIES,
            backoff_factor=config.RETRY_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(config.HEADERS)
        
        return session
    
    def _load_or_create_checkpoint(self) -> Dict[str, Any]:
        """
        Load existing checkpoint or create new one
        
        Returns:
            Checkpoint data dictionary
        """
        checkpoint = utils.load_checkpoint(self.project)
        
        if checkpoint:
            self.logger.info(f"Loaded checkpoint for {self.project}: {checkpoint['issues_processed']} issues processed")
            return checkpoint
        
        return {
            "project": self.project,
            "issues_processed": 0,
            "last_issue_key": None,
            "start_time": datetime.now().isoformat(),
            "last_update": datetime.now().isoformat()
        }
    
    def _save_checkpoint(self):
        """Save current progress to checkpoint file"""
        self.checkpoint_data["last_update"] = datetime.now().isoformat()
        self.checkpoint_data["issues_processed"] = self.issues_scraped
        utils.save_checkpoint(self.project, self.checkpoint_data)
        self.logger.info(f"Checkpoint saved: {self.issues_scraped} issues processed")
    
    @retry(
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.Timeout))
    )
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic and error handling
        
        Args:
            url: URL to request
            params: Query parameters
            
        Returns:
            JSON response data
            
        Raises:
            requests.exceptions.RequestException: On request failure
        """
        try:
            response = self.session.get(
                url,
                params=params,
                timeout=config.REQUEST_TIMEOUT
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                raise requests.exceptions.RequestException("Rate limited")
            
            # Handle server errors
            if response.status_code >= 500:
                self.logger.error(f"Server error: {response.status_code}")
                raise requests.exceptions.RequestException(f"Server error: {response.status_code}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.Timeout:
            self.logger.error(f"Request timeout for URL: {url}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {str(e)}")
            raise requests.exceptions.RequestException(f"Invalid JSON response: {str(e)}")
    
    def _search_issues(self, start_at: int = 0) -> Dict[str, Any]:
        """
        Search for issues in the project
        
        Args:
            start_at: Starting index for pagination
            
        Returns:
            Search results with issues and pagination info
        """
        url = f"{config.JIRA_API_URL}/search"
        
        jql = f"project = {self.project} ORDER BY created ASC"
        
        params = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": config.MAX_RESULTS_PER_PAGE,
            "fields": ",".join(config.JIRA_FIELDS)
        }
        
        self.logger.debug(f"Searching issues: startAt={start_at}")
        return self._make_request(url, params)
    
    def _get_issue_details(self, issue_key: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific issue
        
        Args:
            issue_key: Issue key (e.g., 'KAFKA-1234')
            
        Returns:
            Issue details
        """
        url = f"{config.JIRA_API_URL}/issue/{issue_key}"
        
        params = {
            "fields": ",".join(config.JIRA_FIELDS),
            "expand": "renderedFields"
        }
        
        return self._make_request(url, params)
    
    def _extract_issue_data(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and structure data from a Jira issue
        
        Args:
            issue: Raw issue data from Jira API
            
        Returns:
            Structured issue data
        """
        fields = issue.get("fields", {})
        
        # Extract basic metadata
        issue_data = {
            "issue_key": issue.get("key", ""),
            "issue_id": issue.get("id", ""),
            "project": self.project,
            "url": f"{config.JIRA_BASE_URL}/browse/{issue.get('key', '')}",
            
            # Title and description
            "title": utils.clean_text(fields.get("summary", "")),
            "description": utils.clean_text(
                fields.get("description", ""),
                max_length=config.MAX_DESCRIPTION_LENGTH
            ),
            
            # Status and priority
            "status": fields.get("status", {}).get("name", "Unknown"),
            "priority": fields.get("priority", {}).get("name", "Unknown"),
            "issue_type": fields.get("issuetype", {}).get("name", "Unknown"),
            
            # People
            "reporter": utils.extract_user_info(fields.get("reporter")),
            "assignee": utils.extract_user_info(fields.get("assignee")),
            
            # Timestamps
            "created": utils.format_timestamp(fields.get("created")),
            "updated": utils.format_timestamp(fields.get("updated")),
            "resolved": utils.format_timestamp(fields.get("resolutiondate")),
            
            # Labels and components
            "labels": fields.get("labels", []),
            "components": [c.get("name", "") for c in fields.get("components", [])],
            "versions": [v.get("name", "") for v in fields.get("versions", [])],
            "fix_versions": [v.get("name", "") for v in fields.get("fixVersions", [])],
            
            # Comments
            "comments": []
        }
        
        # Extract comments
        comment_data = fields.get("comment", {})
        comments = comment_data.get("comments", [])
        
        for comment in comments:
            comment_text = utils.clean_text(
                comment.get("body", ""),
                max_length=config.MAX_COMMENT_LENGTH
            )
            
            if comment_text:
                issue_data["comments"].append({
                    "author": utils.extract_user_info(comment.get("author")),
                    "created": utils.format_timestamp(comment.get("created")),
                    "body": comment_text
                })
        
        issue_data["comment_count"] = len(issue_data["comments"])
        
        return issue_data
    
    def scrape_all_issues(self) -> List[Dict[str, Any]]:
        """
        Scrape all issues from the project
        
        Returns:
            List of all scraped issues
        """
        all_issues = []
        start_at = self.checkpoint_data.get("issues_processed", 0)
        
        self.logger.info(f"Starting scrape for project {self.project} from issue {start_at}")
        
        try:
            # Get total count first
            initial_search = self._search_issues(start_at=0)
            total_issues = initial_search.get("total", 0)
            
            self.logger.info(f"Total issues to scrape: {total_issues}")
            
            # Progress bar
            pbar = tqdm(
                total=total_issues,
                initial=start_at,
                desc=f"Scraping {self.project}",
                unit="issue"
            )
            
            while True:
                # Search for issues
                search_results = self._search_issues(start_at=start_at)
                issues = search_results.get("issues", [])
                
                if not issues:
                    break
                
                # Process each issue
                for issue in issues:
                    try:
                        # Extract issue data
                        issue_data = self._extract_issue_data(issue)
                        all_issues.append(issue_data)
                        
                        # Update tracking
                        self.issues_scraped += 1
                        self.checkpoint_data["last_issue_key"] = issue_data["issue_key"]
                        
                        pbar.update(1)
                        
                        # Save checkpoint periodically
                        if self.issues_scraped % config.SAVE_CHECKPOINT_EVERY == 0:
                            self._save_checkpoint()
                        
                        # Small delay to be respectful
                        time.sleep(0.1)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing issue {issue.get('key', 'unknown')}: {str(e)}")
                        continue
                
                # Check if we've processed all issues
                start_at += len(issues)
                if start_at >= search_results.get("total", 0):
                    break
            
            pbar.close()
            
            # Final checkpoint save
            self._save_checkpoint()
            
            self.logger.info(f"Scraping complete for {self.project}: {len(all_issues)} issues scraped")
            
        except Exception as e:
            self.logger.error(f"Fatal error during scraping: {str(e)}")
            self._save_checkpoint()
            raise
        
        return all_issues
    
    def save_raw_data(self, issues: List[Dict[str, Any]]) -> str:
        """
        Save raw scraped data to JSON file
        
        Args:
            issues: List of issue data
            
        Returns:
            Path to saved file
        """
        filename = f"{self.project}_raw.json"
        filepath = config.RAW_DATA_DIR / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(issues, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Raw data saved to {filepath} ({utils.get_file_size_mb(filepath):.2f} MB)")
        return str(filepath)