"""
Data Transformer Module - Converts scraped data into LLM training format
"""

import json
import logging
from typing import Any, Dict, List
from pathlib import Path

import jsonlines
from tqdm import tqdm

import config
import utils


class DataTransformer:
    """
    Transform raw Jira data into structured JSONL format for LLM training
    """
    
    def __init__(self, project: str):
        """
        Initialize the transformer
        
        Args:
            project: Project name
        """
        self.project = project
        self.logger = logging.getLogger(f"{__name__}.{project}")
    
    def _create_summarization_task(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a summarization task from issue data
        
        Args:
            issue: Issue data
            
        Returns:
            Summarization task dictionary
        """
        # Combine description and comments for context
        context_parts = []
        
        if issue.get("description"):
            context_parts.append(f"Description: {issue['description']}")
        
        if issue.get("comments"):
            for i, comment in enumerate(issue["comments"][:3], 1):  # First 3 comments
                context_parts.append(f"Comment {i}: {comment['body']}")
        
        context = "\n\n".join(context_parts)
        
        return {
            "task_type": "summarization",
            "instruction": "Summarize the following software issue and its discussion:",
            "input": context,
            "output": f"{issue['title']} (Status: {issue['status']}, Priority: {issue['priority']})",
            "metadata": {
                "issue_key": issue["issue_key"],
                "project": issue["project"],
                "url": issue["url"]
            }
        }
    
    def _create_classification_task(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a classification task (predict priority)
        
        Args:
            issue: Issue data
            
        Returns:
            Classification task dictionary
        """
        input_text = f"Title: {issue['title']}\n"
        if issue.get("description"):
            input_text += f"Description: {issue['description'][:500]}"  # First 500 chars
        
        return {
            "task_type": "classification",
            "instruction": "Classify the priority of this software issue (Blocker, Critical, Major, Minor, Trivial):",
            "input": input_text,
            "output": issue["priority"],
            "metadata": {
                "issue_key": issue["issue_key"],
                "project": issue["project"],
                "issue_type": issue["issue_type"],
                "url": issue["url"]
            }
        }
    
    def _create_status_prediction_task(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a status prediction task
        
        Args:
            issue: Issue data
            
        Returns:
            Status prediction task dictionary
        """
        input_text = f"Issue: {issue['title']}\n"
        input_text += f"Type: {issue['issue_type']}\n"
        input_text += f"Priority: {issue['priority']}\n"
        
        if issue.get("description"):
            input_text += f"Description: {issue['description'][:500]}"
        
        return {
            "task_type": "status_prediction",
            "instruction": "Predict the current status of this software issue:",
            "input": input_text,
            "output": issue["status"],
            "metadata": {
                "issue_key": issue["issue_key"],
                "project": issue["project"],
                "url": issue["url"]
            }
        }
    
    def _create_qa_task(self, issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Create question-answering tasks from issue data
        
        Args:
            issue: Issue data
            
        Returns:
            List of QA task dictionaries
        """
        qa_tasks = []
        
        # Q1: What is the issue about?
        qa_tasks.append({
            "task_type": "question_answering",
            "instruction": "Answer the following question about this software issue:",
            "input": f"Issue Key: {issue['issue_key']}\nTitle: {issue['title']}\nDescription: {issue.get('description', 'N/A')}\n\nQuestion: What is this issue about?",
            "output": issue['title'],
            "metadata": {
                "issue_key": issue["issue_key"],
                "project": issue["project"],
                "question_type": "summary",
                "url": issue["url"]
            }
        })
        
        # Q2: What is the status?
        qa_tasks.append({
            "task_type": "question_answering",
            "instruction": "Answer the following question about this software issue:",
            "input": f"Issue Key: {issue['issue_key']}\nTitle: {issue['title']}\n\nQuestion: What is the current status of this issue?",
            "output": issue['status'],
            "metadata": {
                "issue_key": issue["issue_key"],
                "project": issue["project"],
                "question_type": "status",
                "url": issue["url"]
            }
        })
        
        # Q3: Who is assigned?
        if issue.get("assignee") and issue["assignee"] != "Unknown":
            qa_tasks.append({
                "task_type": "question_answering",
                "instruction": "Answer the following question about this software issue:",
                "input": f"Issue Key: {issue['issue_key']}\nTitle: {issue['title']}\n\nQuestion: Who is assigned to this issue?",
                "output": issue["assignee"],
                "metadata": {
                    "issue_key": issue["issue_key"],
                    "project": issue["project"],
                    "question_type": "assignee",
                    "url": issue["url"]
                }
            })
        
        return qa_tasks
    
    def _create_issue_resolution_task(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create an issue resolution task (if issue has comments showing resolution)
        
        Args:
            issue: Issue data
            
        Returns:
            Issue resolution task dictionary or None
        """
        if not issue.get("comments") or issue["status"] not in ["Resolved", "Closed"]:
            return None
        
        # Get the last few comments which might contain resolution info
        last_comments = issue["comments"][-2:]
        resolution_context = "\n".join([c["body"] for c in last_comments])
        
        input_text = f"Issue: {issue['title']}\n"
        if issue.get("description"):
            input_text += f"Description: {issue['description'][:500]}\n"
        input_text += f"\nHow was this issue resolved?"
        
        return {
            "task_type": "issue_resolution",
            "instruction": "Based on the issue discussion, explain how this issue was resolved:",
            "input": input_text,
            "output": resolution_context[:500],  # First 500 chars of resolution discussion
            "metadata": {
                "issue_key": issue["issue_key"],
                "project": issue["project"],
                "status": issue["status"],
                "url": issue["url"]
            }
        }
    
    def transform_issue(self, issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform a single issue into multiple training examples
        
        Args:
            issue: Raw issue data
            
        Returns:
            List of training examples
        """
        training_examples = []
        
        try:
            # Create base metadata for all tasks
            base_metadata = {
                "issue_key": issue["issue_key"],
                "project": issue["project"],
                "issue_type": issue["issue_type"],
                "priority": issue["priority"],
                "status": issue["status"],
                "created": issue["created"],
                "url": issue["url"],
                "labels": issue.get("labels", []),
                "components": issue.get("components", [])
            }
            
            # 1. Summarization task
            if issue.get("description") or issue.get("comments"):
                summ_task = self._create_summarization_task(issue)
                summ_task["metadata"].update(base_metadata)
                training_examples.append(summ_task)
            
            # 2. Classification task (priority prediction)
            if issue.get("title") and issue.get("priority"):
                class_task = self._create_classification_task(issue)
                class_task["metadata"].update(base_metadata)
                training_examples.append(class_task)
            
            # 3. Status prediction task
            if issue.get("title") and issue.get("status"):
                status_task = self._create_status_prediction_task(issue)
                status_task["metadata"].update(base_metadata)
                training_examples.append(status_task)
            
            # 4. Question-answering tasks
            qa_tasks = self._create_qa_task(issue)
            for qa_task in qa_tasks:
                qa_task["metadata"].update
                qa_task["metadata"].update(base_metadata)
                training_examples.append(qa_task)
            
            # 5. Issue resolution task (if applicable)
            resolution_task = self._create_issue_resolution_task(issue)
            if resolution_task:
                resolution_task["metadata"].update(base_metadata)
                training_examples.append(resolution_task)
            
        except Exception as e:
            self.logger.error(f"Error transforming issue {issue.get('issue_key', 'unknown')}: {str(e)}")
        
        return training_examples
    
    def transform_all_issues(self, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform all issues into training examples
        
        Args:
            issues: List of raw issue data
            
        Returns:
            List of all training examples
        """
        all_examples = []
        
        self.logger.info(f"Transforming {len(issues)} issues into training examples...")
        
        for issue in tqdm(issues, desc=f"Transforming {self.project}", unit="issue"):
            examples = self.transform_issue(issue)
            all_examples.extend(examples)
        
        self.logger.info(f"Created {len(all_examples)} training examples from {len(issues)} issues")
        
        return all_examples
    
    def save_to_jsonl(self, examples: List[Dict[str, Any]], output_file: Path) -> str:
        """
        Save training examples to JSONL format
        
        Args:
            examples: List of training examples
            output_file: Output file path
            
        Returns:
            Path to saved file
        """
        with jsonlines.open(output_file, mode='w') as writer:
            for example in examples:
                writer.write(example)
        
        self.logger.info(f"Saved {len(examples)} examples to {output_file} ({utils.get_file_size_mb(output_file):.2f} MB)")
        return str(output_file)
    
    def generate_statistics(self, examples: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate statistics about the transformed dataset
        
        Args:
            examples: List of training examples
            
        Returns:
            Statistics dictionary
        """
        stats = {
            "total_examples": len(examples),
            "task_type_distribution": {},
            "projects": set(),
            "issue_types": set(),
            "priorities": set(),
            "statuses": set()
        }
        
        for example in examples:
            # Task type distribution
            task_type = example.get("task_type", "unknown")
            stats["task_type_distribution"][task_type] = stats["task_type_distribution"].get(task_type, 0) + 1
            
            # Metadata statistics
            metadata = example.get("metadata", {})
            if "project" in metadata:
                stats["projects"].add(metadata["project"])
            if "issue_type" in metadata:
                stats["issue_types"].add(metadata["issue_type"])
            if "priority" in metadata:
                stats["priorities"].add(metadata["priority"])
            if "status" in metadata:
                stats["statuses"].add(metadata["status"])
        
        # Convert sets to lists for JSON serialization
        stats["projects"] = list(stats["projects"])
        stats["issue_types"] = list(stats["issue_types"])
        stats["priorities"] = list(stats["priorities"])
        stats["statuses"] = list(stats["statuses"])
        
        return stats
