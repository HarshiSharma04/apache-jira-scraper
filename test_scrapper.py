"""
Unit tests for the Apache Jira Scraper

Run with: python -m pytest test_scraper.py -v
Or simply: python test_scraper.py
"""

import json
import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import shutil

import config
import utils
from scraper import JiraScraper
from transformer import DataTransformer


class TestUtils(unittest.TestCase):
    """Test utility functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = Path(tempfile.mkdtemp())
        
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_clean_text(self):
        """Test text cleaning"""
        # Test whitespace removal
        result = utils.clean_text("  hello   world  ")
        self.assertEqual(result, "hello world")
        
        # Test None handling
        result = utils.clean_text(None)
        self.assertEqual(result, "")
        
        # Test truncation
        result = utils.clean_text("a" * 100, max_length=10)
        self.assertEqual(len(result), 13)  # 10 + "..."
    
    def test_extract_user_info(self):
        """Test user info extraction"""
        # Test with displayName
        user = {"displayName": "John Doe", "name": "jdoe"}
        result = utils.extract_user_info(user)
        self.assertEqual(result, "John Doe")
        
        # Test with only name
        user = {"name": "jdoe"}
        result = utils.extract_user_info(user)
        self.assertEqual(result, "jdoe")
        
        # Test with None
        result = utils.extract_user_info(None)
        self.assertEqual(result, "Unknown")
    
    def test_calculate_sleep_time(self):
        """Test exponential backoff calculation"""
        # Test exponential growth
        self.assertEqual(utils.calculate_sleep_time(0), 1)
        self.assertEqual(utils.calculate_sleep_time(1), 2)
        self.assertEqual(utils.calculate_sleep_time(2), 4)
        
        # Test cap at 60 seconds
        self.assertEqual(utils.calculate_sleep_time(10), 60)
    
    def test_validate_json_structure(self):
        """Test JSON validation"""
        data = {"field1": "value1", "field2": "value2"}
        
        # Test valid
        self.assertTrue(utils.validate_json_structure(data, ["field1", "field2"]))
        
        # Test invalid
        self.assertFalse(utils.validate_json_structure(data, ["field3"]))


class TestJiraScraper(unittest.TestCase):
    """Test Jira scraper functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.project = "TEST"
        self.scraper = JiraScraper(self.project)
        
    @patch('scraper.requests.Session.get')
    def test_make_request_success(self, mock_get):
        """Test successful API request"""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"test": "data"}
        mock_get.return_value = mock_response
        
        result = self.scraper._make_request("http://test.com")
        self.assertEqual(result, {"test": "data"})
    
    @patch('scraper.requests.Session.get')
    def test_make_request_rate_limit(self, mock_get):
        """Test rate limit handling"""
        # Mock rate limit response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {'Retry-After': '1'}
        mock_get.return_value = mock_response
        
        with self.assertRaises(Exception):
            self.scraper._make_request("http://test.com")
    
    def test_extract_issue_data(self):
        """Test issue data extraction"""
        # Mock issue data
        issue = {
            "key": "TEST-123",
            "id": "12345",
            "fields": {
                "summary": "Test Issue",
                "description": "Test description",
                "status": {"name": "Open"},
                "priority": {"name": "Major"},
                "issuetype": {"name": "Bug"},
                "reporter": {"displayName": "Reporter User"},
                "assignee": {"displayName": "Assignee User"},
                "created": "2024-01-01T00:00:00.000+0000",
                "updated": "2024-01-02T00:00:00.000+0000",
                "labels": ["test", "bug"],
                "components": [{"name": "Core"}],
                "comment": {
                    "comments": [
                        {
                            "author": {"displayName": "Commenter"},
                            "created": "2024-01-03T00:00:00.000+0000",
                            "body": "Test comment"
                        }
                    ]
                }
            }
        }
        
        result = self.scraper._extract_issue_data(issue)
        
        # Verify extracted data
        self.assertEqual(result["issue_key"], "TEST-123")
        self.assertEqual(result["title"], "Test Issue")
        self.assertEqual(result["status"], "Open")
        self.assertEqual(result["priority"], "Major")
        self.assertEqual(result["comment_count"], 1)
        self.assertEqual(len(result["comments"]), 1)


class TestDataTransformer(unittest.TestCase):
    """Test data transformation functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.project = "TEST"
        self.transformer = DataTransformer(self.project)
        
        # Sample issue data
        self.sample_issue = {
            "issue_key": "TEST-123",
            "issue_id": "12345",
            "project": "TEST",
            "url": "http://test.com/TEST-123",
            "title": "Test Issue",
            "description": "This is a test issue",
            "status": "Open",
            "priority": "Major",
            "issue_type": "Bug",
            "reporter": "Reporter User",
            "assignee": "Assignee User",
            "created": "2024-01-01T00:00:00.000+0000",
            "updated": "2024-01-02T00:00:00.000+0000",
            "resolved": "",
            "labels": ["test"],
            "components": ["Core"],
            "versions": [],
            "fix_versions": [],
            "comments": [
                {
                    "author": "Commenter",
                    "created": "2024-01-03T00:00:00.000+0000",
                    "body": "Test comment"
                }
            ],
            "comment_count": 1
        }
    
    def test_create_summarization_task(self):
        """Test summarization task creation"""
        task = self.transformer._create_summarization_task(self.sample_issue)
        
        self.assertEqual(task["task_type"], "summarization")
        self.assertIn("instruction", task)
        self.assertIn("input", task)
        self.assertIn("output", task)
        self.assertIn("metadata", task)
        self.assertIn("Test Issue", task["input"])
    
    def test_create_classification_task(self):
        """Test classification task creation"""
        task = self.transformer._create_classification_task(self.sample_issue)
        
        self.assertEqual(task["task_type"], "classification")
        self.assertEqual(task["output"], "Major")
        self.assertIn("Test Issue", task["input"])
    
    def test_create_qa_task(self):
        """Test Q&A task creation"""
        tasks = self.transformer._create_qa_task(self.sample_issue)
        
        self.assertIsInstance(tasks, list)
        self.assertGreater(len(tasks), 0)
        
        # Check first task
        first_task = tasks[0]
        self.assertEqual(first_task["task_type"], "question_answering")
        self.assertIn("Question:", first_task["input"])
    
    def test_transform_issue(self):
        """Test full issue transformation"""
        examples = self.transformer.transform_issue(self.sample_issue)
        
        self.assertIsInstance(examples, list)
        self.assertGreater(len(examples), 0)
        
        # Check that all examples have required fields
        for example in examples:
            self.assertIn("task_type", example)
            self.assertIn("instruction", example)
            self.assertIn("input", example)
            self.assertIn("output", example)
            self.assertIn("metadata", example)
    
    def test_generate_statistics(self):
        """Test statistics generation"""
        examples = self.transformer.transform_issue(self.sample_issue)
        stats = self.transformer.generate_statistics(examples)
        
        self.assertIn("total_examples", stats)
        self.assertIn("task_type_distribution", stats)
        self.assertEqual(stats["total_examples"], len(examples))


class TestEndToEnd(unittest.TestCase):
    """End-to-end integration tests"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = Path(tempfile.mkdtemp())
        
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_checkpoint_save_load(self):
        """Test checkpoint saving and loading"""
        project = "TEST"
        checkpoint_data = {
            "project": project,
            "issues_processed": 100,
            "last_issue_key": "TEST-100"
        }
        
        # Save checkpoint
        filepath = utils.save_checkpoint(project, checkpoint_data)
        self.assertTrue(filepath.exists())
        
        # Load checkpoint
        loaded_data = utils.load_checkpoint(project)
        self.assertIsNotNone(loaded_data)
        self.assertEqual(loaded_data["issues_processed"], 100)
    
    def test_jsonl_merge(self):
        """Test JSONL file merging"""
        # Create test JSONL files
        file1 = self.test_dir / "file1.jsonl"
        file2 = self.test_dir / "file2.jsonl"
        output = self.test_dir / "merged.jsonl"
        
        # Write test data
        with open(file1, 'w') as f:
            f.write('{"test": 1}\n')
            f.write('{"test": 2}\n')
        
        with open(file2, 'w') as f:
            f.write('{"test": 3}\n')
        
        # Merge files
        count = utils.merge_jsonl_files([file1, file2], output)
        
        self.assertEqual(count, 3)
        self.assertTrue(output.exists())
        
        # Verify content
        with open(output, 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 3)


def run_tests():
    """Run all tests"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestUtils))
    suite.addTests(loader.loadTestsFromTestCase(TestJiraScraper))
    suite.addTests(loader.loadTestsFromTestCase(TestDataTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEnd))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return exit code
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    import sys
    sys.exit(run_tests())


