"""
Example usage and testing script for the Apache Jira Scraper

This script demonstrates:
1. How to use individual components
2. How to load and analyze the output
3. How to resume from checkpoint
"""

import json
import jsonlines
from pathlib import Path

import config
from scraper import JiraScraper
from transformer import DataTransformer
import utils


def example_1_scrape_single_project():
    """
    Example 1: Scrape a single project
    """
    print("=" * 80)
    print("Example 1: Scrape Single Project")
    print("=" * 80)
    
    # Initialize scraper
    scraper = JiraScraper("KAFKA")
    
    # Scrape issues
    issues = scraper.scrape_all_issues()
    
    print(f"Scraped {len(issues)} issues")
    
    # Save raw data
    scraper.save_raw_data(issues)
    
    return issues


def example_2_transform_data():
    """
    Example 2: Transform raw data into training format
    """
    print("\n" + "=" * 80)
    print("Example 2: Transform Data")
    print("=" * 80)
    
    # Load raw data
    raw_file = config.RAW_DATA_DIR / "KAFKA_raw.json"
    
    if not raw_file.exists():
        print("Run example_1 first to scrape data!")
        return
    
    with open(raw_file, 'r', encoding='utf-8') as f:
        issues = json.load(f)
    
    # Initialize transformer
    transformer = DataTransformer("KAFKA")
    
    # Transform issues
    training_examples = transformer.transform_all_issues(issues)
    
    print(f"Created {len(training_examples)} training examples")
    
    # Save to JSONL
    output_file = config.PROCESSED_DATA_DIR / "kafka_examples.jsonl"
    transformer.save_to_jsonl(training_examples, output_file)
    
    return training_examples


def example_3_analyze_output():
    """
    Example 3: Analyze output data
    """
    print("\n" + "=" * 80)
    print("Example 3: Analyze Output")
    print("=" * 80)
    
    # Load JSONL file
    output_file = config.PROCESSED_DATA_DIR / "apache_jira_corpus.jsonl"
    
    if not output_file.exists():
        print("Run main.py first to create the corpus!")
        return
    
    examples = []
    with jsonlines.open(output_file) as reader:
        for obj in reader:
            examples.append(obj)
    
    print(f"Total examples: {len(examples)}")
    
    # Count by task type
    task_counts = {}
    for example in examples:
        task_type = example["task_type"]
        task_counts[task_type] = task_counts.get(task_type, 0) + 1
    
    print("\nTask type distribution:")
    for task_type, count in sorted(task_counts.items()):
        print(f"  {task_type}: {count} ({count/len(examples)*100:.1f}%)")
    
    # Show example
    print("\nExample training instance:")
    print(json.dumps(examples[0], indent=2))
    
    return examples


def example_4_resume_from_checkpoint():
    """
    Example 4: Resume from checkpoint
    """
    print("\n" + "=" * 80)
    print("Example 4: Resume from Checkpoint")
    print("=" * 80)
    
    # Load checkpoint
    checkpoint = utils.load_checkpoint("KAFKA")
    
    if not checkpoint:
        print("No checkpoint found for KAFKA")
        return
    
    print("Checkpoint found:")
    print(json.dumps(checkpoint, indent=2))
    
    # Resume scraping
    scraper = JiraScraper("KAFKA")
    print(f"Resuming from issue {checkpoint['issues_processed']}")
    
    # The scraper automatically loads and uses the checkpoint
    issues = scraper.scrape_all_issues()
    
    return issues


def example_5_custom_project():
    """
    Example 5: Scrape a custom project
    """
    print("\n" + "=" * 80)
    print("Example 5: Custom Project")
    print("=" * 80)
    
    # You can scrape any Apache project
    custom_project = "HBASE"  # Change this to any Apache project
    
    print(f"Scraping custom project: {custom_project}")
    
    # Initialize scraper
    scraper = JiraScraper(custom_project)
    
    # Scrape issues (will take a while)
    issues = scraper.scrape_all_issues()
    
    # Save raw data
    scraper.save_raw_data(issues)
    
    # Transform data
    transformer = DataTransformer(custom_project)
    training_examples = transformer.transform_all_issues(issues)
    
    # Save to JSONL
    output_file = config.PROCESSED_DATA_DIR / f"{custom_project.lower()}_issues.jsonl"
    transformer.save_to_jsonl(training_examples, output_file)
    
    print(f"Custom project complete: {len(issues)} issues, {len(training_examples)} examples")


def example_6_load_and_filter():
    """
    Example 6: Load data and filter by criteria
    """
    print("\n" + "=" * 80)
    print("Example 6: Load and Filter Data")
    print("=" * 80)
    
    output_file = config.PROCESSED_DATA_DIR / "apache_jira_corpus.jsonl"
    
    if not output_file.exists():
        print("Run main.py first!")
        return
    
    # Load all examples
    examples = []
    with jsonlines.open(output_file) as reader:
        for obj in reader:
            examples.append(obj)
    
    # Filter by task type
    summarization_tasks = [e for e in examples if e["task_type"] == "summarization"]
    print(f"Summarization tasks: {len(summarization_tasks)}")
    
    # Filter by project
    kafka_examples = [e for e in examples if e["metadata"]["project"] == "KAFKA"]
    print(f"KAFKA examples: {len(kafka_examples)}")
    
    # Filter by priority
    critical_issues = [e for e in examples if e["metadata"].get("priority") == "Critical"]
    print(f"Critical issues: {len(critical_issues)}")
    
    # Save filtered subset
    filtered_file = config.PROCESSED_DATA_DIR / "filtered_examples.jsonl"
    with jsonlines.open(filtered_file, mode='w') as writer:
        for example in summarization_tasks[:100]:  # First 100
            writer.write(example)
    
    print(f"Saved filtered examples to: {filtered_file}")


def main():
    """
    Run all examples
    """
    print("\n")
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 20 + "Apache Jira Scraper - Examples" + " " * 28 + "║")
    print("╚" + "═" * 78 + "╝")
    print("\n")
    
    # Setup logging
    logger = utils.setup_logging()
    
    print("Choose an example to run:")
    print("1. Scrape single project (KAFKA)")
    print("2. Transform raw data")
    print("3. Analyze output")
    print("4. Resume from checkpoint")
    print("5. Scrape custom project")
    print("6. Load and filter data")
    print("0. Run all examples")
    print()
    
    choice = input("Enter choice (0-6): ").strip()
    
    if choice == "1":
        example_1_scrape_single_project()
    elif choice == "2":
        example_2_transform_data()
    elif choice == "3":
        example_3_analyze_output()
    elif choice == "4":
        example_4_resume_from_checkpoint()
    elif choice == "5":
        example_5_custom_project()
    elif choice == "6":
        example_6_load_and_filter()
    elif choice == "0":
        # Run examples that don't require scraping
        print("\nRunning analysis examples...")
        example_3_analyze_output()
        example_6_load_and_filter()
    else:
        print("Invalid choice!")


if __name__ == "__main__":
    main()