"""
Main script to run the Apache Jira scraper and data transformation pipeline
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime

import config
import utils
from scraper import JiraScraper
from transformer import DataTransformer


def main():
    """
    Main execution function
    """
    # Setup logging
    logger = utils.setup_logging()
    logger.info("=" * 80)
    logger.info("Apache Jira Scraper and Transformer Pipeline")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    
    all_project_files = []
    all_statistics = []
    
    try:
        # Process each project
        for project in config.PROJECTS:
            logger.info(f"\nProcessing project: {project}")
            logger.info("-" * 80)
            
            try:
                # Step 1: Scrape data
                logger.info(f"[{project}] Starting scraper...")
                scraper = JiraScraper(project)
                issues = scraper.scrape_all_issues()
                
                if not issues:
                    logger.warning(f"[{project}] No issues scraped. Skipping...")
                    continue
                
                # Save raw data
                raw_file = scraper.save_raw_data(issues)
                logger.info(f"[{project}] Raw data saved to: {raw_file}")
                
                # Step 2: Transform data
                logger.info(f"[{project}] Starting transformation...")
                transformer = DataTransformer(project)
                training_examples = transformer.transform_all_issues(issues)
                
                if not training_examples:
                    logger.warning(f"[{project}] No training examples created. Skipping...")
                    continue
                
                # Save transformed data
                output_file = config.PROCESSED_DATA_DIR / config.OUTPUT_FILE_PATTERN.format(project=project)
                transformer.save_to_jsonl(training_examples, output_file)
                all_project_files.append(output_file)
                
                # Generate statistics
                stats = transformer.generate_statistics(training_examples)
                stats["project"] = project
                stats["raw_issues_count"] = len(issues)
                all_statistics.append(stats)
                
                # Save project statistics
                stats_file = config.PROCESSED_DATA_DIR / f"{project}_statistics.json"
                with open(stats_file, 'w', encoding='utf-8') as f:
                    json.dump(stats, f, indent=2, ensure_ascii=False)
                
                logger.info(f"[{project}] Statistics saved to: {stats_file}")
                logger.info(f"[{project}] Completed successfully!")
                logger.info(f"[{project}] Issues scraped: {len(issues)}")
                logger.info(f"[{project}] Training examples created: {len(training_examples)}")
                
            except Exception as e:
                logger.error(f"[{project}] Error processing project: {str(e)}", exc_info=True)
                continue
        
        # Step 3: Merge all project files
        if all_project_files:
            logger.info("\n" + "=" * 80)
            logger.info("Merging all project files...")
            
            final_output = config.PROCESSED_DATA_DIR / config.FINAL_OUTPUT_FILE
            total_lines = utils.merge_jsonl_files(all_project_files, final_output)
            
            logger.info(f"Final corpus saved to: {final_output}")
            logger.info(f"Total training examples: {total_lines}")
            logger.info(f"File size: {utils.get_file_size_mb(final_output):.2f} MB")
            
            # Save combined statistics
            combined_stats = {
                "total_examples": sum(s["total_examples"] for s in all_statistics),
                "total_issues": sum(s["raw_issues_count"] for s in all_statistics),
                "projects_processed": len(all_statistics),
                "projects": [s["project"] for s in all_statistics],
                "per_project_stats": all_statistics,
                "processing_time_seconds": (datetime.now() - start_time).total_seconds()
            }
            
            combined_stats_file = config.PROCESSED_DATA_DIR / "combined_statistics.json"
            with open(combined_stats_file, 'w', encoding='utf-8') as f:
                json.dump(combined_stats, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Combined statistics saved to: {combined_stats_file}")
            
            # Print summary
            logger.info("\n" + "=" * 80)
            logger.info("PIPELINE SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Projects processed: {combined_stats['projects_processed']}")
            logger.info(f"Total issues scraped: {combined_stats['total_issues']}")
            logger.info(f"Total training examples: {combined_stats['total_examples']}")
            logger.info(f"Output file: {final_output}")
            logger.info(f"Total time: {combined_stats['processing_time_seconds']:.2f} seconds")
            logger.info("=" * 80)
            
        else:
            logger.warning("No data was successfully processed!")
            return 1
        
    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user. Progress has been saved.")
        return 1
    except Exception as e:
        logger.error(f"Fatal error in pipeline: {str(e)}", exc_info=True)
        return 1
    
    logger.info("\nPipeline completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())