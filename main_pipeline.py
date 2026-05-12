"""
Main pipeline for B2B Hiring Intent Dashboard
Integrates scraping, data processing, and database operations
"""

"""
B2B Hiring Intent Pipeline - Main Orchestrator

This module coordinates the entire data pipeline:
1. LinkedIn job scraping
2. Data cleaning and standardization  
3. Database storage with deduplication
4. Dashboard data preparation

Created: May 2026
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

from scraper import LinkedInJobScraper
from data_processor import DataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

class HiringIntentPipeline:
    def __init__(self):
        self.scraper = LinkedInJobScraper()
        self.processor = DataProcessor()
        
        # Configuration
        self.max_pages = int(os.getenv('MAX_PAGES', '5'))
        self.keywords = os.getenv('SEARCH_KEYWORDS', 'software engineer')
        self.location = os.getenv('SEARCH_LOCATION', 'India')
        
    def run_pipeline(self):
        """Run the complete pipeline"""
        logger.info("Starting B2B Hiring Intent Pipeline")
        start_time = datetime.now()
        
        try:
            # Step 1: Scrape jobs
            logger.info("Step 1: Scraping LinkedIn jobs")
            raw_jobs = self.scraper.scrape_jobs(
                keywords=self.keywords,
                location=self.location,
                max_pages=self.max_pages
            )
            
            if not raw_jobs:
                logger.warning("No jobs scraped. Pipeline ending.")
                return False
            
            logger.info(f"Scraped {len(raw_jobs)} raw job listings")
            
            # Step 2: Convert to DataFrame and clean data
            logger.info("Step 2: Cleaning and processing data")
            df = pd.DataFrame(raw_jobs)
            df_clean = self.processor.clean_job_data(df)
            
            if df_clean.empty:
                logger.warning("No valid jobs after cleaning. Pipeline ending.")
                return False
            
            logger.info(f"Cleaned data: {len(df_clean)} valid jobs")
            
            # Step 3: Skills extraction from job cards (already done in scraper)
            logger.info("Step 3: Skills extracted during scraping phase")
            
            # Step 4: Database operations
            logger.info("Step 4: Storing data in database")
            
            # Create schema if needed
            self.processor.create_database_schema()
            
            # Upsert jobs to database
            self.processor.upsert_jobs(df_clean)
            
            # Step 5: Generate statistics
            stats = self.processor.get_job_statistics()
            logger.info(f"Pipeline completed successfully. Database stats: {stats}")
            
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"Total pipeline duration: {duration}")
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return False
    
    def get_dashboard_data(self, days=30, limit=500):
        """Get data for dashboard display"""
        try:
            df = self.processor.get_recent_jobs(days=days, limit=limit)
            return df
        except Exception as e:
            logger.error(f"Error getting dashboard data: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    # Run the pipeline
    pipeline = HiringIntentPipeline()
    success = pipeline.run_pipeline()
    
    if success:
        print("✅ Pipeline completed successfully!")
        
        # Show some sample data
        sample_data = pipeline.get_dashboard_data(days=7, limit=10)
        if not sample_data.empty:
            print("\n📊 Sample recent jobs:")
            print(sample_data[['title', 'company', 'location', 'post_date']].to_string())
    else:
        print("❌ Pipeline failed. Check logs for details.")
        sys.exit(1)
