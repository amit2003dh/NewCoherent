"""
Data processing and database integration for B2B Hiring Intent Dashboard
Handles data cleaning, standardization, and PostgreSQL operations
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import re

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        self.engine = create_engine(self.database_url)
        
    def clean_job_data(self, df):
        """Clean and standardize job data"""
        logger.info("Starting data cleaning process")
        
        # Make a copy to avoid SettingWithCopyWarning
        df_clean = df.copy()
        
        # Standardize text columns
        text_columns = ['title', 'company', 'location']
        for col in text_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
                df_clean[col] = df_clean[col].replace('nan', np.nan)
                df_clean[col] = df_clean[col].replace('Not Specified', np.nan)
        
        # Clean company names - remove extra spaces and standardize
        if 'company' in df_clean.columns:
            df_clean['company'] = df_clean['company'].str.replace(r'\s+', ' ', regex=True)
            df_clean['company'] = df_clean['company'].str.title()
        
        # Standardize locations
        if 'location' in df_clean.columns:
            df_clean['location'] = df_clean['location'].str.replace(r'\s+', ' ', regex=True)
            df_clean['location'] = df_clean['location'].str.title()
            
            # Handle common location variations
            location_mapping = {
                'Remote': 'Remote',
                'Hybrid': 'Hybrid',
                'On-Site': 'On-site',
                'Onsite': 'On-site'
            }
            df_clean['location'] = df_clean['location'].replace(location_mapping)
        
        # Clean job titles - extract role categories
        if 'title' in df_clean.columns:
            df_clean['title'] = df_clean['title'].str.replace(r'\s+', ' ', regex=True)
            # Only apply categorization if we don't already have skills from scraper
            if 'skills_required' not in df_clean.columns or df_clean['skills_required'].isna().all():
                df_clean['role_category'] = df_clean['title'].apply(self.categorize_role)
            else:
                # Keep existing role category if skills were extracted properly
                df_clean['role_category'] = df_clean['title'].apply(self.categorize_role)
        
        # Validate and clean dates
        if 'posted_date' in df_clean.columns:
            df_clean['posted_date'] = pd.to_datetime(df_clean['posted_date'], errors='coerce')
            # Remove dates that are too old or in the future
            today = pd.Timestamp.now()
            df_clean = df_clean[
                (df_clean['posted_date'] >= today - pd.Timedelta(days=90)) &
                (df_clean['posted_date'] <= today)
            ]
        
        # Clean URLs
        if 'job_link' in df_clean.columns:
            df_clean['job_link'] = df_clean['job_link'].apply(self.clean_url)
        
        # Remove duplicates based on job_id or combination of fields
        if 'job_id' in df_clean.columns:
            df_clean = df_clean.drop_duplicates(subset=['job_id'], keep='first')
        else:
            # If no job_id, use combination of title, company, and location
            df_clean = df_clean.drop_duplicates(subset=['title', 'company', 'location'], keep='first')
        
        # Remove rows with missing critical data
        critical_columns = ['title', 'company']
        df_clean = df_clean.dropna(subset=critical_columns)
        
        logger.info(f"Data cleaning complete. {len(df_clean)} records remain from {len(df)} original")
        return df_clean
    
    def categorize_role(self, title):
        """Categorize job titles into standard roles"""
        title_lower = title.lower()
        
        role_categories = {
            'Frontend': ['frontend', 'front-end', 'ui', 'ux', 'react', 'vue', 'angular', 'javascript', 'html', 'css'],
            'Backend': ['backend', 'back-end', 'api', 'server', 'node', 'python', 'java', 'c#', 'php', 'ruby'],
            'Full Stack': ['full stack', 'full-stack', 'fullstack'],
            'DevOps': ['devops', 'dev-ops', 'infrastructure', 'aws', 'azure', 'gcp', 'docker', 'kubernetes'],
            'Data Science': ['data scientist', 'data science', 'machine learning', 'ml', 'ai', 'artificial intelligence'],
            'Data Engineering': ['data engineer', 'data engineering', 'etl', 'data pipeline', 'big data'],
            'Mobile': ['mobile', 'ios', 'android', 'react native', 'flutter', 'swift', 'kotlin'],
            'QA/Testing': ['qa', 'quality assurance', 'testing', 'test engineer', 'automation'],
            'Project Management': ['project manager', 'product manager', 'scrum master', 'agile'],
            'Security': ['security', 'cybersecurity', 'information security', 'penetration testing']
        }
        
        for category, keywords in role_categories.items():
            if any(keyword in title_lower for keyword in keywords):
                return category
        
        return 'Other'
    
    def clean_url(self, url):
        """Clean and validate URLs"""
        if pd.isna(url) or url == '':
            return ''
        
        # Remove tracking parameters
        if '?' in url:
            url = url.split('?')[0]
        
        # Ensure URL starts with http/https
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url
    
    def create_database_schema(self):
        """Create the database schema if it doesn't exist"""
        logger.info("Creating database schema")
        
        schema_sql = """
        CREATE TABLE IF NOT EXISTS job_leads (
            job_id VARCHAR PRIMARY KEY,
            title VARCHAR NOT NULL,
            company VARCHAR NOT NULL,
            location VARCHAR,
            post_date DATE,
            apply_link TEXT,
            skills_required TEXT,
            role_category VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_job_leads_company ON job_leads(company);
        CREATE INDEX IF NOT EXISTS idx_job_leads_location ON job_leads(location);
        CREATE INDEX IF NOT EXISTS idx_job_leads_post_date ON job_leads(post_date);
        CREATE INDEX IF NOT EXISTS idx_job_leads_role_category ON job_leads(role_category);
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(schema_sql))
                conn.commit()
            logger.info("Database schema created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating database schema: {e}")
            raise
    
    def upsert_jobs(self, df):
        """
        Upsert job data into PostgreSQL database with proper deduplication.
        
        This method uses PostgreSQL's ON CONFLICT clause to prevent duplicate job entries
        while updating existing records with fresh data from LinkedIn scraping.
        """
        logger.info(f"Upserting {len(df)} jobs to database")
        
        # Prepare data for database
        df_db = df.copy()
        
        # Map columns to database schema
        column_mapping = {
            'job_id': 'job_id',
            'title': 'title',
            'company': 'company',
            'location': 'location',
            'posted_date': 'post_date',
            'job_link': 'apply_link',
            'skills_required': 'skills_required',
            'role_category': 'role_category'
        }
        
        # Select and rename columns
        db_columns = {col: db_col for col, db_col in column_mapping.items() if col in df_db.columns}
        df_db = df_db[list(db_columns.keys())].rename(columns=db_columns)
        
        # Convert date to proper format
        if 'post_date' in df_db.columns:
            df_db['post_date'] = pd.to_datetime(df_db['post_date']).dt.date
        
        # Add missing columns with default values
        if 'skills_required' not in df_db.columns:
            df_db['skills_required'] = None
        if 'role_category' not in df_db.columns:
            df_db['role_category'] = 'Other'
        
        try:
            # Proper upsert logic using ON CONFLICT to prevent duplicates
            with self.engine.connect() as conn:
                for _, row in df_db.iterrows():
                    upsert_sql = text("""
                        INSERT INTO job_leads (job_id, title, company, location, post_date, apply_link, skills_required, role_category)
                        VALUES (:job_id, :title, :company, :location, :post_date, :apply_link, :skills_required, :role_category)
                        ON CONFLICT (job_id) 
                        DO UPDATE SET 
                            title = EXCLUDED.title,
                            company = EXCLUDED.company,
                            location = EXCLUDED.location,
                            post_date = EXCLUDED.post_date,
                            apply_link = EXCLUDED.apply_link,
                            skills_required = EXCLUDED.skills_required,
                            role_category = EXCLUDED.role_category,
                            updated_at = CURRENT_TIMESTAMP
                    """)
                    
                    conn.execute(upsert_sql, {
                        'job_id': row.get('job_id'),
                        'title': row.get('title'),
                        'company': row.get('company'),
                        'location': row.get('location'),
                        'post_date': row.get('post_date'),
                        'apply_link': row.get('apply_link'),
                        'skills_required': row.get('skills_required'),
                        'role_category': row.get('role_category')
                    })
                
                conn.commit()
            
            logger.info(f"Successfully upserted {len(df_db)} jobs")
            
        except SQLAlchemyError as e:
            logger.error(f"Error upserting jobs: {e}")
            raise
    
    def get_job_statistics(self):
        """Get statistics about jobs in the database"""
        try:
            with self.engine.connect() as conn:
                stats = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_jobs,
                        COUNT(DISTINCT company) as unique_companies,
                        COUNT(DISTINCT location) as unique_locations,
                        COUNT(DISTINCT role_category) as unique_roles,
                        MAX(post_date) as latest_post_date,
                        MIN(post_date) as earliest_post_date
                    FROM job_leads
                """)).fetchone()
                
                return dict(stats._mapping)
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
    
    def get_recent_jobs(self, days=7, limit=100):
        """Get recent jobs from the database"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT * FROM job_leads 
                    WHERE post_date >= CURRENT_DATE - CAST(:days AS INTEGER) * INTERVAL '1 day'
                    ORDER BY post_date DESC
                    LIMIT :limit
                """)
                
                result = conn.execute(query, {"days": days, "limit": limit})
                df = pd.DataFrame(result.fetchall())
                
                if not df.empty:
                    df.columns = result.keys()
                
                return df
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting recent jobs: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    # Test the data processor
    processor = DataProcessor()
    
    # Create schema
    processor.create_database_schema()
    
    # Test with sample data
    sample_data = {
        'job_id': ['123', '456', '789'],
        'title': ['Senior React Developer', 'Backend Python Engineer', 'Full Stack Developer'],
        'company': ['Tech Corp', 'Startup Inc', 'Enterprise Ltd'],
        'location': ['Remote', 'Bangalore', 'Hybrid'],
        'posted_date': ['2024-01-15', '2024-01-14', '2024-01-13'],
        'job_link': ['https://linkedin.com/jobs/view/123', 'https://linkedin.com/jobs/view/456', 'https://linkedin.com/jobs/view/789']
    }
    
    df = pd.DataFrame(sample_data)
    df_clean = processor.clean_job_data(df)
    processor.upsert_jobs(df_clean)
    
    # Print statistics
    stats = processor.get_job_statistics()
    print("Database Statistics:", stats)
