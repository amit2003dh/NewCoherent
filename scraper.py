"""
LinkedIn Job Scraper for B2B Hiring Intent Dashboard
Scrapes public LinkedIn job postings without authentication
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class LinkedInJobScraper:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session.headers.update(self.headers)
        
    def get_user_agent_rotation(self):
        """Rotate user agents to avoid blocking"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]
        return random.choice(user_agents)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_page(self, url):
        """Fetch a page with retry logic"""
        try:
            # Rotate user agent for each request
            self.session.headers['User-Agent'] = self.get_user_agent_rotation()
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            raise
    
    def parse_job_card(self, job_card):
        """Parse individual job card from LinkedIn"""
        try:
            job_data = {}
            
            # Job title - try multiple selectors
            title_selectors = [
                'h3.base-search-card__title',
                'h3.job-card__title',
                '.base-card__title',
                'h3'
            ]
            title_elem = None
            for selector in title_selectors:
                title_elem = job_card.select_one(selector) if '.' in selector else job_card.find(selector.split('.')[-1])
                if title_elem:
                    break
            job_data['title'] = title_elem.get_text(strip=True) if title_elem else "Not Specified"
            
            # Company name - try multiple selectors
            company_selectors = [
                'h4.base-search-card__subtitle',
                '.job-card__company-name',
                '.base-card__subtitle',
                'h4'
            ]
            company_elem = None
            for selector in company_selectors:
                company_elem = job_card.select_one(selector) if '.' in selector else job_card.find(selector.split('.')[-1])
                if company_elem:
                    break
            job_data['company'] = company_elem.get_text(strip=True) if company_elem else "Not Specified"
            
            # Location - try multiple selectors
            location_selectors = [
                'span.job-result-card__location',
                '.job-card__location',
                '.base-search-card__location',
                '.job-location'
            ]
            location_elem = None
            for selector in location_selectors:
                location_elem = job_card.select_one(selector) if '.' in selector else job_card.find(selector.split('.')[-1])
                if location_elem:
                    break
            job_data['location'] = location_elem.get_text(strip=True) if location_elem else "Remote/Not Specified"
            
            # Extract skills from multiple sources
            job_data['skills_required'] = self.extract_skills_from_job_card(job_card)
            
            # Posted date - try multiple selectors
            time_selectors = [
                'time.job-search-card__listdate',
                '.job-card__listdate',
                '.time-ago',
                'time'
            ]
            time_elem = None
            for selector in time_selectors:
                time_elem = job_card.select_one(selector) if '.' in selector else job_card.find(selector.split('.')[-1])
                if time_elem:
                    break
            if time_elem:
                date_text = time_elem.get_text(strip=True)
                job_data['posted_date'] = self.standardize_date(date_text)
            else:
                job_data['posted_date'] = datetime.now().strftime('%Y-%m-%d')
            
            # Job link - try multiple selectors
            link_selectors = [
                'a.base-card__full-link',
                '.job-card__link',
                '.base-search-card__full-link',
                'a[href*="/jobs/view/"]'
            ]
            link_elem = None
            for selector in link_selectors:
                if 'href' in selector:
                    link_elem = job_card.find('a', href=True)
                else:
                    link_elem = job_card.select_one(selector) if '.' in selector else job_card.find(selector.split('.')[-1])
                if link_elem:
                    break
            if link_elem and link_elem.get('href'):
                job_data['job_link'] = self.clean_url(link_elem['href'])
                # Extract job ID from URL
                job_id_match = re.search(r'/view/(\d+)', job_data['job_link'])
                job_data['job_id'] = job_id_match.group(1) if job_id_match else str(hash(job_data['job_link']))
            else:
                job_data['job_link'] = ""
                job_data['job_id'] = str(hash(job_data['title'] + job_data['company']))
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error parsing job card: {e}")
            return None
    
    def standardize_date(self, date_text):
        """Convert LinkedIn date format to YYYY-MM-DD"""
        try:
            now = datetime.now()
            
            if 'hour' in date_text.lower():
                hours = int(re.search(r'(\d+)', date_text).group(1))
                return (now - timedelta(hours=hours)).strftime('%Y-%m-%d')
            elif 'day' in date_text.lower():
                days = int(re.search(r'(\d+)', date_text).group(1))
                return (now - timedelta(days=days)).strftime('%Y-%m-%d')
            elif 'week' in date_text.lower():
                weeks = int(re.search(r'(\d+)', date_text).group(1))
                return (now - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
            elif 'month' in date_text.lower():
                months = int(re.search(r'(\d+)', date_text).group(1))
                return (now - timedelta(days=months*30)).strftime('%Y-%m-%d')
            else:
                return now.strftime('%Y-%m-%d')
        except:
            return datetime.now().strftime('%Y-%m-%d')
    
    def clean_url(self, url):
        """Remove tracking parameters from URL"""
        if '?' in url:
            return url.split('?')[0]
        return url
    
    def extract_basic_skills(self, title):
        """Extract basic skills from job title"""
        if not title:
            return ""
        
        title_lower = title.lower()
        skills = []
        
        # Common tech skills to look for
        skill_keywords = {
            'python': ['python', 'django', 'flask'],
            'java': ['java', 'spring', 'hibernate'],
            'javascript': ['javascript', 'js', 'react', 'vue', 'angular', 'node', 'nodejs'],
            'react': ['react', 'reactjs', 'react.js'],
            'aws': ['aws', 'amazon web services', 'ec2', 's3'],
            'docker': ['docker', 'kubernetes', 'k8s'],
            'sql': ['sql', 'mysql', 'postgresql', 'oracle'],
            'git': ['git', 'github', 'gitlab'],
            'api': ['api', 'rest', 'graphql'],
            'cloud': ['cloud', 'aws', 'azure', 'gcp'],
            'devops': ['devops', 'ci/cd', 'jenkins'],
            'testing': ['testing', 'test', 'qa'],
            'mobile': ['mobile', 'ios', 'android', 'swift', 'kotlin'],
            'frontend': ['frontend', 'frontend', 'ui', 'ux'],
            'backend': ['backend', 'backend', 'server'],
            'full stack': ['full stack', 'fullstack', 'full-stack'],
            'machine learning': ['machine learning', 'ml', 'ai', 'artificial intelligence'],
            'data science': ['data science', 'analytics', 'big data']
        }
        
        for skill, keywords in skill_keywords.items():
            if any(keyword in title_lower for keyword in keywords):
                skills.append(skill.title())
        
        return ', '.join(skills) if skills else "General"
    
    def extract_skills_from_job_card(self, job_card):
        """Extract skills from job card using multiple selectors"""
        skills = []
        
        # Look for skill lists in job cards
        skill_selectors = [
            '.job-card__skills-list',
            '.skills-list',
            '[data-test-id="job-card-skills"]',
            '.job-description-skill-list',
            '.base-search-card__subtitle-secondary',  # Sometimes skills are in subtitle
            '.job-card__subtitle-secondary'
        ]
        
        for selector in skill_selectors:
            skill_elem = job_card.select_one(selector)
            if skill_elem:
                skill_text = skill_elem.get_text(strip=True)
                if skill_text and len(skill_text) > 5:  # Avoid short generic text
                    skills = [skill.strip() for skill in skill_text.split(',') if skill.strip()]
                    return ', '.join(skills)
        
        # Look for individual skill tags
        skill_tag_selectors = [
            '.skill-tag',
            '.job-skill-tag',
            '[data-test-id="skill-tag"]',
            '.base-search-card__skill-tag'
        ]
        
        for selector in skill_tag_selectors:
            skill_tags = job_card.select(selector)
            if skill_tags and len(skill_tags) > 0:
                skills = [tag.get_text(strip=True) for tag in skill_tags if tag.get_text(strip=True)]
                return ', '.join(skills)
        
        # Look for skills in any text element that might contain skills
        all_text_elements = job_card.find_all(['span', 'div', 'p'])
        for elem in all_text_elements:
            text = elem.get_text(strip=True)
            if text and any(keyword in text.lower() for keyword in ['python', 'java', 'javascript', 'react', 'aws', 'docker', 'sql', 'git', 'api', 'cloud', 'devops', 'testing', 'mobile', 'frontend', 'backend', 'full stack', 'machine learning', 'data science']):
                # Extract skills from this text
                found_skills = []
                for skill in ['Python', 'Java', 'JavaScript', 'React', 'AWS', 'Docker', 'SQL', 'Git', 'API', 'Cloud', 'DevOps', 'Testing', 'Mobile', 'Frontend', 'Backend', 'Full Stack', 'Machine Learning', 'Data Science']:
                    if skill.lower() in text.lower():
                        found_skills.append(skill)
                if found_skills:
                    return ', '.join(found_skills)
        
        # Enhanced skills extraction using title and company analysis
        title_elem = None
        title_selectors = [
            'h3.base-search-card__title',
            'h3.job-card__title',
            '.base-card__title',
            'h3'
        ]
        for selector in title_selectors:
            title_elem = job_card.select_one(selector) if '.' in selector else job_card.find(selector.split('.')[-1])
            if title_elem:
                break
        
        company_elem = None
        company_selectors = [
            'h4.base-search-card__subtitle',
            '.job-card__company-name',
            '.base-card__subtitle',
            'h4'
        ]
        for selector in company_selectors:
            company_elem = job_card.select_one(selector) if '.' in selector else job_card.find(selector.split('.')[-1])
            if company_elem:
                break
        
        if title_elem and company_elem:
            title_text = title_elem.get_text(strip=True)
            company_text = company_elem.get_text(strip=True)
            
            # Enhanced skills extraction based on title and company context
            skills = self.extract_enhanced_skills(title_text, company_text)
            if skills:
                return skills
        
        # Fallback to basic title-based extraction
        if title_elem:
            return self.extract_basic_skills(title_elem.get_text(strip=True))
        
        return "General"
    
    def extract_enhanced_skills(self, title, company):
        """Simple and reliable skills extraction"""
        title_lower = title.lower()
        skills = []
        
        # Direct skill matching from title
        direct_skills = {
            'python': 'python',
            'java': 'java',
            'javascript': 'javascript',
            'react': 'react',
            'aws': 'aws',
            'docker': 'docker',
            'sql': 'sql',
            'git': 'git',
            'api': 'api',
            'cloud': 'cloud',
            'devops': 'devops',
            'testing': 'testing',
            'mobile': 'mobile',
            'frontend': 'frontend',
            'backend': 'backend',
            'full stack': 'full stack',
            'fullstack': 'fullstack',
            'machine learning': 'machine learning',
            'ml': 'ml',
            'ai': 'ai',
            'data science': 'data science',
            'node': 'node',
            'angular': 'angular',
            'vue': 'vue',
            'typescript': 'typescript',
            'kubernetes': 'kubernetes',
            'k8s': 'k8s',
            'azure': 'azure',
            'gcp': 'gcp',
            'microservices': 'microservices',
            'saas': 'saas',
            'blockchain': 'blockchain',
            'web3': 'web3'
        }
        
        # Extract skills from title
        for skill_name, keyword in direct_skills.items():
            if keyword in title_lower:
                skills.append(skill_name.title())
        
        # Remove duplicates and return
        unique_skills = list(set(skills))
        # Ensure we return a proper string, not individual characters
        if unique_skills:
            return ', '.join(unique_skills)
        else:
            return "General"
    
    def scrape_jobs(self, keywords="software engineer", location="India", max_pages=5):
        """Main scraping function"""
        logger.info(f"Starting to scrape jobs: {keywords} in {location}")
        
        all_jobs = []
        
        for page in range(max_pages):
            start = page * 25  # LinkedIn shows 25 jobs per page
            url = f"https://www.linkedin.com/jobs/search?keywords={keywords.replace(' ', '%20')}&location={location.replace(' ', '%20')}&start={start}"
            
            try:
                logger.info(f"Scraping page {page + 1}: {url}")
                response = self.fetch_page(url)
                soup = BeautifulSoup(response.content, 'lxml')
                
                # Find job cards
                job_cards = soup.find_all('div', class_='base-search-card')
                
                if not job_cards:
                    logger.warning(f"No job cards found on page {page + 1}")
                    break
                
                page_jobs = []
                for card in job_cards:
                    job_data = self.parse_job_card(card)
                    if job_data:
                        page_jobs.append(job_data)
                
                all_jobs.extend(page_jobs)
                logger.info(f"Found {len(page_jobs)} jobs on page {page + 1}")
                
                # Random delay between requests
                delay = random.uniform(2, 5)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error scraping page {page + 1}: {e}")
                continue
        
        logger.info(f"Total jobs scraped: {len(all_jobs)}")
        return all_jobs
    
    def save_to_csv(self, jobs, filename="raw_jobs.csv"):
        """Save scraped jobs to CSV"""
        if not jobs:
            logger.warning("No jobs to save")
            return
        
        df = pd.DataFrame(jobs)
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(jobs)} jobs to {filename}")

if __name__ == "__main__":
    scraper = LinkedInJobScraper()
    jobs = scraper.scrape_jobs(keywords="software engineer", location="India", max_pages=3)
    scraper.save_to_csv(jobs)
