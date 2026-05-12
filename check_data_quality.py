from data_processor import DataProcessor
import pandas as pd

processor = DataProcessor()
df = processor.get_recent_jobs(days=1, limit=10)
if not df.empty:
    print('Current data extraction results:')
    for i, row in df.iterrows():
        title = row['title'][:50] if len(row['title']) > 50 else row['title']
        company = row['company']
        role = row['role_category']
        skills = row['skills_required']
        print(f'{i+1}. Title: {title}...')
        print(f'   Company: {company}')
        print(f'   Role: {role}')
        print(f'   Skills: {skills}')
        print('---')
else:
    print('No recent data found')
