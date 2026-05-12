"""
B2B Hiring Intent Dashboard - Streamlit Interface
A dynamic dashboard for businesses to track hiring signals and generate leads
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import re

# Configure page
st.set_page_config(
    page_title="B2B Hiring Intent Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

load_dotenv()

class Dashboard:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            st.error("DATABASE_URL environment variable not set")
            st.stop()
        
        self.engine = create_engine(self.database_url)
        
    def load_data(self, days=30, limit=1000):
        """Load job data from database"""
        try:
            query = text("""
                SELECT * FROM job_leads 
                WHERE post_date >= CURRENT_DATE - CAST(:days AS INTEGER) * INTERVAL '1 day'
                ORDER BY post_date DESC
                LIMIT :limit
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {"days": days, "limit": limit})
                df = pd.DataFrame(result.fetchall())
                
                if not df.empty:
                    df.columns = result.keys()
                    # Convert post_date to datetime
                    df['post_date'] = pd.to_datetime(df['post_date'])
                
                return df
                
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return pd.DataFrame()
    
    def get_statistics(self):
        """Get database statistics"""
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
                
                return dict(stats._mapping) if stats else {}
                
        except Exception as e:
            st.error(f"Error getting statistics: {e}")
            return {}
    
    def load_new_data(self):
        """Load new data by running the pipeline"""
        try:
            with st.spinner("🔄 Scraping new job data... This may take a few minutes."):
                # Import and run the pipeline
                from main_pipeline import HiringIntentPipeline
                pipeline = HiringIntentPipeline()
                success = pipeline.run_pipeline()
                
                if success:
                    st.success("✅ New data loaded successfully!")
                    st.balloons()  # Add celebration effect
                    time.sleep(2)  # Brief pause to show success
                    st.rerun()  # Refresh the dashboard
                else:
                    st.error("❌ Pipeline completed but may have issues. Check the output above for details.")
                    
        except Exception as e:
            st.error(f"❌ Error loading new data: {e}")
            st.info("💡 Tip: This might be due to API limits or network issues. Try again in a few minutes.")
    
    def get_last_updated(self):
        """Get the last updated timestamp from database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(updated_at) as last_updated 
                    FROM job_leads
                """)).fetchone()
                
                if result and result[0]:
                    return result[0].strftime("%Y-%m-%d %H:%M")
                else:
                    return "Never"
                    
        except Exception as e:
            return "Unknown"
    
    def filter_data(self, df, filters):
        """Apply filters to the data"""
        filtered_df = df.copy()
        
        # Date range filter
        if filters.get('date_range'):
            start_date, end_date = filters['date_range']
            # Convert date objects to datetime for comparison
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)
            filtered_df = filtered_df[
                (filtered_df['post_date'] >= start_date) & 
                (filtered_df['post_date'] <= end_date)
            ]
        
        # Location filter
        if filters.get('locations'):
            filtered_df = filtered_df[filtered_df['location'].isin(filters['locations'])]
        
        # Role category filter
        if filters.get('role_categories'):
            filtered_df = filtered_df[filtered_df['role_category'].isin(filters['role_categories'])]
        
        # Company filter
        if filters.get('companies'):
            filtered_df = filtered_df[filtered_df['company'].isin(filters['companies'])]
        
        # Skills filter
        if filters.get('skills'):
            def has_skill(skills_str):
                if pd.isna(skills_str) or skills_str == 'Not specified':
                    return False
                return any(skill.lower() in str(skills_str).lower() for skill in filters['skills'])
            
            filtered_df = filtered_df[filtered_df['skills_required'].apply(has_skill)]
        
        # Search term filter
        if filters.get('search_term'):
            search_term = filters['search_term'].lower()
            filtered_df = filtered_df[
                filtered_df['title'].str.lower().str.contains(search_term, na=False) |
                filtered_df['company'].str.lower().str.contains(search_term, na=False) |
                filtered_df['skills_required'].str.lower().str.contains(search_term, na=False)
            ]
        
        return filtered_df
    
    def create_charts(self, df):
        """Create various charts for the dashboard"""
        if df.empty:
            return {}
        
        charts = {}
        
        # Jobs posted over time
        daily_jobs = df.groupby(df['post_date'].dt.date).size().reset_index(name='count')
        charts['timeline'] = px.line(
            daily_jobs, 
            x='post_date', 
            y='count',
            title='Jobs Posted Over Time',
            labels={'post_date': 'Date', 'count': 'Number of Jobs'}
        )
        
        # Top companies hiring
        top_companies = df['company'].value_counts().head(10)
        charts['companies'] = px.bar(
            x=top_companies.values,
            y=top_companies.index,
            orientation='h',
            title='Top 10 Companies Hiring',
            labels={'x': 'Number of Jobs', 'y': 'Company'}
        )
        
        # Jobs by location
        location_counts = df['location'].value_counts().head(10)
        charts['locations'] = px.pie(
            values=location_counts.values,
            names=location_counts.index,
            title='Jobs by Location (Top 10)'
        )
        
        # Jobs by role category
        role_counts = df['role_category'].value_counts()
        charts['roles'] = px.bar(
            x=role_counts.index,
            y=role_counts.values,
            title='Jobs by Role Category',
            labels={'x': 'Role Category', 'y': 'Number of Jobs'}
        )
        
        return charts
    
    def render_sidebar(self, df):
        """Render the sidebar with filters"""
        st.sidebar.header("🔍 Filters")
        
        filters = {}
        
        # Date range filter
        if not df.empty:
            min_date = df['post_date'].min().date()
            max_date = df['post_date'].max().date()
            
            filters['date_range'] = st.sidebar.date_input(
                "Date Range",
                value=[min_date, max_date],
                min_value=min_date,
                max_value=max_date
            )
        
        # Search term
        filters['search_term'] = st.sidebar.text_input("🔎 Search Jobs", placeholder="Search titles, companies, skills...")
        
        # Location filter
        if 'location' in df.columns:
            locations = sorted(df['location'].dropna().unique())
            if locations:
                filters['locations'] = st.sidebar.multiselect(
                    "📍 Locations",
                    options=locations,
                    default=[]
                )
        
        # Role category filter
        if 'role_category' in df.columns:
            roles = sorted(df['role_category'].dropna().unique())
            if roles:
                filters['role_categories'] = st.sidebar.multiselect(
                    "💼 Role Categories",
                    options=roles,
                    default=[]
                )
        
        # Company filter
        if 'company' in df.columns:
            companies = sorted(df['company'].dropna().unique())[:50]  # Limit to 50 for performance
            if companies:
                filters['companies'] = st.sidebar.multiselect(
                    "🏢 Companies",
                    options=companies,
                    default=[]
                )
        
        # Skills filter
        if 'skills_required' in df.columns:
            # Extract unique skills
            all_skills = set()
            for skills in df['skills_required'].dropna():
                if skills and skills != 'Not specified':
                    all_skills.update([skill.strip() for skill in str(skills).split(',')])
            
            if all_skills:
                skill_list = sorted(list(all_skills))[:50]  # Limit to 50 for performance
                filters['skills'] = st.sidebar.multiselect(
                    "⚡ Skills",
                    options=skill_list,
                    default=[]
                )
        
        return filters
    
    def render_kpi_cards(self, stats, filtered_df):
        """Render KPI cards"""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="📊 Total Jobs",
                value=len(filtered_df),
                delta=f"From {stats.get('total_jobs', 0)} total"
            )
        
        with col2:
            st.metric(
                label="🏢 Active Companies",
                value=filtered_df['company'].nunique() if not filtered_df.empty else 0,
                delta=f"From {stats.get('unique_companies', 0)} total"
            )
        
        with col3:
            st.metric(
                label="📍 Locations",
                value=filtered_df['location'].nunique() if not filtered_df.empty else 0,
                delta=f"From {stats.get('unique_locations', 0)} total"
            )
        
        with col4:
            # Calculate recent jobs (last 7 days)
            recent_date = datetime.now().date() - timedelta(days=7)
            recent_jobs = filtered_df[filtered_df['post_date'].dt.date >= recent_date] if not filtered_df.empty else pd.DataFrame()
            st.metric(
                label="🆕 Recent Jobs (7 days)",
                value=len(recent_jobs),
                delta="Last week"
            )
    
    def render_data_table(self, df):
        """Render the main data table"""
        if df.empty:
            st.info("No jobs found matching your criteria.")
            return
        
        # Prepare display columns
        display_columns = ['title', 'company', 'location', 'role_category', 'post_date', 'skills_required']
        available_columns = [col for col in display_columns if col in df.columns]
        
        display_df = df[available_columns].copy()
        
        # Format date column
        if 'post_date' in display_df.columns:
            display_df['post_date'] = display_df['post_date'].dt.strftime('%Y-%m-%d')
        
        # Format skills column
        if 'skills_required' in display_df.columns:
            display_df['skills_required'] = display_df['skills_required'].apply(
                lambda x: x[:50] + "..." if pd.notna(x) and len(str(x)) > 50 else x
            )
        
        # Rename columns for display
        column_renames = {
            'title': 'Job Title',
            'company': 'Company',
            'location': 'Location',
            'role_category': 'Role',
            'post_date': 'Posted Date',
            'skills_required': 'Skills'
        }
        display_df = display_df.rename(columns=column_renames)
        
        # Add apply link if available
        if 'apply_link' in df.columns:
            display_df['Apply'] = df['apply_link'].apply(
                lambda x: f"[Apply]({x})" if pd.notna(x) and x else ""
            )
        
        # Display the table
        st.dataframe(
            display_df,
            width='stretch',
            hide_index=True
        )
    
    def run(self):
        """Main dashboard function"""
        st.title("🎯 B2B Hiring Intent Dashboard")
        st.markdown("---")
        
        # Add refresh button
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🔄 Load New Data", type="primary"):
                self.load_new_data()
        with col2:
            last_updated = self.get_last_updated()
            st.info(f"Last updated: {last_updated}")
        
        # Load data
        with st.spinner("Loading data..."):
            df = self.load_data(days=30)
            stats = self.get_statistics()
        
        if df.empty:
            st.error("No data available. Please run the scraper first.")
            st.info("To populate data, run: `python main_pipeline.py`")
            return
        
        # Render sidebar filters
        filters = self.render_sidebar(df)
        
        # Apply filters
        filtered_df = self.filter_data(df, filters)
        
        # Render KPI cards
        self.render_kpi_cards(stats, filtered_df)
        st.markdown("---")
        
        # Render charts
        charts = self.create_charts(filtered_df)
        
        if charts:
            col1, col2 = st.columns(2)
            
            with col1:
                if 'timeline' in charts:
                    st.plotly_chart(charts['timeline'], width='stretch')
                if 'companies' in charts:
                    st.plotly_chart(charts['companies'], width='stretch')
            
            with col2:
                if 'locations' in charts:
                    st.plotly_chart(charts['locations'], width='stretch')
                if 'roles' in charts:
                    st.plotly_chart(charts['roles'], width='stretch')
        
        st.markdown("---")
        
        # Render data table
        st.subheader("📋 Job Listings")
        self.render_data_table(filtered_df)
        
        # Export functionality
        if not filtered_df.empty:
            st.markdown("---")
            col1, col2 = st.columns(2)
            
            with col1:
                csv_data = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name=f"hiring_leads_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            
            with col2:
                st.info(f"Showing {len(filtered_df)} of {len(df)} total jobs")

if __name__ == "__main__":
    dashboard = Dashboard()
    dashboard.run()
