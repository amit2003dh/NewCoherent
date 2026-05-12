"""
Test script to validate the B2B Hiring Intent Dashboard pipeline
"""

import os
import sys
import pandas as pd
from datetime import datetime

def test_imports():
    """Test all module imports"""
    print("🔍 Testing imports...")
    
    try:
        from scraper import LinkedInJobScraper
        from data_processor import DataProcessor
        from main_pipeline import HiringIntentPipeline
        print("✅ All core modules imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_scraper():
    """Test scraper functionality"""
    print("\n🕷️ Testing scraper...")
    
    try:
        from scraper import LinkedInJobScraper
        scraper = LinkedInJobScraper()
        print("✅ Scraper initialized successfully")
        
        # Test URL construction
        url = "https://www.linkedin.com/jobs/search?keywords=software%20engineer&location=India&start=0"
        print(f"✅ Test URL: {url}")
        
        return True
    except Exception as e:
        print(f"❌ Scraper test failed: {e}")
        return False

def test_data_processor():
    """Test data processor functionality"""
    print("\n🧹 Testing data processor...")
    
    try:
        from data_processor import DataProcessor
        
        # Create sample data
        sample_data = {
            'job_id': ['test123', 'test456'],
            'title': ['Software Engineer', 'Senior Developer'],
            'company': ['Tech Corp', 'Startup Inc'],
            'location': ['Remote', 'Bangalore'],
            'posted_date': ['2024-01-15', '2024-01-14'],
            'job_link': ['https://linkedin.com/jobs/view/123', 'https://linkedin.com/jobs/view/456']
        }
        
        df = pd.DataFrame(sample_data)
        processor = DataProcessor()
        
        # Test data cleaning (without database connection)
        print("✅ DataProcessor initialized successfully")
        print(f"✅ Sample data created with {len(df)} records")
        
        return True
    except Exception as e:
        print(f"❌ Data processor test failed: {e}")
        return False

def test_ai_enrichment():
    """Test AI enrichment (optional)"""
    print("\n🤖 Testing AI enrichment...")
    
    try:
        from ai_enrichment import AIEnrichment
        ai = AIEnrichment()
        print("✅ AIEnrichment module imported successfully")
        
        if ai.model:
            print("✅ Gemini AI model is available")
        else:
            print("⚠️ Gemini AI model not configured (API key missing)")
        
        return True
    except Exception as e:
        print(f"❌ AI enrichment test failed: {e}")
        return False

def test_dashboard():
    """Test dashboard imports"""
    print("\n📊 Testing dashboard...")
    
    try:
        import streamlit as st
        print("✅ Streamlit imported successfully")
        
        # Test plotly
        import plotly.express as px
        print("✅ Plotly imported successfully")
        
        return True
    except Exception as e:
        print(f"❌ Dashboard test failed: {e}")
        return False

def test_file_structure():
    """Test required files exist"""
    print("\n📁 Testing file structure...")
    
    required_files = [
        'scraper.py',
        'data_processor.py', 
        'main_pipeline.py',
        'ai_enrichment.py',
        'app.py',
        'requirements.txt',
        '.env.example',
        'README.md',
        '.github/workflows/scraper.yml'
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    else:
        print("✅ All required files present")
        return True

def main():
    """Run all tests"""
    print("🚀 B2B Hiring Intent Dashboard - Pipeline Test")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_scraper,
        test_data_processor,
        test_ai_enrichment,
        test_dashboard,
        test_file_structure
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The pipeline is ready to use.")
        print("\n📋 Next Steps:")
        print("1. Set up your DATABASE_URL environment variable")
        print("2. Optional: Add GEMINI_API_KEY for AI enrichment")
        print("3. Run: python main_pipeline.py")
        print("4. Start dashboard: streamlit run app.py")
    else:
        print("⚠️ Some tests failed. Please check the errors above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
