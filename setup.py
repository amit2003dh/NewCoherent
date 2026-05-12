"""
Setup script for B2B Hiring Intent Dashboard
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="b2b-hiring-intent-dashboard",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Transform LinkedIn job postings into actionable B2B sales leads",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/b2b-hiring-intent-dashboard",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "b2b-scraper=main_pipeline:main",
        ],
    },
)
