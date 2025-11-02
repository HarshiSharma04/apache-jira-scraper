# 🧠 Apache Jira Scraper: LLM Training Data Pipeline

A **production-grade data scraping and transformation pipeline** that extracts public issue data from **Apache’s Jira** instance and converts it into a **structured JSONL corpus** suitable for fine-tuning **Large Language Models (LLMs)**.

---

## 📘 Overview

This project fulfills the **Web Scraping Tutor Assignment**, where the goal is to build a **robust, fault-tolerant, and scalable data extraction system** for Apache Jira.  
It demonstrates end-to-end capabilities — from **scraping**, **error handling**, and **checkpoint recovery** to **transforming real-world text into machine-learning-ready datasets**.

### ✅ Projects Scraped
- **Apache Kafka**
- **Apache Spark**
- **Apache Hadoop**

---

## 🎯 Objectives

- Scrape issue data (metadata, comments, and descriptions) from public Apache Jira projects.  
- Handle rate limits, pagination, and network errors gracefully.  
- Convert unstructured data into **high-quality JSONL** suitable for LLM tasks such as summarization, classification, and Q&A.  
- Ensure the system is **resumable**, **fault-tolerant**, and **efficient**.

---

## ⚙️ Features Implemented

### 🧩 Data Scraping
- Uses **Apache Jira REST API v2**
- Fetches **issues, comments, and metadata**
- Handles **pagination** (50 results/page)
- Gracefully manages:
  - HTTP 429 (rate limit)
  - 5xx errors
  - Empty or malformed responses
- **Exponential backoff retries** (up to 5 attempts)
- **Checkpoint system** for resuming interrupted runs
- **Comprehensive logging** with timestamps and levels

### 🧠 Data Transformation
- Converts raw Jira data → structured **JSONL**
- Each record includes:
  - Issue metadata (title, status, priority, project, reporter, timestamps)
  - Description and comments
  - Derived NLP tasks:
    - **Summarization**
    - **Classification**
    - **Question Answering**
    - **Status Prediction**
    - **Resolution Extraction**

### ⚡ Optimization & Reliability
- Session pooling (30–40% faster)
- Batch requests (50 issues/request)
- Progress tracking with `tqdm`
- Checkpoint auto-save every 50 issues
- Recoverable from crashes or network failures

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- pip installed
- Internet connection

---

### 🪟 On Windows
```cmd
git clone https://github.com/YOUR_USERNAME/apache-jira-scraper.git
cd apache-jira-scraper

# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run main pipeline
python main.py
