# 10Academy-Kifiya-Week-7

# Building a Scalable Data Pipeline for Ethiopian Medical Businesses:

## Introduction

In the rapidly evolving landscape of Ethiopian e-commerce, structured and accessible data is a key enabler for informed decision-making. At **Kara Solutions**, embarking on a mission to build a **comprehensive data warehouse** for Ethiopian medical businesses by leveraging data scraped from **Telegram channels**, the approach focuses on creating a **scalable, modular, and efficient pipeline**, ensuring seamless data collection, transformation, and analysis.


---
## Task 1: Data Scraping from Telegram

### **Objective:**
The goal of this task was to **collect raw data from Telegram channels** where medical businesses actively post their products, services, and contact details. Given the decentralized nature of business listings in Ethiopia, Telegram serves as a crucial data source.

### **Approach:**
A **scalable Telegram scraper** was developed using **Telethon**, an asynchronous Python library for interacting with the Telegram API. The scraper extracts messages from targeted channels, including:
- **Business Listings** (clinic and pharmacy details, supplier information)
- **Product Advertisements** (medications, equipment, medical supplies)
- **Contact Information** (phone numbers, addresses, website links)
- **Media Attachments** (images, PDFs, and videos related to the listings)

### **Key Features Implemented:**
✅ **Asynchronous scraping** for high efficiency and reduced API rate limitations  
✅ **Storage in MongoDB**, ensuring structured collection and retrieval  
✅ **Metadata extraction**, including timestamps, message IDs, and sender details  
✅ **Handling media files**, downloading and storing paths for images and videos  
✅ **Filtering relevant messages** based on keywords and patterns  

### **Challenges Faced & Solutions:**
**1. API Rate Limits:** implemented adaptive request throttling to avoid Telegram restrictions.  
**2. Unstructured Data:** Messages vary widely in format; applied regex-based parsing for structured extraction.  
**3. Media Handling:** To manage large files efficiently; store only the **file paths** in the database while keeping the actual media files in a designated storage directory.

---
## Task 2: Data Cleaning & Preprocessing

### **Objective:**
Raw data from Telegram is **highly unstructured**—often containing duplicate entries, missing values, and inconsistent formats. The objective of this task was to develop a **data cleaning pipeline** that transforms raw messages into **structured, high-quality data** ready for analysis and storage.

### **Approach:**
A **modular data cleaning pipeline** was built in Python, incorporating **Pandas**, **Regex**, and **SQLAlchemy** for efficient transformation. The pipeline follows these key steps:

### **Key Features Implemented:**
✅ **Duplicate Removal** – Ensuring unique business listings by identifying repeated message patterns  
✅ **Missing Value Handling** – Filling gaps in phone numbers, addresses, and product details  
✅ **Standardized Formatting** – Cleaning phone numbers, timestamps, and text for consistency  
✅ **Link Extraction** – Parsing URLs from message texts and storing them separately  
✅ **Media Grouping** – Grouping media files under a **single business listing** using message group IDs  
✅ **PostgreSQL Storage** – After cleaning, structured data is stored in a **relational database** for further processing  

### **Challenges Faced & Solutions:**
**1. Multiple Media Entries per Listing:** Many businesses post multiple images under one listing; grouped related media by **group_id** for accurate representation.  
**2. Extracting Relevant Information:** Messages often contain noise; refined the regex patterns and NLP-based parsing to extract only meaningful content.  
**3. Data Consistency:** Different posts use various date formats and phone number conventions; standardized all entries for uniformity.

## Task 3: Data Transformation

### **DBT Transformation**
With **DBT (Data Build Tool)**, the data was structured for efficient querying by:
- Setting up a **DBT project** for modeling transformations.
- Defining SQL-based transformations to clean and enrich data.
- Running automated tests to validate data consistency.
- Generating documentation for future scalability.

**Outcome:**
- Data is now structured for easy analytics.
- Unified schema across MongoDB and PostgreSQL.
- Better querying performance.

---

## Task 4: Object Detection Using YOLO
**YOLOv5**, a deep learning-based object detection model, was integrated to analyze images scraped from Telegram. The goal was to:
- Detect medical products or relevant entities in images.
- Extract bounding box coordinates and classification labels.
- Store detected objects alongside structured text data.

The pipeline involved:
1. **Downloading and processing media files** using optimized parallelized downloading.
2. **Running YOLOv5 inference** on stored images.
3. **Storing detection results** in MongoDB/PostgreSQL.

**Outcome:**
- Businesses now have an automated way to classify medical-related images.
- Detection data enhances analytics for better decision-making.

---

## Task 5: Exposing Data via FastAPI
To make the collected data accessible, a **FastAPI-based RESTful API** was built, following best practices for structuring the application:

**Key Components:**
- **Database Configuration:** SQLAlchemy-based database setup.
- **Models:** Defined SQLAlchemy ORM models for structured data representation.
- **Pydantic Schemas:** For data validation and serialization.
- **CRUD Operations:** Implemented Create, Read, Update, and Delete functionalities.
- **Endpoints:** Developed API routes to fetch and filter business information, images, and object detection results.

Users can now query businesses, filter by medical category, and retrieve images with detected objects—all via a **fast, efficient API**.

---

## Task 6: Automation with Docker and CI/CD
To ensure smooth deployment and scaling, the solution has been containerized using **Docker**:
- **Dockerized FastAPI application** with PostgreSQL.
- **Separate containers for MongoDB and YOLO inference.**
- **Automated deployments using GitHub Actions for CI/CD.**

This setup allows seamless updates, making it easy to maintain and scale the project.

---

## Conclusion
This project successfully streamlined the process of collecting, cleaning, and analyzing business data from Telegram. By leveraging **MongoDB, PostgreSQL, YOLOv5, FastAPI, and DBT**, a **scalable and maintainable** data warehouse that enables advanced analytics and business insights was built.

**Final Achievements:**
✅ Efficient Telegram data scraping and media collection.  
✅ Robust cleaning and transformation pipeline using DBT.  
✅ YOLO-powered object detection for media classification.  
✅ FastAPI-based RESTful service for easy data access.  
✅ Scalable, containerized deployment with Docker and CI/CD.

This project showcases the power of data engineering in **unlocking hidden insights** for Ethiopian medical businesses, paving the way for **data-driven decision-making**. 🚀

---
