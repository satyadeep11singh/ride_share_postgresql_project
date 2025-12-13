# Ride-Share Data Warehouse Project

A complete **PostgreSQL data warehouse** for a ride-sharing platform (similar to Uber/Lyft) featuring **star schema design**, **ETL pipelines**, and **interactive analytics dashboards**.

---

## 📋 Project Overview

This project demonstrates modern data engineering practices:
- **Raw data ingestion** from CSV files
- **Dimensional modeling** (star schema with 4 dimension tables + 1 fact table)
- **Data transformation** using SQL CTEs and window functions
- **Performance optimization** with strategic indexing
- **Analytics views** for business intelligence
- **Interactive visualizations** using Python + Plotly

**Key Metrics:**
- 50,000 rides analyzed
- 10,000 unique users
- 300 drivers tracked
- 278 unique dates (9+ months of data)
- 5 interactive HTML reports generated

---

## 📁 Project Structure

```
ride_share_sql_project/
├── sql/
│   ├── 01_ddl_create_raw_tables.sql          # Create raw staging tables
│   ├── 02_load_raw_data_from_csv.sql         # Bulk load CSV files
│   ├── 03_ddl_create_star_schema.sql         # Create dimensional schema
│   ├── 04_etl_populate_dimensions.sql        # Populate dimension tables
│   ├── 05_etl_populate_facts.sql             # Transform & load fact table
│   ├── 06_ddl_create_indexes_and_views.sql   # Optimize performance
│   └── 07_analytics_reporting_queries.sql    # Business intelligence queries
├── raw_data/
│   ├── users.csv          (10,000 rows)
│   ├── drivers.csv        (300 rows)
│   ├── vehicles.csv       (300 rows)
│   ├── rides.csv          (50,000 rows)
│   └── ratings.csv        (15,000 rows)
├── 08_analytics_dashboard.py   # Python Plotly visualization script
├── analytics_reports/          # Generated HTML reports
├── README.md                   # This file
└── .gitignore                  # Git ignore file
```

---

## 🏗️ Architecture

### **Data Layers**

```
┌─────────────────────────────────────────┐
│         Raw CSV Files                   │
│  (Users, Drivers, Vehicles, Rides)      │
└────────────────┬────────────────────────┘
                 │ (Load via \COPY)
┌────────────────▼────────────────────────┐
│      Raw Tables (Staging Layer)         │
│  raw_users, raw_drivers, raw_vehicles   │
│  raw_rides, raw_ratings                 │
└────────────────┬────────────────────────┘
                 │ (Transform & Cleanse)
┌────────────────▼────────────────────────┐
│    Star Schema (Analytical Layer)       │
│  ┌─────────────────────────────────┐    │
│  │ Dimension Tables                │    │
│  │ - dim_user (masked PII)         │    │
│  │ - dim_driver (vehicles)         │    │
│  │ - dim_date (time analysis)      │    │
│  └──────────────┬──────────────────┘    │
│                 │                       │
│  ┌──────────────▼──────────────────┐    │
│  │ Fact Table                      │    │
│  │ - fact_rides (50K transactions) │    │
│  │ - Metrics: duration, distance   │    │
│  │ - Derived: peak hours, wait times   │    │
│  └─────────────────────────────────┘    │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│     Analytics Layer                     │
│  Indexes, Views & Reporting Queries     │
│  v_driver_daily_utilization             │
│  3 Business Intelligence Queries        │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│   Interactive Dashboards (Python)       │
│  5 HTML Reports with Plotly Charts      │
└─────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### **Prerequisites**
- PostgreSQL 12+ installed and running
- Python 3.8+ installed
- psql CLI available

### **Step 1: Set Up Database**

```bash
# Create database
createdb ride_share_project

# Connect to database
psql -U <username> -d ride_share_project
```

### **Step 2: Run SQL Pipeline (Sequential Order)**

```bash
cd sql/

# 1. Create raw tables
psql -U <username> -d ride_share_project -f 01_ddl_create_raw_tables.sql

# 2. Load CSV data
psql -U <username> -d ride_share_project -f 02_load_raw_data_from_csv.sql

# 3. Create star schema
psql -U <username> -d ride_share_project -f 03_ddl_create_star_schema.sql

# 4. Populate dimensions
psql -U <username> -d ride_share_project -f 04_etl_populate_dimensions.sql

# 5. Load fact table
psql -U <username> -d ride_share_project -f 05_etl_populate_facts.sql

# 6. Create indexes & views
psql -U <username> -d ride_share_project -f 06_ddl_create_indexes_and_views.sql

# 7. Run reporting queries
psql -U <username> -d ride_share_project -f 07_analytics_reporting_queries.sql
```

### **Step 3: Generate Interactive Dashboards**

```bash
cd ..

# Install Python dependencies
pip install pandas plotly psycopg2 python-dotenv

# Create .env file (optional, uses defaults if missing)
# DB_HOST=localhost
# DB_PORT=5432
# DB_USER=<username>
# DB_PASSWORD=<password>
# DB_NAME=ride_share_project

# Run dashboard generator
python 08_analytics_dashboard.py
```

Open `analytics_reports/*.html` files in your browser to view interactive charts.

---

## 📊 Generated Reports

| Report | Chart Type | Key Metrics |
|--------|-----------|-------------|
| **01_driver_efficiency.html** | Scatter Plot | Top 10 most efficient drivers, wait times, ride count |
| **02_peak_hours_heatmap.html** | Heatmap | Revenue by day of week and hour of day |
| **03_top_peak_hours.html** | Bar Chart | Top 20 peak hours ranked by revenue |
| **04_vip_customers.html** | Dual Chart | Top 5 customers: spending vs. behavior |
| **05_vip_revenue_distribution.html** | Pie Chart | Revenue split among top 5 VIP customers |

---

## 🔍 Key Features

### **Data Quality & Privacy**
✓ PII masking (email/phone hashed with MD5)  
✓ Name anonymization (first initial only)  
✓ Location normalization  
✓ Idempotent operations (ON CONFLICT DO NOTHING)  

### **Advanced SQL Techniques**
✓ Window functions (LAG) for sequential analysis  
✓ CTEs (Common Table Expressions) for readable pipelines  
✓ Surrogate keys for dimension tables  
✓ Composite indexes for query optimization  

### **Business Metrics**
✓ Ride duration calculation  
✓ Peak hour detection (7-9 AM, 4-6 PM)  
✓ Driver turnaround time analysis  
✓ Customer lifetime value tracking  

### **Performance Optimization**
✓ 4 strategic indexes on foreign keys  
✓ Composite index for sequential queries  
✓ Pre-aggregated view for daily metrics  
✓ Query LIMIT constraints for responsiveness  

---

## 📈 Analytics Queries

### **Q1: Driver Efficiency**
Identifies top 10 most efficient drivers by minimizing idle (wait) time.
- Filters: >100 rides minimum
- Sorts: By average wait time (ascending), then by ride count (descending)

### **Q2: Peak Hour Revenue**
Discovers highest revenue-generating hours and days for surge pricing strategy.
- Extracts: Hour of day (0-23) from ride timestamps
- Aggregates: Revenue by day of week and hour
- Top: 20 results ranked by total revenue

### **Q3: Customer Segmentation**
Identifies VIP customers for loyalty programs based on lifetime value.
- Metrics: Total rides, total spending, average distance
- Ranks: By total spending (primary), then ride frequency (tie-breaker)
- Top: 5 highest-value customers

---

## 🛠️ SQL Pipeline Details

### **01 - Raw Tables (DDL)**
- 5 tables mirroring CSV structure
- Foreign key relationships defined
- No PII transformation at this stage

### **02 - Data Loading**
- Uses PostgreSQL `\COPY` command for bulk loading
- Loads ~75K total rows across all tables
- Expected execution time: <5 seconds

### **03 - Star Schema (DDL)**
- Creates 4 dimension tables with surrogate keys
- Fact table with foreign keys to all dimensions
- NUMERIC precision tuned for financial data

### **04 - Dimension Population (ETL)**
- `dim_date`: 278 dates extracted from ride data
- `dim_user`: PII hashing, name masking, location normalization
- `dim_driver`: Vehicle join, availability flags
- ON CONFLICT ensures idempotency

### **05 - Fact Loading (Advanced ETL)**
- 3 CTEs for data transformation pipeline
- Window function (LAG) calculates driver turnaround times
- Peak hour flag for surge pricing analysis
- INSERT 50K rows with calculated metrics

### **06 - Indexes & Views**
- 4 single-column indexes on foreign keys
- 1 composite index for driver sequential analysis
- 1 analytical view pre-aggregating daily metrics

### **07 - Analytics Queries**
- 3 business intelligence queries
- Use analytical view for simplified joins
- Return actionable business insights

---

## 🐍 Python Dashboard (08)

**Framework:** Plotly Express + Pandas  
**Database Driver:** psycopg2  
**Output:** Interactive HTML reports  

**Key Classes:**
- `RideShareAnalytics`: Manages database connections and visualization generation
- 8 methods for data fetching and charting
- Auto-creates `analytics_reports/` directory

**Features:**
- Custom hover templates with all relevant metrics
- Color scales for data emphasis (RdYlGn, Viridis, Plasma, Teal)
- Responsive layouts optimized for web viewing
- Pre-formatted text for pie chart hover data

---

## 📝 Technical Notes

### **Column Precision**
- `ride_duration_minutes`: NUMERIC(10, 2) - accommodates hours
- `time_to_next_ride_minutes`: NUMERIC(10, 2) - accommodates multi-day gaps
- `fare_amount`: NUMERIC(10, 2) - supports currency with cents

### **Window Function Logic**
```sql
LAG(ride_end_time) OVER (
    PARTITION BY driver_key
    ORDER BY ride_start_time
) AS previous_ride_end_time
```
Calculates previous ride's end time for each driver chronologically.

### **Peak Hour Definition**
Hours 7, 8, 16, 17 (7-9 AM, 4-6 PM) identified as commute times.

### **PII Protection**
- Email/Phone: MD5 hashing (irreversible)
- Name: First initial only (e.g., "J.")
- No raw PII stored in analytical layer

---

## 🔄 Execution Dependencies

```
01_ddl_create_raw_tables
    ↓
02_load_raw_data_from_csv
    ↓
03_ddl_create_star_schema
    ↓
04_etl_populate_dimensions
    ↓
05_etl_populate_facts
    ↓
06_ddl_create_indexes_and_views
    ↓
07_analytics_reporting_queries
    ↓
08_analytics_dashboard (Python)
```

**Must execute in order.** Each script assumes previous ones completed successfully.

---

## 📊 Sample Results

**Driver Efficiency (Top 3):**
- Joann Wolfe: 2,184 min avg wait, 188 rides
- Amber Taylor: 2,192 min avg wait, 193 rides
- John Foster: 2,205 min avg wait, 193 rides

**Peak Revenue Hours (Top 3):**
- Wednesday 11 PM: 335 rides, $19,987.79
- Thursday 7 AM: 333 rides, $19,943.49
- Wednesday 7 AM: 323 rides, $19,636.49

**VIP Customers (Top 3):**
- User "J.": 7,337 rides, $420,757
- User "M.": 5,838 rides, $331,578
- User "A.": 4,407 rides, $251,635

---

## 🎓 Learning Outcomes

By exploring this project, you'll learn:
- ✓ Star schema dimensional modeling
- ✓ PostgreSQL CTEs and window functions
- ✓ Data warehouse ETL pipeline design
- ✓ PII masking and data privacy
- ✓ Query optimization with indexes
- ✓ Python data visualization with Plotly
- ✓ Database-to-web reporting workflows

---

## 📄 License

This project is provided as-is for educational purposes.

---

## 👤 Author

Created: December 2025

---

## 🤝 Contributing

This is an educational project. Feel free to fork, modify, and learn!

---

**Happy analyzing! 📊**
