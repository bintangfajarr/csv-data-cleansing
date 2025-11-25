# Data Engineer Technical Test - CSV Data Cleansing

## Overview

This project is a comprehensive solution for **CSV Data Cleansing** technical test. The application reads raw CSV data containing duplicate records, cleans the data, transforms it into the proper format, and stores both clean and duplicate records into a PostgreSQL database. Additionally, it creates backup files in JSON and CSV formats.

---

## Features

### Test 1 - Data Cleansing (MANDATORY) 
- ✅ Read CSV file with raw data
- ✅ Identify and remove duplicate records based on `ids` column
- ✅ Transform data to proper format:
  - Dates: DD/MM/YYYY → YYYY-MM-DD
  - Names: Converted to UPPERCASE
  - Numeric fields: Proper integer conversion
  - Array fields: Parse from string to array
- ✅ Insert clean data to `data` table
- ✅ Insert duplicate data to `data_reject` table
- ✅ Create JSON backup for clean data with specific format
- ✅ Create CSV backup for duplicate data
- ✅ Comprehensive error handling and logging
- ✅ Database connection retry logic
- ✅ Multiple date format support

### Test 2 - Dockerization (BONUS) 
- ✅ Dockerfile for containerizing the application
- ✅ Docker Compose for orchestrating services
- ✅ PostgreSQL database in container
- ✅ Volume mounting for source and target directories
- ✅ Health checks and service dependencies
- ✅ Automated initialization with DDL script

---

## Prerequisites

- **Docker Desktop** (Windows/Mac) or **Docker Engine** (Linux)
- **Docker Compose** v2.0+
- **Python 3.11+** (for local development)
- **Git** (for version control)

### Check Installation

```bash
# Check Docker
docker --version
# Expected: Docker version 24.0.x or higher

# Check Docker Compose
docker-compose --version
# Expected: Docker Compose version v2.x.x or higher

# Check if Docker is running
docker ps
```

---

##  Installation

### Step 1: Clone Repository

```bash
git clone <repository-url>
```

### Step 2: Prepare Data File

```bash
# Place your scrap.csv file in the source directory
cp /path/to/your/scrap.csv ./source/

# Verify file exists
ls -l source/scrap.csv
```

### Step 3: Configure Environment

```bash
# For Docker (already configured in docker-compose.yaml)
# No additional configuration needed

# For Local Development (optional)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=test_db
DB_USER=postgres
DB_PASSWORD=password
SOURCE_PATH=./source
TARGET_PATH=./target
```

---

##  How to Run

### Option 1: Run with Docker Compose (RECOMMENDED)

This is the easiest and recommended method for Test 2.

```bash
# Step 1: Clean previous runs (optional)
docker-compose down -v
rm -rf target/*

# Step 2: Build and start services
docker-compose up --build

# Or run in detached mode (background)
docker-compose up -d --build

# Step 3: View logs
docker-compose logs -f app

# Step 4: Stop services
docker-compose down
```

**Expected Output:**
```
postgres_1  | database system is ready to accept connections
app_1       | Starting Data Cleansing Process
app_1       | Step 1: Reading CSV file...
app_1       | Total rows in CSV: 1000
app_1       | Step 2: Cleaning data (removing duplicates)...
app_1       | Clean records: 850
app_1       | Duplicate records: 150
app_1       | Step 3: Transforming data...
app_1       | Step 4: Inserting data to database...
app_1       | Step 5: Saving duplicate data to CSV...
app_1       | Step 6: Saving clean data to JSON...
app_1       | Process Completed Successfully!
```

### Option 2: Run Locally (Without Docker)

For local development and testing.

```bash
# Step 1: Start PostgreSQL with Docker
docker run -d \
  --name postgres-test \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=test_db \
  -p 5432:5432 \
  postgres:15-alpine

# Wait for PostgreSQL to be ready
sleep 15

# Step 2: Setup database schema
docker exec -i postgres-test psql -U postgres -d test_db < ddl.sql

# Step 3: Install Python dependencies
pip install -r requirements.txt

# Step 4: Update .env for localhost

DB_HOST=localhost
DB_PORT=5432
DB_NAME=test_db
DB_USER=postgres
DB_PASSWORD=password
SOURCE_PATH=./source
TARGET_PATH=./target

# Step 5: Run application
python main.py
```

### Option 3: Step-by-Step Docker Commands

For better control and debugging.

```bash
# Step 1: Build Docker image
docker-compose build --no-cache

# Step 2: Start PostgreSQL only
docker-compose up -d postgres

# Step 3: Wait for PostgreSQL to be ready
sleep 20
docker-compose exec postgres pg_isready -U postgres

# Step 4: Verify database tables
docker-compose exec postgres psql -U postgres -d test_db -c "\dt"

# Step 5: Run application
docker-compose up app

# Step 6: View results
docker-compose logs app
ls -lh target/
```

---

## Database Schema

The application uses PostgreSQL with two main tables:

### Table: `data` (Clean Data)

Stores deduplicated and cleaned records.

```sql
CREATE TABLE data (
    dates DATE NOT NULL,
    ids VARCHAR(255) PRIMARY KEY,
    names VARCHAR(500) NOT NULL,
    monthly_listeners INTEGER,
    popularity INTEGER,
    followers BIGINT,
    genres TEXT[],
    first_release VARCHAR(4),
    last_release VARCHAR(4),
    num_releases INTEGER,
    num_tracks INTEGER,
    playlists_found VARCHAR(50),
    feat_track_ids TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Table: `data_reject` (Duplicate Data)

Stores rejected duplicate records with reason.

```sql
CREATE TABLE data_reject (
    dates DATE NOT NULL,
    ids VARCHAR(255),
    names VARCHAR(500) NOT NULL,
    monthly_listeners INTEGER,
    popularity INTEGER,
    followers BIGINT,
    genres TEXT[],
    first_release VARCHAR(4),
    last_release VARCHAR(4),
    num_releases INTEGER,
    num_tracks INTEGER,
    playlists_found VARCHAR(50),
    feat_track_ids TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reject_reason VARCHAR(255)
);
```

**Key Design Decisions:**
- `ids` is PRIMARY KEY in `data` table (ensures uniqueness)
- Array columns use PostgreSQL's native `TEXT[]` type
- Timestamps track when records were inserted
- `reject_reason` column in `data_reject` for audit trail

---

## Output Format

### JSON Output (Clean Data)

**File:** `target/data_YYYYMMDDHHMMSS.json`

```json
{
  "row_count": 850,
  "data": [
    {
      "dates": "2024-04-13",
      "ids": "abc123",
      "names": "ARTIST NAME",
      "monthly_listeners": 1000000,
      "popularity": 85,
      "followers": 500000,
      "genres": ["pop", "rock"],
      "first_release": "2015",
      "last_release": "2024",
      "num_releases": 5,
      "num_tracks": 50,
      "playlists_found": "100",
      "feat_track_ids": ["track1", "track2"]
    }
  ]
}
```

### CSV Output (Duplicate Data)

**File:** `target/data_reject_YYYYMMDDHHMMSS.csv`

Same format as input CSV file, containing all duplicate records (keeping original format).

---

## Verification

### Check Database Records

```bash
# Connect to database
docker-compose exec postgres psql -U postgres -d test_db

# Count records in each table
SELECT 
    'Clean Records' as table_name, 
    COUNT(*) as total 
FROM data
UNION ALL
SELECT 
    'Duplicate Records' as table_name, 
    COUNT(*) as total 
FROM data_reject;

# View sample clean data
SELECT * FROM data LIMIT 5;

# View sample duplicate data
SELECT * FROM data_reject LIMIT 5;

# Exit psql
\q
```

### Check Output Files

```bash
# List output files
ls -lh target/

# View JSON structure
cat target/data_*.json | head -30

# Check JSON row count
cat target/data_*.json | grep "row_count"

# Count CSV lines
wc -l target/data_reject_*.csv
```

### Verification Script

```bash
#!/bin/bash

echo "=========================================="
echo "VERIFICATION REPORT"
echo "=========================================="

echo -e "\n1. DATABASE RECORDS:"
docker-compose exec postgres psql -U postgres -d test_db -c "
SELECT 
    (SELECT COUNT(*) FROM data) as clean_records,
    (SELECT COUNT(*) FROM data_reject) as duplicate_records;
"

echo -e "\n2. OUTPUT FILES:"
ls -lh target/

echo -e "\n3. SAMPLE DATA:"
docker-compose exec postgres psql -U postgres -d test_db -c "
SELECT ids, names, dates, monthly_listeners 
FROM data 
LIMIT 5;
"

echo -e "\n=========================================="
```

---

##  Data Transformation Rules

The application applies the following transformations:

1. **Duplicate Detection**
   - Based on `ids` column
   - Keeps first occurrence
   - Rejects subsequent duplicates

2. **Date Format**
   - Input: Various formats (DD/MM/YYYY, MM/DD/YYYY, etc.)
   - Output: YYYY-MM-DD (ISO 8601)

3. **Names**
   - Converted to UPPERCASE
   - Example: "artist name" → "ARTIST NAME"

4. **Numeric Fields**
   - Converted to integers
   - Invalid values become 0
   - Fields: monthly_listeners, popularity, followers, num_releases, num_tracks

5. **Array Fields**
   - Parsed from string to array
   - Example: "['pop', 'rock']" → ["pop", "rock"]
   - Fields: genres, feat_track_ids

6. **Year Fields**
   - Kept as string in YYYY format
   - Fields: first_release, last_release

---

##  Improvements Implemented
### Mandatory Requirements 
- ✅ Python script for data cleaning
- ✅ DDL script for database tables
- ✅ Duplicate detection and removal
- ✅ JSON and CSV output files
- ✅ Proper file naming with timestamp
- ✅ Database insertion for both clean and duplicate data
- ✅ Comprehensive README documentation

### Bonus Features 
- ✅ **Error Handling:** Comprehensive try-catch blocks with logging
- ✅ **Retry Logic:** Database connection retry mechanism
- ✅ **Multiple Date Formats:** Support for various date formats
- ✅ **Docker Containerization:** Full Docker and Docker Compose setup
- ✅ **Health Checks:** PostgreSQL health check in docker-compose
- ✅ **Logging:** Detailed logging for debugging
- ✅ **Documentation:** Comprehensive README with examples
- ✅ **Clean Code:** Well-structured with classes and functions
- ✅ **Type Hints:** Python type annotations
- ✅ **Environment Variables:** Configurable via .env file

### Potential Future Enhancements 
- Unit tests with pytest
- Integration tests
- Data validation layer
- Progress bar for large files
- Email notifications
- Scheduler integration (Airflow/Cron)
- API endpoint for triggering jobs
- Support for multiple database types
- Incremental loading
- Data versioning

---

## Configuration

### Environment Variables

| Variable | Description | Default | Docker Value |
|----------|-------------|---------|--------------|
| `DB_HOST` | Database host | localhost | postgres |
| `DB_PORT` | Database port | 5432 | 5432 |
| `DB_NAME` | Database name | test_db | test_db |
| `DB_USER` | Database user | postgres | postgres |
| `DB_PASSWORD` | Database password | password | password |
| `SOURCE_PATH` | Input directory | ./source | /app/source |
| `TARGET_PATH` | Output directory | ./target | /app/target |

### Customization

You can customize the behavior by modifying:

1. **Database connection:** Edit `.env` or `docker-compose.yaml`
2. **File paths:** Change SOURCE_PATH and TARGET_PATH
3. **Logging level:** Modify `logging.basicConfig()` in `main.py`
4. **Retry logic:** Adjust `max_retries` and `retry_delay` in `get_db_connection()`

---

##  Screenshots 
![ss](img\Screenshot 2025-11-22 192434.png)



##  Dependencies

### Python Packages

```txt
pandas==2.1.4         # Data manipulation
psycopg2-binary==2.9.9  # PostgreSQL adapter
python-dotenv==1.0.0   # Environment variables
pytest==7.4.3          # Testing (optional)
```


##  License

This project is created for technical assessment purposes.

---
