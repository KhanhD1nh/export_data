# Usage Examples for XML to PostgreSQL Extraction Tool

## Quick Start

### 1. Setup PostgreSQL Database

Using Docker (recommended):

```bash
docker run -d \
  --name sodo-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=cadastral_db \
  -p 5432:5432 \
  -v cadastral_db_pgdata:/var/lib/postgresql/data \
  postgres:16
```

Or create database manually:

```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE sodo;

# Exit psql
\q
```

### 2. Install Dependencies

Create and activate virtual environment:

```bash
# Create virtual environment with Python 3.11
uv venv .venv --python 3.11

# Activate (Windows PowerShell)
.venv\Scripts\activate

# Install dependencies
uv pip install -r requirements_xml_extractor.txt --python .venv/Scripts/python.exe
```

### 3. Test XML Parsing (No Database Required)

Test the XML parser without database connection:

```bash
# Test with default sample file
python test_xml_parser.py

# Test with specific XML file
python test_xml_parser.py test-data/xml/To1(BD299-LS)/DD-08071-37PK_XTHP-96-545F4CBE837342459A1144EB0BE28899.xml
```

**Note:** The tool scans only directories named `xml` within the root directory structure.

Expected output:
```
Testing XML file: test-data/xml/To1(BD299-LS)/DD-08071-37PK_XTHP-96-545F4CBE837342459A1144EB0BE28899.xml
======================================================================
✓ XML file parsed successfully

ThuaDat Data:
----------------------------------------------------------------------
  Record 1:
    thuaDatID: 545F4CBE837342459A1144EB0BE28899
    maDVHCXa: 08071
    soHieuToBanDo: 0
    soThuTuThua: 96
    dienTich: 300
    ...
```

### 4. Run Full Extraction to Database

#### Option A: Using Default Configuration

```bash
python extract_xml_to_db.py \
  --host localhost \
  --port 5432 \
  --database cadastral_db \
  --user postgres \
  --password yourpassword \
  --xml-dir test-data/xml
```

#### Option B: Using Environment Variables

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=cadastral_db
export DB_USER=postgres
export DB_PASSWORD=yourpassword

python extract_xml_to_db.py --xml-dir test-data/xml
```

#### Option C: Process Single Directory

Process only a specific subdirectory:

```bash
python extract_xml_to_db.py \
  --user postgres \
  --password yourpassword \
  --xml-dir "test-data/xml/To1(BD299-LS)"
```

## Verification Queries

After running the extraction, verify the data:

### Check Record Counts

```sql
-- Connect to database
psql -U postgres -d cadastral_db

-- Count records in each table
SELECT 'thuadat' as table_name, COUNT(*) as count FROM thuadat
UNION ALL
SELECT 'canhan', COUNT(*) FROM canhan
UNION ALL
SELECT 'giaychungnhan', COUNT(*) FROM giaychungnhan
UNION ALL
SELECT 'hoso', COUNT(*) FROM hoso;
```

### View Sample Data

```sql
-- View ThuaDat with spouse information
SELECT 
    thuaDatID,
    maDVHCXa,
    soThuTuThua,
    dienTich,
    voChongID
FROM thuadat
LIMIT 5;

-- View CaNhan with ID documents
SELECT 
    caNhanID,
    hoTen,
    namSinh,
    tenLoaiGiayToTuyThan,
    maDinhDanhCaNhan
FROM canhan
LIMIT 5;

-- View Certificates
SELECT 
    giayChungNhanID,
    soVaoSo,
    soPhatHanh,
    ngayCap
FROM giaychungnhan
LIMIT 5;

-- View Documents
SELECT 
    thanhPhanHoSoID,
    loaiGiayTo,
    tepTin
FROM hoso
LIMIT 5;
```

### Join Queries

```sql
-- Find land parcels with owner information
SELECT 
    t.thuaDatID,
    t.maDVHCXa,
    t.soThuTuThua,
    t.dienTich,
    c1.hoTen as vo_name,
    c2.hoTen as chong_name
FROM thuadat t
LEFT JOIN canhan c1 ON t.voID = c1.caNhanID
LEFT JOIN canhan c2 ON t.chongID = c2.caNhanID
WHERE t.voChongID IS NOT NULL
LIMIT 10;

-- Certificates with their documents
SELECT 
    g.giayChungNhanID,
    g.soVaoSo,
    g.ngayCap,
    h.loaiGiayTo,
    h.tepTin,
    h.url
FROM giaychungnhan g
JOIN hoso h ON g.giayChungNhanID = h.giayChungNhanID
LIMIT 10;
```

## Expected Output

When running the extraction tool, you should see output like:

```
============================================================
XML to PostgreSQL Extraction Tool
============================================================
✓ Database schema created/verified successfully
Scanning directory: test-data/xml
Found 125 XML files

Processing 125 XML files...
------------------------------------------------------------
[1/125] Processing: DD-08071-37PK_XTHP-96-545F4CBE83734.xml... ✓
[2/125] Processing: DD-08071-38PK_XTHP-45-123ABC456DEF78.xml... ✓
[3/125] Processing: DD-08072-40PK_XTHP-12-789DEF123ABC45.xml... ✓
...

============================================================
PROCESSING SUMMARY
============================================================
Files processed:      125
Files failed:         0

Data inserted:
  ThuaDat:            125 (skipped: 0)
  CaNhan:             250 (skipped: 0)
  GiayChungNhan:      125 (skipped: 0)
  HoSo:               125 (skipped: 0)
============================================================
```

## Handling Errors

### If Some Files Fail

The tool continues processing even if some files fail:

```
[10/125] Processing: bad_file.xml... ✗
Error processing test-data/xml/bad_file.xml: Invalid XML
```

Files that fail are logged but don't stop the entire process.

### Re-running the Tool

You can safely re-run the tool multiple times. Duplicate records are automatically skipped:

```
Data inserted:
  ThuaDat:            0 (skipped: 125)  ← All records already exist
  CaNhan:             5 (skipped: 245)  ← 5 new records, 245 duplicates
  ...
```

## Production Deployment

### Create a Configuration File

Create `db_config.env`:

```bash
DB_HOST=production-db.example.com
DB_PORT=5432
DB_NAME=cadastral_production
DB_USER=cadastral_user
DB_PASSWORD=secure_password_here
```

Load and run:

```bash
source db_config.env
python extract_xml_to_db.py --xml-dir /path/to/production/xml/files
```

### Automated Processing with Cron

Add to crontab for daily processing:

```bash
# Edit crontab
crontab -e

# Add line (runs daily at 2 AM)
0 2 * * * cd /path/to/project && source venv/bin/activate && source db_config.env && python extract_xml_to_db.py --xml-dir /path/to/xml >> /var/log/xml_extraction.log 2>&1
```

### Monitoring

Check logs and statistics:

```bash
# View recent extractions
tail -f /var/log/xml_extraction.log

# Check database growth
psql -U postgres -d cadastral_db -c "
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    n_tup_ins AS inserts,
    n_tup_upd AS updates
FROM pg_stat_user_tables
WHERE tablename IN ('thuadat', 'canhan', 'giaychungnhan', 'hoso')
ORDER BY tablename;
"
```

## Troubleshooting

### Memory Issues with Large Directories

If processing thousands of files:

```bash
# Process in batches by subdirectory
for dir in test-data/xml/*/; do
    echo "Processing $dir"
    python extract_xml_to_db.py --xml-dir "$dir"
done
```

### Performance Tuning

Adjust batch size by modifying the script's `page_size` parameter (default: 100).

For faster inserts on initial load, temporarily disable indexes:

```sql
-- Drop indexes before bulk load
DROP INDEX IF EXISTS idx_thuadat_madv;
DROP INDEX IF EXISTS idx_canhan_hoten;

-- After loading, recreate indexes
CREATE INDEX idx_thuadat_madv ON thuadat(maDVHCXa);
CREATE INDEX idx_canhan_hoten ON canhan(hoTen);
CREATE INDEX idx_hoso_gcn ON hoso(giayChungNhanID);
```

## Advanced Usage

### Export Statistics to CSV

```bash
python extract_xml_to_db.py --xml-dir test-data/xml > extraction_log.txt

# Extract summary
grep "PROCESSING SUMMARY" -A 10 extraction_log.txt > summary.txt
```

### Process Only New Files

Track processed files:

```bash
# Create processed files list
psql -U postgres -d cadastral_db -c "
SELECT DISTINCT thuaDatID FROM thuadat
" -t -o processed_ids.txt

# Use this list to skip already processed files (custom logic needed)
```

## Next Steps

1. ✅ Test with sample XML file
2. ✅ Verify database schema
3. ✅ Run full extraction
4. ✅ Verify data with SQL queries
5. 📋 Add indexes for performance
6. 📋 Setup automated backups
7. 📋 Configure monitoring
8. 📋 Document business rules

## Support

For questions or issues:
- Review the main README: `README_xml_extractor.md`
- Check the source code: `extract_xml_to_db.py`
- Test parsing: `python test_xml_parser.py <xml-file>`

