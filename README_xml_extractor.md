# XML to PostgreSQL Extraction Tool

A Python tool for extracting data from XML cadastral files and importing them into a PostgreSQL database.

## Overview

This tool scans only directories named `xml` within the root directory and processes all XML files found in those subdirectories. It populates four PostgreSQL tables:
- **thuadat**: Land parcel information
- **canhan**: Individual person information  
- **giaychungnhan**: Certificate information
- **hoso**: Document/file information

**Note:** The tool specifically looks for directories named `xml` (e.g., `G:\So lieu day 04.11\1. Hien Luong\xml\`) and will not process XML files in other locations.

## Features

- ✅ Batch processing of multiple XML files
- ✅ Automatic table creation with foreign keys
- ✅ Conflict resolution (ON CONFLICT DO NOTHING)
- ✅ Transaction management with rollback on errors
- ✅ Relationship joining (VoChong to ThuaDat)
- ✅ Nested data extraction (GiayToTuyThan, ThanhPhanHoSo)
- ✅ Progress tracking and statistics
- ✅ Error handling with continue-on-error
- ✅ Automatic indexes for query optimization
- ✅ Referential integrity with foreign keys

## Requirements

```bash
pip install psycopg2-binary
```

Or if you prefer the source distribution:
```bash
pip install psycopg2
```

## Database Schema

### Entity Relationships

```
canhan (Cá nhân - Person)
    ↑
    |
    ├─── thuadat.voID (Vợ - Wife)
    └─── thuadat.chongID (Chồng - Husband)

giaychungnhan (Giấy chứng nhận - Certificate)
    ↑
    |
    └─── hoso.giayChungNhanID (Hồ sơ - Documents)
```

### Foreign Key Relationships

1. **thuadat → canhan**
   - `thuadat.voID` → `canhan.caNhanID` (Wife reference)
   - `thuadat.chongID` → `canhan.caNhanID` (Husband reference)
   - ON DELETE: SET NULL (preserve land records if person deleted)

2. **hoso → giaychungnhan**
   - `hoso.giayChungNhanID` → `giaychungnhan.giayChungNhanID`
   - ON DELETE: CASCADE (delete documents when certificate deleted)



### Table: thuadat
```sql
CREATE TABLE thuadat (
    thuaDatID VARCHAR(255) PRIMARY KEY,
    maDVHCXa VARCHAR(50),
    soHieuToBanDo VARCHAR(100),
    soThuTuThua VARCHAR(50),
    dienTich NUMERIC(15, 2),
    dienTichPhapLy NUMERIC(15, 2),
    diaChiID VARCHAR(255),
    voChongID VARCHAR(255),
    voID VARCHAR(255),
    chongID VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_thuadat_vo FOREIGN KEY (voID) REFERENCES canhan(caNhanID) ON DELETE SET NULL,
    CONSTRAINT fk_thuadat_chong FOREIGN KEY (chongID) REFERENCES canhan(caNhanID) ON DELETE SET NULL
);

-- Indexes
CREATE INDEX idx_thuadat_madv ON thuadat(maDVHCXa);
CREATE INDEX idx_thuadat_vochong ON thuadat(voChongID);
CREATE INDEX idx_thuadat_vo ON thuadat(voID);
CREATE INDEX idx_thuadat_chong ON thuadat(chongID);
```

### Table: canhan
```sql
CREATE TABLE canhan (
    caNhanID VARCHAR(255) PRIMARY KEY,
    hoTen VARCHAR(255),
    namSinh VARCHAR(10),
    diaChiID VARCHAR(255),
    giayToTuyThanID VARCHAR(255),
    tenLoaiGiayToTuyThan VARCHAR(255),
    ngayCap DATE,
    noiCap VARCHAR(255),
    maDinhDanhCaNhan VARCHAR(100),
    hieuLuc BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_canhan_hoten ON canhan(hoTen);
CREATE INDEX idx_canhan_namsinh ON canhan(namSinh);
```

### Table: giaychungnhan
```sql
CREATE TABLE giaychungnhan (
    giayChungNhanID VARCHAR(255) PRIMARY KEY,
    soVaoSo VARCHAR(100),
    soPhatHanh VARCHAR(100),
    MaGiayChungNhan VARCHAR(255),
    ngayCap TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_giaychungnhan_sovat ON giaychungnhan(soVaoSo);
CREATE INDEX idx_giaychungnhan_ngaycap ON giaychungnhan(ngayCap);
```

### Table: hoso
```sql
CREATE TABLE hoso (
    thanhPhanHoSoID VARCHAR(255) PRIMARY KEY,
    hoSoDangKySoID VARCHAR(255),
    giayChungNhanID VARCHAR(255),
    loaiGiayTo VARCHAR(255),
    tepTin VARCHAR(255),
    url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_hoso_giaychungnhan FOREIGN KEY (giayChungNhanID) REFERENCES giaychungnhan(giayChungNhanID) ON DELETE CASCADE
);

-- Indexes
CREATE INDEX idx_hoso_gcn ON hoso(giayChungNhanID);
CREATE INDEX idx_hoso_loaigiayto ON hoso(loaiGiayTo);
```

## Usage

### Basic Usage

Using default settings (localhost, port 5432, database: sodo, xml-dir: G:\So lieu day 04.11):

```bash
# Using default database connection (postgres/postgres on localhost:5432/sodo)
python extract_xml_to_db.py

# Test with only 1 file
python extract_xml_to_db.py --limit 1
```

### With Custom Database Configuration

```bash
python extract_xml_to_db.py \
  --host localhost \
  --port 5432 \
  --database my_cadastral_db \
  --user postgres \
  --password mypassword \
  --xml-dir test-data/xml
```

### Using Environment Variables

Set environment variables for database configuration:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=cadastral_db
export DB_USER=postgres
export DB_PASSWORD=yourpassword

python extract_xml_to_db.py
```

### Command-line Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--host` | `DB_HOST` | `localhost` | Database host |
| `--port` | `DB_PORT` | `5432` | Database port |
| `--database` | `DB_NAME` | `cadastral_db` | Database name |
| `--user` | `DB_USER` | `postgres` | Database user |
| `--password` | `DB_PASSWORD` | (empty) | Database password |
| `--xml-dir` | - | `test-data/xml` | XML files directory |

### Help

```bash
python extract_xml_to_db.py --help
```

## How It Works

### 1. Database Setup
- Connects to PostgreSQL database
- Creates tables if they don't exist (with `IF NOT EXISTS`)
- Uses transactions for data integrity

### 2. File Scanning
- Scans the root directory for subdirectories named `xml`
- Recursively scans within each `xml` directory for `.xml` files
- Ignores XML files in other directories
- Reports total `xml` directories and files found

### 3. XML Parsing & Data Extraction

For each XML file:

**ThuaDat Data:**
- Extracts from `<ThuaDatCollection>/<DC_ThuaDat>`
- Joins with VoChong data via `QuyenSuDungDat.doiTuongID`
- Includes: land parcel details, area, location, and spouse information

**CaNhan Data:**
- Extracts from `<CaNhanCollection>/<CaNhan>`
- Includes nested `GiayToTuyThan` (identification document)
- Captures: name, birth year, ID document details

**GiayChungNhan Data:**
- Extracts from `<GiayChungNhanCollection>/<GiayChungNhan>`
- Certificate numbers and issue dates

**HoSo Data:**
- Extracts from `<HoSoDangKyDatDaiCollection>/<HoSoDangKyDatDai>`
- Includes nested `ThanhPhanHoSoDangKyDatDai` entries
- Document files and URLs

### 4. Data Insertion
- Batch insert with `execute_batch` for performance
- `ON CONFLICT DO NOTHING` to skip duplicates
- Commits per file for better error isolation
- Rollback on errors

### 5. Progress & Statistics
- Real-time progress display
- Summary report with:
  - Files processed/failed
  - Records inserted per table
  - Records skipped (duplicates)

## Example Output

```
============================================================
XML to PostgreSQL Extraction Tool
============================================================
OK Database schema created/verified successfully
Scanning directory: G:\So lieu day 04.11
Looking for 'xml' subdirectories...
Found 5 'xml' directories
Found 125 XML files in 'xml' directories

Processing 125 XML files...
------------------------------------------------------------
[1/125] Processing: DD-08071-37PK_XTHP-96-545F4CBE83734.xml... ✓
[2/125] Processing: DD-08071-38PK_XTHP-45-123ABC456DEF78.xml... ✓
[3/125] Processing: DD-08072-40PK_XTHP-12-789DEF123ABC45.xml... ✓
...

============================================================
PROCESSING SUMMARY
============================================================
Files processed:      123
Files failed:         2

Data inserted:
  ThuaDat:            123 (skipped: 5)
  CaNhan:             246 (skipped: 12)
  GiayChungNhan:      123 (skipped: 3)
  HoSo:               123 (skipped: 0)
============================================================
```

## Data Relationships

### VoChong to ThuaDat Relationship

The tool intelligently joins spouse (VoChong) information with land parcels:

1. Extracts all VoChong records (voChongID, voID, chongID)
2. For each ThuaDat, searches QuyenSuDungDat records
3. Matches `QuyenSuDungDat.doiTuongID` with `VoChong.voChongID`
4. Stores spouse information directly in thuadat table

```xml
<ThuaDat>
  <thuaDatID>545F4CBE...</thuaDatID>
  ...
</ThuaDat>

<VoChong>
  <voChongID>9B2A0F27...</voChongID>
  <voID>C8D9568A...</voID>
  <chongID>9B2A0F27...</chongID>
</VoChong>

<QuyenSuDungDat>
  <thuaDatID>545F4CBE...</thuaDatID>
  <doiTuongID>9B2A0F27...</doiTuongID>  ← Links to VoChong
</QuyenSuDungDat>
```

### Nested Collections

**CaNhan → GiayToTuyThan:**
- Extracts first identification document from nested collection
- Flattens into single canhan record

**HoSoDangKyDatDai → ThanhPhanHoSoDangKyDatDai:**
- Creates separate hoso record for each document entry
- One-to-many relationship preserved

## Error Handling

- **File parsing errors**: Logged, file skipped, processing continues
- **Database errors**: Transaction rolled back, file skipped
- **Missing data**: NULL values inserted for optional fields
- **Duplicates**: Skipped via `ON CONFLICT DO NOTHING`

## Troubleshooting

### Connection Refused
```
Database connection error: could not connect to server
```
**Solution**: Check database is running and credentials are correct.

### Permission Denied
```
Database connection error: FATAL: role "user" does not exist
```
**Solution**: Verify database user exists and has appropriate permissions.

### No XML Files Found
```
No XML files found. Exiting.
```
**Solution**: Check `--xml-dir` path is correct and contains .xml files.

### Import psycopg2 Error
```
ModuleNotFoundError: No module named 'psycopg2'
```
**Solution**: Install psycopg2: `pip install psycopg2-binary`

## Performance Tips

1. **Batch Size**: Default batch size is 100 records. Modify `page_size` parameter in `execute_batch` calls for tuning.

2. **Database Indexes**: Indexes are automatically created during setup for:
   - Search by administrative code (`maDVHCXa`)
   - Search by person name (`hoTen`)
   - Search by certificate number (`soVaoSo`)
   - Foreign key relationships
   - Date ranges (`ngayCap`)
   
   No manual index creation needed!

3. **Large Datasets**: The tool processes directories on-the-fly, minimizing memory usage. Each directory is processed completely before moving to the next.

4. **Limit Processing**: Use `--limit N` to test with a limited number of files before full processing.

## Database Queries

### Example Queries

**Count records per table:**
```sql
SELECT 'thuadat' as table_name, COUNT(*) FROM thuadat
UNION ALL
SELECT 'canhan', COUNT(*) FROM canhan
UNION ALL
SELECT 'giaychungnhan', COUNT(*) FROM giaychungnhan
UNION ALL
SELECT 'hoso', COUNT(*) FROM hoso;
```

**Find land parcels with spouse information (using foreign keys):**
```sql
SELECT 
    t.thuaDatID,
    t.maDVHCXa,
    t.soThuTuThua,
    t.dienTich,
    v.hoTen AS ten_vo,
    v.namSinh AS namsinh_vo,
    c.hoTen AS ten_chong,
    c.namSinh AS namsinh_chong
FROM thuadat t
LEFT JOIN canhan v ON t.voID = v.caNhanID
LEFT JOIN canhan c ON t.chongID = c.caNhanID
WHERE t.voChongID IS NOT NULL;
```

**Find individuals with ID documents:**
```sql
SELECT 
    hoTen,
    namSinh,
    tenLoaiGiayToTuyThan,
    maDinhDanhCaNhan,
    ngayCap
FROM canhan
WHERE giayToTuyThanID IS NOT NULL;
```

**List all certificates with documents (using foreign keys):**
```sql
SELECT 
    g.giayChungNhanID,
    g.soVaoSo,
    g.ngayCap,
    COUNT(h.thanhPhanHoSoID) AS so_ho_so,
    STRING_AGG(h.loaiGiayTo, ', ') AS cac_loai_giay_to
FROM giaychungnhan g
LEFT JOIN hoso h ON g.giayChungNhanID = h.giayChungNhanID
GROUP BY g.giayChungNhanID, g.soVaoSo, g.ngayCap;
```

**Check referential integrity:**
```sql
-- Find land records with invalid person references
SELECT 
    t.thuaDatID,
    t.voID,
    t.chongID,
    CASE WHEN v.caNhanID IS NULL THEN 'Missing Wife' END AS vo_status,
    CASE WHEN c.caNhanID IS NULL THEN 'Missing Husband' END AS chong_status
FROM thuadat t
LEFT JOIN canhan v ON t.voID = v.caNhanID
LEFT JOIN canhan c ON t.chongID = c.caNhanID
WHERE (t.voID IS NOT NULL AND v.caNhanID IS NULL)
   OR (t.chongID IS NOT NULL AND c.caNhanID IS NULL);
```

## Architecture

### Class Structure

```
XMLToDBExtractor (Orchestrator)
├── DatabaseSchema (Schema management)
├── XMLParser (XML parsing)
├── DatabaseInserter (Data insertion)
└── FileScanner (Directory scanning)
```

### Data Flow

```
XML Files → FileScanner → XMLParser → DatabaseInserter → PostgreSQL
                              ↓
                     Extract & Transform
                       - ThuaDat
                       - CaNhan  
                       - GiayChungNhan
                       - HoSo
```

## License

This tool is part of the cadastral data management system.

## Support

For issues or questions, please contact the development team.

