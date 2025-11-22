<!-- 16dd25f9-4fa6-41cf-809e-7a6a2bf9107f 6b5f0c0f-32c3-4688-8bfb-9237833c92e6 -->
# XML to PostgreSQL Extraction Tool

## Overview

Create a Python script to parse XML files from the cadastral database and populate PostgreSQL tables with extracted data including ThuaDat, CaNhan, GiayChungNhan, and HoSo tables.

## Database Schema

Create 4 tables with the following structure:

**thuadat table:**

- thuaDatID (primary key)
- maDVHCXa, soHieuToBanDo, soThuTuThua, dienTich, dienTichPhapLy, diaChiID
- voChongID, voID, chongID (from VoChongCollection)

**canhan table:**

- caNhanID (primary key)
- hoTen, namSinh, diaChiID
- giayToTuyThanID, tenLoaiGiayToTuyThan, ngayCap, noiCap, maDinhDanhCaNhan, hieuLuc

**giaychungnhan table:**

- giayChungNhanID (primary key)
- soVaoSo, soPhatHanh, MaGiayChungNhan, ngayCap

**hoso table:**

- thanhPhanHoSoID (primary key)
- hoSoDangKySoID, giayChungNhanID
- loaiGiayTo, tepTin, url

## Implementation Approach

### Core Components

1. **Schema Creation** (`schema.sql` or within script)

- CREATE TABLE statements for all 4 tables
- Proper data types (VARCHAR, INTEGER, DATE, etc.)
- Primary key constraints

2. **XML Parser Class**

- Parse XML using `xml.etree.ElementTree`
- Handle nested collections (CaNhan → GiayToTuyThan)
- Extract data from specific XPath locations
- Join VoChong data with ThuaDat using matching IDs from QuyenSuDungDat

3. **Database Inserter Class**

- Use psycopg2 (matching existing codebase pattern in `import_to_database.py`)
- Batch insert with execute_batch for performance
- Handle duplicate key conflicts (ON CONFLICT DO NOTHING)
- Transaction management with commit/rollback

4. **File Scanner**

- Recursively scan `/test-data/xml/` directory
- Process all `.xml` files
- Track success/failure per file
- Continue processing on individual file errors

### Key Logic Details

**VoChong Relationship Resolution:**

- Extract VoChongCollection data
- Match with ThuaDat via QuyenSuDungDat.doiTuongID == VoChong.voChongID
- Store voChongID, voID, chongID in thuadat table rows

**Nested Data Extraction:**

- CaNhan has nested GiayToTuyThanCollection - extract first GiayToTuyThan entry
- HoSoDangKyDatDai has nested ThanhPhanHoSoDangKyDatDaiCollection - extract each entry

## Files to Create

- `extract_xml_to_db.py` - Main extraction script
- `README_xml_extractor.md` - Usage documentation

## Configuration

Database connection via:

- Environment variables OR
- Command-line arguments OR  
- Config dict (matching pattern in existing `import_to_database.py`)

### To-dos

- [ ] Create database schema SQL and Python code for 4 tables
- [ ] Implement XML parser class with nested collection handling
- [ ] Implement VoChong to ThuaDat relationship joining logic
- [ ] Create database insertion class with batch operations
- [ ] Implement directory scanner and batch file processor
- [ ] Create main script with CLI interface and error handling
- [ ] Write README with usage examples and configuration