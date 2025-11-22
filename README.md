# XML to PostgreSQL Extractor

A tool for extracting data from cadastral XML files and importing them into a PostgreSQL database with multithreading support.

## Features

- ✅ Automatic scanning and processing of XML directories
- ✅ Multithreading with 10 default threads (customizable)
- ✅ Extracts data into 4 tables: `thuadat`, `canhan`, `giaychungnhan`, `hoso`
- ✅ Automatic foreign key constraint handling
- ✅ Batch insertion with conflict handling
- ✅ Thread-safe statistics tracking

## Requirements

- Python 3.7+
- PostgreSQL database
- Libraries listed in `requirements_xml_extractor.txt`

## Installation

```bash
pip install -r requirements_xml_extractor.txt
```

## Usage

### Basic

```bash
python extract_xml_to_db.py --xml-dir "G:\So lieu day 04.11"
```

### With Full Options

```bash
python extract_xml_to_db.py \
  --host localhost \
  --port 5432 \
  --database cadastral_db \
  --user postgres \
  --password your_password \
  --xml-dir "G:\So lieu day 04.11" \
  --threads 10 \
  --limit 100
```

### Parameters

- `--host`: Database host (default: localhost)
- `--port`: Database port (default: 5432)
- `--database`: Database name (default: cadastral_db)
- `--user`: Database user (default: postgres)
- `--password`: Database password (default: postgres)
- `--xml-dir`: Directory containing XML files
- `--threads`: Number of worker threads (default: 10)
- `--limit`: Limit number of files to process (for testing)

### Using Environment Variables

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=cadastral_db
export DB_USER=postgres
export DB_PASSWORD=your_password

python extract_xml_to_db.py --xml-dir "G:\So lieu day 04.11"
```

## Database Schema

The tool automatically creates the following tables:

### thuadat
Land parcel information with fields: thuaDatID, maDVHCXa, soHieuToBanDo, dienTich, voID, chongID, etc.

### canhan
Individual person information with fields: caNhanID, hoTen, namSinh, gioiTinh, soGiayTo, etc.

### giaychungnhan
Certificate information with fields: giayChungNhanID, soVaoSo, ngayCap, maVach, etc.

### hoso
Document/file information with fields: thanhPhanHoSoID, hoSoDangKySoID, giayChungNhanID, loaiGiayTo, etc.

## Multithreading

The tool uses `ThreadPoolExecutor` to process multiple XML files in parallel:
- Each thread has its own database connection
- Thread-safe statistics tracking
- Automatic commit and connection closure after processing

## Performance

With multithreading enabled, the tool can process multiple XML files simultaneously, significantly improving processing speed compared to single-threaded execution.

## Error Handling

- Failed files are tracked and reported in the summary
- Database transactions are rolled back on errors
- Processing continues even if individual files fail

## License

MIT
