# Kế Hoạch Thêm Multi-Threading cho extract_xml_to_db.py

## Tổng Quan
Thêm multi-threading để:
1. Quét thư mục song song
2. Xử lý XML files song song
3. Tích lũy batch và insert vào database hiệu quả hơn

---

## BƯỚC 1: Thêm Dependencies và Imports

### 1.1. Thêm imports cần thiết
- `threading` - cho locks và synchronization
- `concurrent.futures.ThreadPoolExecutor` - cho thread pool
- `queue.Queue` - cho batch accumulation
- `collections.defaultdict` - cho batch grouping

### 1.2. Thêm argument mới
- `--threads` hoặc `--workers`: số lượng worker threads (default: số CPU cores)
- `--batch-size`: kích thước batch trước khi insert (default: 1000)

---

## BƯỚC 2: Tạo Thread-Safe Database Connection Pool

### 2.1. Tạo class `DatabaseConnectionPool`
- Quản lý pool các database connections
- Mỗi thread có connection riêng
- Thread-safe: sử dụng lock khi lấy/trả connection
- Methods:
  - `get_connection()` - lấy connection từ pool
  - `return_connection(conn)` - trả connection về pool
  - `close_all()` - đóng tất cả connections

### 2.2. Sửa `DatabaseInserter`
- Thay vì giữ connection riêng, nhận connection từ pool
- Mỗi thread tạo `DatabaseInserter` instance riêng với connection riêng

---

## BƯỚC 3: Tạo Batch Accumulator Thread-Safe

### 3.1. Tạo class `BatchAccumulator`
- Tích lũy data từ nhiều threads
- Thread-safe với locks
- Tự động flush khi đạt batch size
- Methods:
  - `add_data(table_name, data_list)` - thêm data vào batch
  - `flush()` - force flush tất cả batches
  - `get_batch_size()` - lấy kích thước batch hiện tại

### 3.2. Batch Structure
```python
batches = {
    'canhan': [],
    'giaychungnhan': [],
    'thuadat': [],
    'hoso': []
}
```

---

## BƯỚC 4: Thread-Safe Statistics Tracking

### 4.1. Tạo class `ThreadSafeStats`
- Sử dụng `threading.Lock()` cho mỗi counter
- Methods:
  - `increment(key, value=1)` - tăng counter
  - `get_stats()` - lấy tất cả stats
  - `reset()` - reset counters

### 4.2. Thay thế `self.stats` dict bằng `ThreadSafeStats` instance

---

## BƯỚC 5: Multi-Threaded Directory Scanning

### 5.1. Sửa `FileScanner.find_xml_directories()`
- Sử dụng `ThreadPoolExecutor` để scan nhiều subdirectories song song
- Mỗi thread scan một subdirectory
- Collect results thread-safely

### 5.2. Sửa `FileScanner.get_xml_files_in_directory()`
- Có thể parallelize nếu có nhiều subdirectories trong xml folder
- Sử dụng `os.walk()` với thread pool

---

## BƯỚC 6: Worker Function cho File Processing

### 6.1. Tạo function `process_single_file_worker()`
- Nhận: xml_file_path, db_config, batch_accumulator, stats
- Parse XML file
- Extract data
- Add vào batch_accumulator (không insert ngay)
- Update stats
- Return success/failure

### 6.2. Xử lý Foreign Key Dependencies
- Vì insert theo batch, cần đảm bảo thứ tự:
  1. canhan → 2. giaychungnhan → 3. thuadat → 4. hoso
- Batch accumulator phải group theo table và insert theo thứ tự

---

## BƯỚC 7: Batch Insertion với Thread Pool

### 7.1. Tạo function `batch_insert_worker()`
- Nhận: table_name, data_batch, db_connection
- Insert batch vào database
- Return (inserted_count, skipped_count)

### 7.2. Tạo `BatchInserter` class
- Quản lý batch insertion với thread pool riêng
- Insert các tables theo thứ tự dependency
- Sử dụng connection pool

---

## BƯỚC 8: Sửa `XMLToDBExtractor.run()`

### 8.1. Flow mới:
```
1. Setup database (single thread)
2. Scan directories (multi-threaded)
3. Collect all XML files
4. Process files với ThreadPoolExecutor:
   - Parse XML
   - Extract data
   - Add to batch accumulator
5. Khi batch đầy hoặc hết files:
   - Insert batches theo thứ tự dependency
6. Flush remaining batches
7. Print summary
```

### 8.2. Progress Reporting
- Sử dụng `threading.Lock()` cho print statements
- Hoặc sử dụng `tqdm` với thread-safe wrapper
- Hiển thị: files processed, current batch size, etc.

---

## BƯỚC 9: Error Handling và Recovery

### 9.1. Per-Thread Error Handling
- Mỗi worker catch exceptions riêng
- Log errors với thread ID
- Continue processing các files khác

### 9.2. Database Error Handling
- Retry logic cho database connection errors
- Rollback per-thread transactions
- Track failed batches

### 9.3. Graceful Shutdown
- Handle KeyboardInterrupt
- Flush batches trước khi exit
- Close all connections properly

---

## BƯỚC 10: Optimization và Tuning

### 10.1. Batch Size Tuning
- Tự động điều chỉnh batch size dựa trên:
  - Memory usage
  - Database connection pool size
  - Number of threads

### 10.2. Thread Count Optimization
- Default: `min(32, os.cpu_count() + 4)`
- Có thể tune dựa trên:
  - I/O bound (file reading) → nhiều threads hơn
  - CPU bound (XML parsing) → ít threads hơn

### 10.3. Connection Pool Size
- Số connections = số threads + buffer
- Default: threads + 2

---

## BƯỚC 11: Testing và Validation

### 11.1. Unit Tests
- Test thread-safe classes
- Test batch accumulator
- Test connection pool

### 11.2. Integration Tests
- Test với small dataset
- Verify data integrity
- Check foreign key constraints

### 11.3. Performance Tests
- Compare single-threaded vs multi-threaded
- Measure throughput
- Check memory usage

---

## CẤU TRÚC CODE MỚI

```
XMLToDBExtractor
├── DatabaseConnectionPool (NEW)
├── ThreadSafeStats (NEW)
├── BatchAccumulator (NEW)
├── BatchInserter (NEW)
└── run()
    ├── setup_database()
    ├── scan_directories_parallel() (NEW)
    ├── process_files_parallel() (NEW)
    │   └── process_single_file_worker() (NEW)
    ├── insert_batches() (NEW)
    │   └── batch_insert_worker() (NEW)
    └── print_summary()
```

---

## LƯU Ý QUAN TRỌNG

1. **Foreign Key Constraints**: Phải insert theo thứ tự dependency
2. **Transaction Management**: Mỗi batch insert trong transaction riêng
3. **Memory Management**: Batch size không quá lớn để tránh OOM
4. **Database Locks**: PostgreSQL có thể lock tables, cần monitor
5. **Progress Tracking**: Thread-safe progress reporting
6. **Error Isolation**: Lỗi ở một file không ảnh hưởng files khác

---

## THỨ TỰ TRIỂN KHAI ĐỀ XUẤT

1. Bước 1-2: Setup infrastructure (imports, connection pool)
2. Bước 3-4: Thread-safe utilities (batch accumulator, stats)
3. Bước 5: Multi-threaded scanning
4. Bước 6-7: Worker functions và batch insertion
5. Bước 8: Integrate vào main flow
6. Bước 9-10: Error handling và optimization
7. Bước 11: Testing

---

## METRICS ĐỂ ĐO LƯỜNG

- **Throughput**: Files/second
- **Latency**: Time per file
- **Database Load**: Connections, queries/second
- **Memory Usage**: Peak memory consumption
- **CPU Usage**: Average CPU utilization
- **Error Rate**: Failed files / total files

