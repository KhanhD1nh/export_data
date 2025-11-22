# Kế Hoạch Triển Khai Multi-Threading cho extract_xml_to_db.py

## Tổng Quan Dự Án

**Mục tiêu**: Thêm multi-threading để tăng tốc độ xử lý XML files và insert vào PostgreSQL database.

**Phạm vi**: 
- Multi-threaded directory scanning
- Parallel XML file processing
- Batch accumulation và insertion
- Thread-safe statistics tracking

**Thời gian ước tính**: 5-7 ngày làm việc

---

## PHASE 1: Foundation & Infrastructure Setup
**Milestone**: Cơ sở hạ tầng thread-safe hoàn chỉnh, có thể test độc lập

**Thời gian ước tính**: 1.5 ngày

**Phụ thuộc**: Không có

**Critical Path**: ✅ Có (blocking Phase 2)

---

### Task 1.1: Add Required Imports and Arguments
**Task ID**: T1.1  
**Complexity**: Easy  
**Dependencies**: None  
**Critical Path**: ✅ Yes

#### Mô tả
Thêm các imports cần thiết cho multi-threading và thêm command-line arguments mới.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Thêm imports ở đầu file (sau dòng 20)**:
```python
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from collections import defaultdict
import os
```

**2. Sửa function `main()` - thêm arguments (sau dòng 848)**:
```python
parser.add_argument(
    '--threads',
    type=int,
    default=None,
    help='Number of worker threads (default: min(32, cpu_count + 4))'
)
parser.add_argument(
    '--batch-size',
    type=int,
    default=1000,
    help='Batch size before database insertion (default: 1000)'
)
```

**3. Tính toán default threads trong `main()` (sau dòng 860)**:
```python
if args.threads is None:
    import multiprocessing
    args.threads = min(32, multiprocessing.cpu_count() + 4)
```

#### Mục đích
Cung cấp infrastructure cơ bản và cho phép user config số threads và batch size.

#### Quan hệ với components khác
- Input cho các Phase sau
- `args.threads` → `XMLToDBExtractor.__init__()`
- `args.batch_size` → `BatchAccumulator.__init__()`

#### Acceptance Criteria
- ✅ Code compile không lỗi
- ✅ `--threads` và `--batch-size` arguments hoạt động
- ✅ Default threads = min(32, cpu_count + 4)
- ✅ Default batch-size = 1000

---

### Task 1.2: Create ThreadSafeStats Class
**Task ID**: T1.2  
**Complexity**: Medium  
**Dependencies**: T1.1  
**Critical Path**: ✅ Yes

#### Mô tả
Tạo class thread-safe để track statistics từ nhiều threads.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Tạo class mới (sau dòng 196, trước class XMLParser)**:
```python
class ThreadSafeStats:
    """Thread-safe statistics tracker"""
    
    def __init__(self):
        self._stats = {
            'files_processed': 0,
            'files_failed': 0,
            'thuadat_inserted': 0,
            'thuadat_skipped': 0,
            'canhan_inserted': 0,
            'canhan_skipped': 0,
            'giaychungnhan_inserted': 0,
            'giaychungnhan_skipped': 0,
            'hoso_inserted': 0,
            'hoso_skipped': 0
        }
        self._lock = threading.Lock()
    
    def increment(self, key: str, value: int = 1) -> None:
        """
        Thread-safe increment of a statistic counter.
        
        Args:
            key: Statistic key to increment
            value: Amount to increment (default: 1)
        """
        with self._lock:
            if key in self._stats:
                self._stats[key] += value
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get a copy of all statistics.
        
        Returns:
            Dictionary with all statistics
        """
        with self._lock:
            return self._stats.copy()
    
    def reset(self) -> None:
        """Reset all statistics to zero."""
        with self._lock:
            for key in self._stats:
                self._stats[key] = 0
    
    def get(self, key: str, default: int = 0) -> int:
        """
        Get a specific statistic value.
        
        Args:
            key: Statistic key
            default: Default value if key not found
            
        Returns:
            Statistic value or default
        """
        with self._lock:
            return self._stats.get(key, default)
```

#### Mục đích
Cung cấp thread-safe mechanism để track statistics từ nhiều threads mà không bị race condition.

#### Quan hệ với components khác
- Sử dụng bởi: `XMLToDBExtractor`, worker functions
- Thay thế: `self.stats` dict trong `XMLToDBExtractor`

#### Acceptance Criteria
- ✅ Class có thể instantiate
- ✅ `increment()` thread-safe (test với multiple threads)
- ✅ `get_stats()` trả về snapshot đúng
- ✅ Không có race conditions trong unit tests

---

### Task 1.3: Create DatabaseConnectionPool Class
**Task ID**: T1.3  
**Complexity**: Hard  
**Dependencies**: T1.1  
**Critical Path**: ✅ Yes

#### Mô tả
Tạo connection pool để quản lý database connections cho multiple threads.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Tạo class mới (sau class ThreadSafeStats, trước class XMLParser)**:
```python
class DatabaseConnectionPool:
    """Thread-safe database connection pool"""
    
    def __init__(self, db_config: Dict[str, Any], pool_size: int):
        """
        Initialize connection pool.
        
        Args:
            db_config: Database configuration dictionary
            pool_size: Maximum number of connections in pool
        """
        self.db_config = db_config
        self.pool_size = pool_size
        self._pool = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created_connections = []
        self._initialize_pool()
    
    def _initialize_pool(self) -> None:
        """Create initial connections for the pool."""
        for _ in range(self.pool_size):
            try:
                conn = psycopg2.connect(**self.db_config)
                self._pool.put(conn)
                self._created_connections.append(conn)
            except Exception as e:
                print(f"Warning: Failed to create connection: {e}")
    
    def get_connection(self, timeout: float = 30.0) -> Optional[psycopg2.extensions.connection]:
        """
        Get a connection from the pool (thread-safe).
        
        Args:
            timeout: Maximum time to wait for a connection (seconds)
            
        Returns:
            Database connection or None if timeout
        """
        try:
            conn = self._pool.get(timeout=timeout)
            # Test if connection is still alive
            if conn.closed:
                # Create new connection
                conn = psycopg2.connect(**self.db_config)
                with self._lock:
                    self._created_connections.append(conn)
            return conn
        except Exception as e:
            print(f"Error getting connection from pool: {e}")
            return None
    
    def return_connection(self, conn: psycopg2.extensions.connection) -> None:
        """
        Return a connection to the pool (thread-safe).
        
        Args:
            conn: Connection to return
        """
        if conn and not conn.closed:
            try:
                # Rollback any uncommitted transactions
                conn.rollback()
                self._pool.put_nowait(conn)
            except Exception as e:
                print(f"Error returning connection to pool: {e}")
                try:
                    conn.close()
                except:
                    pass
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            # Close connections in pool
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                except:
                    pass
            
            # Close all created connections
            for conn in self._created_connections:
                try:
                    if not conn.closed:
                        conn.close()
                except:
                    pass
            
            self._created_connections.clear()
```

#### Mục đích
Quản lý pool các database connections để mỗi thread có connection riêng, tránh connection conflicts.

#### Quan hệ với components khác
- Sử dụng bởi: `BatchInserter`, worker functions
- Tạo trong: `XMLToDBExtractor.setup_database()`
- Thay thế: single connection trong `DatabaseInserter`

#### Acceptance Criteria
- ✅ Pool tạo đúng số connections
- ✅ `get_connection()` thread-safe
- ✅ `return_connection()` thread-safe
- ✅ `close_all()` đóng tất cả connections
- ✅ Handle closed connections gracefully
- ✅ Test với multiple concurrent threads

---

## PHASE 2: Batch Accumulation & Database Infrastructure
**Milestone**: Batch accumulator hoạt động, DatabaseInserter hỗ trợ connection pool

**Thời gian ước tính**: 1.5 ngày

**Phụ thuộc**: Phase 1

**Critical Path**: ✅ Có (blocking Phase 3)

---

### Task 2.1: Create BatchAccumulator Class
**Task ID**: T2.1  
**Complexity**: Hard  
**Dependencies**: T1.1, T1.2  
**Critical Path**: ✅ Yes

#### Mô tả
Tạo class thread-safe để tích lũy data từ nhiều threads trước khi insert vào database.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Tạo class mới (sau class DatabaseConnectionPool, trước class XMLParser)**:
```python
class BatchAccumulator:
    """Thread-safe batch accumulator for database insertions"""
    
    # Table insertion order (respecting foreign key dependencies)
    TABLE_ORDER = ['canhan', 'giaychungnhan', 'thuadat', 'hoso']
    
    def __init__(self, batch_size: int = 1000):
        """
        Initialize batch accumulator.
        
        Args:
            batch_size: Maximum size before auto-flush
        """
        self.batch_size = batch_size
        self._batches = {
            'canhan': [],
            'giaychungnhan': [],
            'thuadat': [],
            'hoso': []
        }
        self._locks = {
            'canhan': threading.Lock(),
            'giaychungnhan': threading.Lock(),
            'thuadat': threading.Lock(),
            'hoso': threading.Lock()
        }
        self._flush_callback = None
    
    def set_flush_callback(self, callback: callable) -> None:
        """
        Set callback function to be called when batch is full.
        
        Args:
            callback: Function(table_name: str, batch: List[Dict]) -> None
        """
        self._flush_callback = callback
    
    def add_data(self, table_name: str, data_list: List[Dict[str, Any]]) -> bool:
        """
        Add data to batch (thread-safe).
        
        Args:
            table_name: Name of table ('canhan', 'giaychungnhan', 'thuadat', 'hoso')
            data_list: List of data dictionaries to add
            
        Returns:
            True if batch was flushed, False otherwise
        """
        if table_name not in self._batches:
            raise ValueError(f"Unknown table: {table_name}")
        
        if not data_list:
            return False
        
        with self._locks[table_name]:
            self._batches[table_name].extend(data_list)
            current_size = len(self._batches[table_name])
            
            if current_size >= self.batch_size:
                return self._flush_table(table_name)
        
        return False
    
    def _flush_table(self, table_name: str) -> bool:
        """
        Flush a specific table's batch (internal, assumes lock held).
        
        Args:
            table_name: Table to flush
            
        Returns:
            True if flushed, False otherwise
        """
        if not self._batches[table_name]:
            return False
        
        batch = self._batches[table_name].copy()
        self._batches[table_name].clear()
        
        if self._flush_callback:
            self._flush_callback(table_name, batch)
        
        return True
    
    def flush(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Force flush all batches (thread-safe).
        
        Returns:
            Dictionary of {table_name: batch_data}
        """
        flushed = {}
        
        for table_name in self.TABLE_ORDER:
            with self._locks[table_name]:
                if self._batches[table_name]:
                    flushed[table_name] = self._batches[table_name].copy()
                    self._batches[table_name].clear()
        
        return flushed
    
    def get_batch_size(self, table_name: str) -> int:
        """
        Get current batch size for a table (thread-safe).
        
        Args:
            table_name: Table name
            
        Returns:
            Current batch size
        """
        if table_name not in self._batches:
            return 0
        
        with self._locks[table_name]:
            return len(self._batches[table_name])
    
    def get_total_size(self) -> int:
        """
        Get total size across all tables (thread-safe).
        
        Returns:
            Total number of items in all batches
        """
        total = 0
        for table_name in self._batches:
            with self._locks[table_name]:
                total += len(self._batches[table_name])
        return total
```

#### Mục đích
Tích lũy data từ nhiều threads, tự động flush khi đạt batch size, đảm bảo thread-safety.

#### Quan hệ với components khác
- Sử dụng bởi: worker functions trong Phase 3
- Callback: `BatchInserter.insert_batch()` (Phase 2)
- Tích hợp: `XMLToDBExtractor.run()`

#### Acceptance Criteria
- ✅ `add_data()` thread-safe cho mỗi table
- ✅ Tự động flush khi đạt batch_size
- ✅ `flush()` trả về tất cả batches
- ✅ Callback được gọi khi flush
- ✅ Không có race conditions trong tests

---

### Task 2.2: Modify DatabaseInserter to Support Connection Pool
**Task ID**: T2.2  
**Complexity**: Medium  
**Dependencies**: T1.3  
**Critical Path**: ✅ Yes

#### Mô tả
Sửa `DatabaseInserter` để có thể nhận connection từ pool thay vì tự tạo.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Sửa `__init__` method (dòng 435-438)**:
```python
def __init__(self, db_config: Dict[str, Any], connection: Optional[psycopg2.extensions.connection] = None):
    """
    Initialize database inserter.
    
    Args:
        db_config: Database configuration (used if connection is None)
        connection: Optional existing database connection
    """
    self.db_config = db_config
    self.conn = connection
    self.cursor = None
    if self.conn:
        self.cursor = self.conn.cursor()
```

**2. Sửa `connect` method (dòng 440-448)**:
```python
def connect(self) -> bool:
    """
    Connect to PostgreSQL database (only if no connection provided).
    
    Returns:
        True if connected, False otherwise
    """
    if self.conn:
        return True  # Already have connection
    
    try:
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()
        return True
    except Exception as e:
        print(f"Database connection error: {e}")
        return False
```

**3. Thêm method mới (sau `rollback` method, dòng 598)**:
```python
def set_connection(self, connection: psycopg2.extensions.connection) -> None:
    """
    Set an existing connection (for connection pool usage).
    
    Args:
        connection: Database connection to use
    """
    if self.conn and self.cursor:
        # Don't close, just replace
        pass
    self.conn = connection
    if self.conn and not self.conn.closed:
        self.cursor = self.conn.cursor()
```

**4. Sửa `disconnect` method (dòng 450-455)**:
```python
def disconnect(self, return_to_pool: bool = False):
    """
    Close database connection.
    
    Args:
        return_to_pool: If True, don't close connection (for pool usage)
    """
    if self.cursor:
        self.cursor.close()
        self.cursor = None
    
    if self.conn and not return_to_pool:
        self.conn.close()
        self.conn = None
```

#### Mục đích
Cho phép `DatabaseInserter` sử dụng connection từ pool thay vì tự quản lý connection.

#### Quan hệ với components khác
- Sử dụng bởi: `BatchInserter` (Phase 2)
- Nhận connection từ: `DatabaseConnectionPool.get_connection()`
- Tương thích ngược: vẫn hoạt động với single connection mode

#### Acceptance Criteria
- ✅ Có thể khởi tạo với connection từ pool
- ✅ Có thể khởi tạo với db_config (backward compatible)
- ✅ `disconnect(return_to_pool=True)` không đóng connection
- ✅ Tất cả insert methods vẫn hoạt động bình thường

---

### Task 2.3: Create BatchInserter Class
**Task ID**: T2.3  
**Complexity**: Hard  
**Dependencies**: T1.3, T2.1, T2.2  
**Critical Path**: ✅ Yes

#### Mô tả
Tạo class để quản lý batch insertion với connection pool và thread pool.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Tạo class mới (sau class DatabaseInserter, trước class FileScanner)**:
```python
class BatchInserter:
    """Manages batch database insertions with connection pool"""
    
    # Insertion order respecting foreign key dependencies
    TABLE_ORDER = ['canhan', 'giaychungnhan', 'thuadat', 'hoso']
    
    def __init__(self, connection_pool: DatabaseConnectionPool):
        """
        Initialize batch inserter.
        
        Args:
            connection_pool: Database connection pool
        """
        self.connection_pool = connection_pool
    
    def insert_batch(self, table_name: str, batch: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Insert a batch of data for a specific table.
        
        Args:
            table_name: Table name
            batch: List of data dictionaries
            
        Returns:
            Tuple of (inserted_count, skipped_count)
        """
        if not batch:
            return 0, 0
        
        conn = self.connection_pool.get_connection()
        if not conn:
            print(f"Error: Could not get connection for {table_name} batch insert")
            return 0, len(batch)
        
        try:
            inserter = DatabaseInserter({}, connection=conn)
            
            if table_name == 'canhan':
                inserted, skipped = inserter.insert_canhan(batch)
            elif table_name == 'giaychungnhan':
                inserted, skipped = inserter.insert_giaychungnhan(batch)
            elif table_name == 'thuadat':
                inserted, skipped = inserter.insert_thuadat(batch)
            elif table_name == 'hoso':
                inserted, skipped = inserter.insert_hoso(batch)
            else:
                print(f"Error: Unknown table {table_name}")
                return 0, len(batch)
            
            inserter.commit()
            return inserted, skipped
            
        except Exception as e:
            print(f"Error inserting batch for {table_name}: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return 0, len(batch)
        finally:
            inserter.disconnect(return_to_pool=True)
            self.connection_pool.return_connection(conn)
    
    def insert_batches_ordered(self, batches: Dict[str, List[Dict[str, Any]]], 
                                stats: ThreadSafeStats) -> None:
        """
        Insert multiple batches respecting foreign key order.
        
        Args:
            batches: Dictionary of {table_name: batch_data}
            stats: ThreadSafeStats instance to update
        """
        for table_name in self.TABLE_ORDER:
            if table_name in batches and batches[table_name]:
                inserted, skipped = self.insert_batch(table_name, batches[table_name])
                
                # Update stats
                if table_name == 'canhan':
                    stats.increment('canhan_inserted', inserted)
                    stats.increment('canhan_skipped', skipped)
                elif table_name == 'giaychungnhan':
                    stats.increment('giaychungnhan_inserted', inserted)
                    stats.increment('giaychungnhan_skipped', skipped)
                elif table_name == 'thuadat':
                    stats.increment('thuadat_inserted', inserted)
                    stats.increment('thuadat_skipped', skipped)
                elif table_name == 'hoso':
                    stats.increment('hoso_inserted', inserted)
                    stats.increment('hoso_skipped', skipped)
```

#### Mục đích
Quản lý batch insertion với connection pool, đảm bảo insert theo thứ tự dependency.

#### Quan hệ với components khác
- Sử dụng: `DatabaseConnectionPool`, `DatabaseInserter`, `ThreadSafeStats`
- Sử dụng bởi: `BatchAccumulator` callback, `XMLToDBExtractor`
- Đảm bảo: Foreign key constraints được respect

#### Acceptance Criteria
- ✅ Insert batches theo đúng thứ tự dependency
- ✅ Sử dụng connection pool đúng cách
- ✅ Update stats correctly
- ✅ Handle errors gracefully
- ✅ Return connections to pool

---

## PHASE 3: Multi-Threaded File Processing
**Milestone**: Files được process song song, data được accumulate vào batches

**Thời gian ước tính**: 1.5 ngày

**Phụ thuộc**: Phase 2

**Critical Path**: ✅ Có (blocking Phase 4)

---

### Task 3.1: Create Worker Function for File Processing
**Task ID**: T3.1  
**Complexity**: Medium  
**Dependencies**: T2.1, T1.2  
**Critical Path**: ✅ Yes

#### Mô tả
Tạo worker function để process một XML file, extract data và add vào batch accumulator.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Tạo function mới (sau class BatchInserter, trước class FileScanner)**:
```python
def process_single_file_worker(xml_file: str, 
                                batch_accumulator: BatchAccumulator,
                                stats: ThreadSafeStats) -> Tuple[bool, str]:
    """
    Worker function to process a single XML file (thread-safe).
    
    Args:
        xml_file: Path to XML file
        batch_accumulator: BatchAccumulator instance
        stats: ThreadSafeStats instance
        
    Returns:
        Tuple of (success: bool, error_message: str)
    """
    try:
        # Parse XML
        parser = XMLParser(xml_file)
        if not parser.parse():
            stats.increment('files_failed')
            return False, f"Failed to parse XML: {xml_file}"
        
        # Extract all data
        thuadat_data = parser.extract_thuadat_data()
        canhan_data = parser.extract_canhan_data()
        gcn_data = parser.extract_giaychungnhan_data()
        hoso_data = parser.extract_hoso_data()
        
        # Add to batch accumulator (thread-safe)
        batch_accumulator.add_data('canhan', canhan_data)
        batch_accumulator.add_data('giaychungnhan', gcn_data)
        batch_accumulator.add_data('thuadat', thuadat_data)
        batch_accumulator.add_data('hoso', hoso_data)
        
        stats.increment('files_processed')
        return True, ""
        
    except Exception as e:
        stats.increment('files_failed')
        error_msg = f"Error processing {xml_file}: {str(e)}"
        return False, error_msg
```

#### Mục đích
Worker function để process XML files trong thread pool, extract data và accumulate vào batches.

#### Quan hệ với components khác
- Sử dụng: `XMLParser`, `BatchAccumulator`, `ThreadSafeStats`
- Được gọi bởi: `XMLToDBExtractor.process_files_parallel()` (Phase 3)
- Thread-safe: Không cần locks vì sử dụng thread-safe components

#### Acceptance Criteria
- ✅ Parse XML thành công
- ✅ Extract tất cả 4 loại data
- ✅ Add vào batch accumulator đúng cách
- ✅ Update stats correctly
- ✅ Handle errors gracefully
- ✅ Return success/failure status

---

### Task 3.2: Implement Multi-Threaded Directory Scanning
**Task ID**: T3.2  
**Complexity**: Medium  
**Dependencies**: T1.1  
**Critical Path**: No

#### Mô tả
Sửa `FileScanner` để scan directories song song.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Thêm method mới vào class `FileScanner` (sau method `get_xml_files_in_directory`, dòng 644)**:
```python
@staticmethod
def scan_subdirectory_worker(item_path: str, root_directory: str) -> Optional[Tuple[str, str]]:
    """
    Worker function to scan a single subdirectory for 'xml' folder.
    
    Args:
        item_path: Path to subdirectory
        root_directory: Root directory path
        
    Returns:
        Tuple of (parent_name, xml_dir_path) or None
    """
    try:
        if os.path.isdir(item_path):
            xml_dir = os.path.join(item_path, 'xml')
            if os.path.exists(xml_dir) and os.path.isdir(xml_dir):
                parent_name = os.path.basename(item_path)
                return (parent_name, xml_dir)
    except Exception as e:
        print(f"Error scanning {item_path}: {e}")
    return None

def find_xml_directories_parallel(self, max_workers: int = None) -> List[Tuple[str, str]]:
    """
    Find all 'xml' directories using parallel scanning.
    
    Args:
        max_workers: Maximum number of worker threads (default: min(32, cpu_count + 4))
        
    Returns:
        List of (parent_name, xml_dir_path) tuples
    """
    print(f"Scanning directory: {self.root_directory}")
    print("Looking for 'xml' subdirectories (parallel)...")
    
    if not os.path.exists(self.root_directory):
        print(f"Error: Directory not found: {self.root_directory}")
        return []
    
    if max_workers is None:
        import multiprocessing
        max_workers = min(32, multiprocessing.cpu_count() + 4)
    
    xml_dirs = []
    
    try:
        # Get all subdirectories
        subdirs = []
        for item in os.listdir(self.root_directory):
            item_path = os.path.join(self.root_directory, item)
            subdirs.append(item_path)
        
        # Scan in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.scan_subdirectory_worker, item_path, self.root_directory): item_path
                for item_path in subdirs
            }
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    xml_dirs.append(result)
        
        print(f"Found {len(xml_dirs)} 'xml' directories\n")
        return xml_dirs
        
    except Exception as e:
        print(f"Error scanning directory: {e}")
        return []
```

**2. Sửa method `get_xml_files_in_directory` để hỗ trợ parallel (dòng 636-644)**:
```python
@staticmethod
def get_xml_files_in_directory(xml_dir: str, parallel: bool = False, 
                                max_workers: int = None) -> List[str]:
    """
    Get all XML files in a specific directory.
    
    Args:
        xml_dir: Directory to scan
        parallel: If True, use parallel scanning for subdirectories
        max_workers: Maximum worker threads (if parallel=True)
        
    Returns:
        List of XML file paths
    """
    xml_files = []
    
    if parallel and max_workers:
        # For deeply nested structures, could parallelize os.walk
        # For now, keep simple sequential walk
        pass
    
    for root, dirs, files in os.walk(xml_dir):
        for file in files:
            if file.endswith('.xml'):
                full_path = os.path.join(root, file)
                xml_files.append(full_path)
    
    return xml_files
```

#### Mục đích
Tăng tốc độ scanning directories bằng cách scan nhiều subdirectories song song.

#### Quan hệ với components khác
- Sử dụng bởi: `XMLToDBExtractor.run()` (Phase 4)
- Thay thế: `find_xml_directories()` (có thể giữ cả hai methods)

#### Acceptance Criteria
- ✅ Scan directories song song
- ✅ Tìm đúng tất cả 'xml' directories
- ✅ Handle errors gracefully
- ✅ Faster than sequential scanning

---

### Task 3.3: Implement process_files_parallel Method
**Task ID**: T3.3  
**Complexity**: Hard  
**Dependencies**: T3.1, T2.1, T1.2  
**Critical Path**: ✅ Yes

#### Mô tả
Tạo method trong `XMLToDBExtractor` để process files song song với ThreadPoolExecutor.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Thêm method mới vào class `XMLToDBExtractor` (sau method `process_xml_file`, dòng 722)**:
```python
def process_files_parallel(self, xml_files: List[str], 
                          batch_accumulator: BatchAccumulator,
                          max_workers: int,
                          print_lock: threading.Lock = None) -> None:
    """
    Process XML files in parallel using ThreadPoolExecutor.
    
    Args:
        xml_files: List of XML file paths
        batch_accumulator: BatchAccumulator instance
        max_workers: Maximum number of worker threads
        print_lock: Optional lock for thread-safe printing
    """
    if not xml_files:
        return
    
    def safe_print(*args, **kwargs):
        """Thread-safe print function"""
        if print_lock:
            with print_lock:
                print(*args, **kwargs)
        else:
            print(*args, **kwargs)
    
    total_files = len(xml_files)
    processed_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_single_file_worker, xml_file, 
                          batch_accumulator, self.stats): xml_file
            for xml_file in xml_files
        }
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            xml_file = future_to_file[future]
            file_name = os.path.basename(xml_file)
            
            try:
                success, error_msg = future.result()
                
                processed_count += 1
                if success:
                    safe_print(f"  [{processed_count}/{total_files}] {file_name}... OK")
                else:
                    failed_count += 1
                    safe_print(f"  [{processed_count}/{total_files}] {file_name}... FAILED")
                    if error_msg:
                        safe_print(f"    Error: {error_msg}")
                        
            except Exception as e:
                failed_count += 1
                processed_count += 1
                safe_print(f"  [{processed_count}/{total_files}] {file_name}... FAILED")
                safe_print(f"    Exception: {str(e)}")
```

#### Mục đích
Process nhiều XML files song song, accumulate data vào batches, track progress thread-safely.

#### Quan hệ với components khác
- Sử dụng: `process_single_file_worker()`, `BatchAccumulator`, `ThreadSafeStats`
- Được gọi bởi: `XMLToDBExtractor.run()` (Phase 4)
- Tích hợp: Với batch flushing logic

#### Acceptance Criteria
- ✅ Process files song song
- ✅ Thread-safe progress reporting
- ✅ Handle errors per file
- ✅ Update stats correctly
- ✅ Add data to batch accumulator

---

## PHASE 4: Integration & Main Flow
**Milestone**: Toàn bộ flow hoạt động end-to-end với multi-threading

**Thời gian ước tính**: 1 ngày

**Phụ thuộc**: Phase 3

**Critical Path**: ✅ Có (final integration)

---

### Task 4.1: Modify XMLToDBExtractor.__init__ to Accept Thread Parameters
**Task ID**: T4.1  
**Complexity**: Easy  
**Dependencies**: T1.1  
**Critical Path**: Yes

#### Mô tả
Sửa `__init__` để nhận thread count và batch size parameters.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Sửa `__init__` method (dòng 650-666)**:
```python
def __init__(self, db_config: Dict[str, Any], xml_directory: str, 
             limit: Optional[int] = None, num_threads: int = None, 
             batch_size: int = 1000):
    """
    Initialize XML to DB extractor.
    
    Args:
        db_config: Database configuration
        xml_directory: Root directory containing XML files
        limit: Optional limit on number of files to process
        num_threads: Number of worker threads (default: min(32, cpu_count + 4))
        batch_size: Batch size for accumulation (default: 1000)
    """
    self.db_config = db_config
    self.xml_directory = xml_directory
    self.limit = limit
    self.num_threads = num_threads
    self.batch_size = batch_size
    self.db_inserter = None
    self.connection_pool = None
    self.stats = ThreadSafeStats()  # Changed from dict
```

#### Mục đích
Cho phép config số threads và batch size khi khởi tạo extractor.

#### Quan hệ với components khác
- Sử dụng: `ThreadSafeStats` thay vì dict
- Parameters: Được truyền từ `main()` function

#### Acceptance Criteria
- ✅ Nhận num_threads và batch_size
- ✅ Khởi tạo ThreadSafeStats
- ✅ Backward compatible (default values)

---

### Task 4.2: Modify setup_database to Create Connection Pool
**Task ID**: T4.2  
**Complexity**: Medium  
**Dependencies**: T1.3, T4.1  
**Critical Path**: ✅ Yes

#### Mô tả
Sửa `setup_database` để tạo connection pool thay vì single connection.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Sửa `setup_database` method (dòng 668-681)**:
```python
def setup_database(self) -> bool:
    """
    Setup database connection pool and create tables.
    
    Returns:
        True if setup successful, False otherwise
    """
    # Calculate pool size
    pool_size = self.num_threads + 2 if self.num_threads else 10
    
    # Create connection pool
    self.connection_pool = DatabaseConnectionPool(self.db_config, pool_size)
    
    # Get a connection for schema setup
    setup_conn = self.connection_pool.get_connection()
    if not setup_conn:
        print("Failed to get connection from pool")
        return False
    
    try:
        setup_cursor = setup_conn.cursor()
        DatabaseSchema.create_tables(setup_cursor)
        setup_conn.commit()
        setup_cursor.close()
        print("OK Database schema created/verified successfully")
        return True
    except Exception as e:
        print(f"Error setting up database: {e}")
        setup_conn.rollback()
        return False
    finally:
        self.connection_pool.return_connection(setup_conn)
```

#### Mục đích
Tạo connection pool và setup database schema sử dụng connection từ pool.

#### Quan hệ với components khác
- Tạo: `DatabaseConnectionPool`
- Sử dụng: `DatabaseSchema.create_tables()`
- Sử dụng bởi: `run()` method

#### Acceptance Criteria
- ✅ Tạo connection pool với đúng size
- ✅ Setup schema thành công
- ✅ Return connection về pool
- ✅ Handle errors gracefully

---

### Task 4.3: Rewrite run() Method with Multi-Threading
**Task ID**: T4.3  
**Complexity**: Hard  
**Dependencies**: T3.3, T4.1, T4.2, T2.3  
**Critical Path**: ✅ Yes

#### Mô tả
Viết lại method `run()` để sử dụng multi-threading cho scanning và processing.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Thay thế toàn bộ `run()` method (dòng 724-791)**:
```python
def run(self) -> Dict[str, int]:
    """
    Main execution method with multi-threading support.
    
    Returns:
        Dictionary of statistics
    """
    print("=" * 60)
    print("XML to PostgreSQL Extraction Tool (Multi-Threaded)")
    print("=" * 60)
    print(f"Threads: {self.num_threads}, Batch Size: {self.batch_size}\n")
    
    # Setup database
    if not self.setup_database():
        print("Failed to setup database. Exiting.")
        return self.stats.get_stats()
    
    # Find XML directories (parallel)
    scanner = FileScanner(self.xml_directory)
    xml_dirs = scanner.find_xml_directories_parallel(max_workers=self.num_threads)
    
    if not xml_dirs:
        print("No 'xml' directories found. Exiting.")
        self.connection_pool.close_all()
        return self.stats.get_stats()
    
    # Collect all XML files
    all_xml_files = []
    for parent_name, xml_dir in xml_dirs:
        xml_files = FileScanner.get_xml_files_in_directory(xml_dir)
        all_xml_files.extend(xml_files)
    
    print(f"Total XML files found: {len(all_xml_files)}\n")
    
    if not all_xml_files:
        print("No XML files found. Exiting.")
        self.connection_pool.close_all()
        return self.stats.get_stats()
    
    # Apply limit if specified
    if self.limit and self.limit > 0:
        all_xml_files = all_xml_files[:self.limit]
        print(f"[!] TEST MODE: Processing only {len(all_xml_files)} files\n")
    
    # Create batch accumulator
    batch_accumulator = BatchAccumulator(batch_size=self.batch_size)
    batch_inserter = BatchInserter(self.connection_pool)
    print_lock = threading.Lock()
    
    # Set flush callback
    def flush_callback(table_name: str, batch: List[Dict[str, Any]]):
        """Callback when batch is full"""
        batches = {table_name: batch}
        batch_inserter.insert_batches_ordered(batches, self.stats)
    
    batch_accumulator.set_flush_callback(flush_callback)
    
    # Process files in parallel
    print("Processing XML files...")
    print("-" * 60)
    
    self.process_files_parallel(
        all_xml_files,
        batch_accumulator,
        max_workers=self.num_threads,
        print_lock=print_lock
    )
    
    # Flush remaining batches
    print("\nFlushing remaining batches...")
    remaining_batches = batch_accumulator.flush()
    if remaining_batches:
        batch_inserter.insert_batches_ordered(remaining_batches, self.stats)
    
    # Cleanup
    self.connection_pool.close_all()
    
    # Print summary
    self.print_summary()
    
    return self.stats.get_stats()
```

#### Mục đích
Tích hợp tất cả components để chạy end-to-end với multi-threading.

#### Quan hệ với components khác
- Sử dụng: Tất cả components từ Phase 1-3
- Orchestrates: Scanning, processing, batch insertion
- Main entry point: Cho multi-threaded execution

#### Acceptance Criteria
- ✅ Scan directories song song
- ✅ Process files song song
- ✅ Batch accumulation hoạt động
- ✅ Auto-flush khi batch đầy
- ✅ Flush remaining batches
- ✅ Close connections properly
- ✅ Print summary correctly

---

### Task 4.4: Update print_summary to Use ThreadSafeStats
**Task ID**: T4.4  
**Complexity**: Easy  
**Dependencies**: T1.2, T4.1  
**Critical Path**: No

#### Mô tả
Sửa `print_summary` để sử dụng `ThreadSafeStats.get_stats()`.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Sửa `print_summary` method (dòng 793-805)**:
```python
def print_summary(self):
    """Print processing summary"""
    stats_dict = self.stats.get_stats()
    
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Files processed:      {stats_dict['files_processed']}")
    print(f"Files failed:         {stats_dict['files_failed']}")
    print(f"\nData inserted:")
    print(f"  ThuaDat:            {stats_dict['thuadat_inserted']} (skipped: {stats_dict['thuadat_skipped']})")
    print(f"  CaNhan:             {stats_dict['canhan_inserted']} (skipped: {stats_dict['canhan_skipped']})")
    print(f"  GiayChungNhan:      {stats_dict['giaychungnhan_inserted']} (skipped: {stats_dict['giaychungnhan_skipped']})")
    print(f"  HoSo:               {stats_dict['hoso_inserted']} (skipped: {stats_dict['hoso_skipped']})")
    print("=" * 60)
```

#### Mục đích
Update summary printing để sử dụng ThreadSafeStats.

#### Quan hệ với components khác
- Sử dụng: `ThreadSafeStats.get_stats()`
- Được gọi bởi: `run()` method

#### Acceptance Criteria
- ✅ Print đúng tất cả statistics
- ✅ Format giống như trước
- ✅ Không có errors

---

### Task 4.5: Update main() Function to Pass Thread Parameters
**Task ID**: T4.5  
**Complexity**: Easy  
**Dependencies**: T1.1, T4.1  
**Critical Path**: Yes

#### Mô tả
Sửa `main()` để truyền thread parameters vào `XMLToDBExtractor`.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Sửa dòng 868**:
```python
# Run extraction
extractor = XMLToDBExtractor(
    db_config, 
    args.xml_dir, 
    limit=args.limit,
    num_threads=args.threads,
    batch_size=args.batch_size
)
```

#### Mục đích
Truyền thread và batch size parameters từ command line vào extractor.

#### Quan hệ với components khác
- Sử dụng: `args.threads`, `args.batch_size` từ Task 1.1
- Truyền vào: `XMLToDBExtractor.__init__()`

#### Acceptance Criteria
- ✅ Truyền đúng parameters
- ✅ Default values hoạt động
- ✅ Command line arguments hoạt động

---

## PHASE 5: Error Handling & Optimization
**Milestone**: Robust error handling, graceful shutdown, performance optimization

**Thời gian ước tính**: 1 ngày

**Phụ thuộc**: Phase 4

**Critical Path**: No (enhancement)

---

### Task 5.1: Add Graceful Shutdown Handling
**Task ID**: T5.1  
**Complexity**: Medium  
**Dependencies**: T4.3  
**Critical Path**: No

#### Mô tả
Thêm xử lý KeyboardInterrupt để shutdown gracefully.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Sửa `run()` method - wrap main logic trong try-except (trong Task 4.3)**:
```python
def run(self) -> Dict[str, int]:
    """... existing docstring ..."""
    try:
        # ... existing setup code ...
        
        # Process files in parallel
        print("Processing XML files...")
        print("-" * 60)
        
        self.process_files_parallel(
            all_xml_files,
            batch_accumulator,
            max_workers=self.num_threads,
            print_lock=print_lock
        )
        
    except KeyboardInterrupt:
        print("\n\n[!] Interrupted by user. Flushing batches and shutting down...")
        # Flush remaining batches
        remaining_batches = batch_accumulator.flush()
        if remaining_batches:
            batch_inserter.insert_batches_ordered(remaining_batches, self.stats)
    finally:
        # Cleanup
        if self.connection_pool:
            self.connection_pool.close_all()
        
        # Print summary
        self.print_summary()
    
    return self.stats.get_stats()
```

#### Mục đích
Cho phép user interrupt và vẫn flush batches trước khi exit.

#### Quan hệ với components khác
- Wraps: Main processing logic
- Sử dụng: `batch_accumulator.flush()`, `connection_pool.close_all()`

#### Acceptance Criteria
- ✅ Handle KeyboardInterrupt gracefully
- ✅ Flush batches before exit
- ✅ Close connections properly
- ✅ Print summary even on interrupt

---

### Task 5.2: Add Retry Logic for Database Operations
**Task ID**: T5.2  
**Complexity**: Medium  
**Dependencies**: T2.3  
**Critical Path**: No

#### Mô tả
Thêm retry logic cho database operations khi có connection errors.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Thêm helper function (sau imports, trước classes)**:
```python
def retry_db_operation(func, max_retries: int = 3, delay: float = 1.0):
    """
    Retry a database operation on failure.
    
    Args:
        func: Function to retry (should return tuple of (success, result))
        max_retries: Maximum number of retries
        delay: Delay between retries (seconds)
        
    Returns:
        Result from function or None if all retries failed
    """
    import time
    
    for attempt in range(max_retries):
        try:
            success, result = func()
            if success:
                return result
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))  # Exponential backoff
                continue
            else:
                print(f"Error after {max_retries} retries: {e}")
                return None
    
    return None
```

**2. Sửa `BatchInserter.insert_batch()` để sử dụng retry (trong Task 2.3)**:
```python
def insert_batch(self, table_name: str, batch: List[Dict[str, Any]], 
                 max_retries: int = 3) -> Tuple[int, int]:
    """... existing docstring ..."""
    
    def _do_insert():
        conn = self.connection_pool.get_connection()
        if not conn:
            return False, (0, len(batch))
        
        try:
            inserter = DatabaseInserter({}, connection=conn)
            # ... existing insert logic ...
            inserter.commit()
            return True, (inserted, skipped)
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return False, (0, len(batch))
        finally:
            if 'inserter' in locals():
                inserter.disconnect(return_to_pool=True)
            self.connection_pool.return_connection(conn)
    
    result = retry_db_operation(_do_insert, max_retries=max_retries)
    if result:
        return result
    return 0, len(batch)
```

#### Mục đích
Tăng reliability bằng cách retry khi có database connection errors.

#### Quan hệ với components khác
- Sử dụng bởi: `BatchInserter.insert_batch()`
- Handles: Connection pool errors, transient database errors

#### Acceptance Criteria
- ✅ Retry on connection errors
- ✅ Exponential backoff
- ✅ Return correct results after retry
- ✅ Handle permanent failures gracefully

---

### Task 5.3: Add Progress Reporting with Thread-Safe Counter
**Task ID**: T5.3  
**Complexity**: Easy  
**Dependencies**: T3.3  
**Critical Path**: No

#### Mô tả
Cải thiện progress reporting với real-time updates.

#### Chi tiết triển khai

**File**: `extract_xml_to_db.py`

**1. Sửa `process_files_parallel` để thêm progress updates (Task 3.3)**:
```python
def process_files_parallel(self, xml_files: List[str], 
                          batch_accumulator: BatchAccumulator,
                          max_workers: int,
                          print_lock: threading.Lock = None) -> None:
    """... existing docstring ..."""
    
    total_files = len(xml_files)
    progress_counter = {'processed': 0, 'failed': 0}
    progress_lock = threading.Lock()
    
    def safe_print(*args, **kwargs):
        """Thread-safe print function"""
        if print_lock:
            with print_lock:
                print(*args, **kwargs)
        else:
            print(*args, **kwargs)
    
    def update_progress(success: bool):
        """Thread-safe progress update"""
        with progress_lock:
            if success:
                progress_counter['processed'] += 1
            else:
                progress_counter['failed'] += 1
            
            current = progress_counter['processed'] + progress_counter['failed']
            if current % 10 == 0 or current == total_files:
                safe_print(f"  Progress: {current}/{total_files} files "
                          f"({progress_counter['processed']} OK, "
                          f"{progress_counter['failed']} failed)")
    
    # ... existing ThreadPoolExecutor code ...
    
    for future in as_completed(future_to_file):
        xml_file = future_to_file[future]
        file_name = os.path.basename(xml_file)
        
        try:
            success, error_msg = future.result()
            update_progress(success)
            
            # ... existing print logic ...
```

#### Mục đích
Cung cấp real-time progress updates cho user.

#### Quan hệ với components khác
- Enhances: `process_files_parallel()` method
- Uses: Thread-safe counters

#### Acceptance Criteria
- ✅ Progress updates mỗi 10 files
- ✅ Thread-safe
- ✅ Hiển thị processed và failed counts

---

## PHASE 6: Testing & Validation
**Milestone**: Code tested, validated, ready for production

**Thời gian ước tính**: 1 ngày

**Phụ thuộc**: Phase 5

**Critical Path**: No (but recommended)

---

### Task 6.1: Create Unit Tests for Thread-Safe Classes
**Task ID**: T6.1  
**Complexity**: Medium  
**Dependencies**: T1.2, T1.3, T2.1  
**Critical Path**: No

#### Mô tả
Tạo unit tests cho các thread-safe classes.

#### Chi tiết triển khai

**File**: `test_multithreading.py` (new file)

**1. Test ThreadSafeStats**:
```python
import unittest
import threading
from extract_xml_to_db import ThreadSafeStats

class TestThreadSafeStats(unittest.TestCase):
    def test_concurrent_increment(self):
        stats = ThreadSafeStats()
        num_threads = 10
        increments_per_thread = 100
        
        def increment_worker():
            for _ in range(increments_per_thread):
                stats.increment('files_processed')
        
        threads = [threading.Thread(target=increment_worker) 
                  for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        self.assertEqual(stats.get('files_processed'), 
                        num_threads * increments_per_thread)
```

**2. Test DatabaseConnectionPool**:
```python
# Test connection pool with mock database
# Test get/return connections
# Test pool exhaustion
# Test closed connection handling
```

**3. Test BatchAccumulator**:
```python
# Test concurrent add_data
# Test auto-flush
# Test flush callback
# Test thread-safety
```

#### Mục đích
Đảm bảo thread-safe classes hoạt động đúng trong concurrent environment.

#### Acceptance Criteria
- ✅ All tests pass
- ✅ No race conditions detected
- ✅ Thread-safety verified

---

### Task 6.2: Create Integration Test
**Task ID**: T6.2  
**Complexity**: Medium  
**Dependencies**: Phase 4  
**Critical Path**: No

#### Mô tả
Tạo integration test với small dataset.

#### Chi tiết triển khai

**File**: `test_integration.py` (new file)

```python
import unittest
import tempfile
import os
from extract_xml_to_db import XMLToDBExtractor

class TestIntegration(unittest.TestCase):
    def test_small_dataset(self):
        # Create test XML files
        # Create test database
        # Run extractor with 2 threads
        # Verify data integrity
        # Check foreign key constraints
        pass
```

#### Acceptance Criteria
- ✅ Processes all files correctly
- ✅ Data integrity maintained
- ✅ Foreign keys valid
- ✅ Statistics accurate

---

### Task 6.3: Performance Benchmark
**Task ID**: T6.3  
**Complexity**: Easy  
**Dependencies**: Phase 4  
**Critical Path**: No

#### Mô tả
Tạo benchmark script để so sánh single-threaded vs multi-threaded.

#### Chi tiết triển khai

**File**: `benchmark.py` (new file)

```python
import time
from extract_xml_to_db import XMLToDBExtractor

def benchmark():
    # Test với 100 files
    # Single-threaded: time measurement
    # Multi-threaded (4 threads): time measurement
    # Multi-threaded (8 threads): time measurement
    # Compare results
    pass
```

#### Acceptance Criteria
- ✅ Multi-threaded faster than single-threaded
- ✅ Performance scales with thread count (up to optimal point)
- ✅ Results documented

---

## Tổng Kết

### Dependencies Graph

```
Phase 1 (Foundation)
├── T1.1 (Imports/Args) → T1.2, T1.3
├── T1.2 (ThreadSafeStats)
└── T1.3 (ConnectionPool)

Phase 2 (Batch & DB)
├── T2.1 (BatchAccumulator) → T1.1, T1.2
├── T2.2 (Modify DatabaseInserter) → T1.3
└── T2.3 (BatchInserter) → T1.3, T2.1, T2.2

Phase 3 (File Processing)
├── T3.1 (Worker Function) → T2.1, T1.2
├── T3.2 (Parallel Scanning) → T1.1
└── T3.3 (process_files_parallel) → T3.1, T2.1, T1.2

Phase 4 (Integration)
├── T4.1 (Modify __init__) → T1.1
├── T4.2 (Modify setup_database) → T1.3, T4.1
├── T4.3 (Rewrite run()) → T3.3, T4.1, T4.2, T2.3
├── T4.4 (Update print_summary) → T1.2, T4.1
└── T4.5 (Update main()) → T1.1, T4.1

Phase 5 (Error Handling)
├── T5.1 (Graceful Shutdown) → T4.3
├── T5.2 (Retry Logic) → T2.3
└── T5.3 (Progress Reporting) → T3.3

Phase 6 (Testing)
├── T6.1 (Unit Tests) → T1.2, T1.3, T2.1
├── T6.2 (Integration Test) → Phase 4
└── T6.3 (Benchmark) → Phase 4
```

### Critical Path

**T1.1 → T1.2 → T1.3 → T2.1 → T2.2 → T2.3 → T3.1 → T3.3 → T4.1 → T4.2 → T4.3 → T4.5**

### Risk Areas

1. **Database Connection Pool**: Phức tạp, cần test kỹ
2. **Batch Accumulation**: Thread-safety critical
3. **Foreign Key Ordering**: Phải insert đúng thứ tự
4. **Error Handling**: Mỗi thread phải handle errors độc lập

### Success Metrics

- ✅ Throughput tăng ít nhất 2-3x với 4-8 threads
- ✅ Không có data loss
- ✅ Foreign key constraints được respect
- ✅ Memory usage ổn định
- ✅ Error rate < 1%

