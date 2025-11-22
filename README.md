# XML to PostgreSQL Extractor

Công cụ trích xuất dữ liệu từ các file XML địa chính và import vào cơ sở dữ liệu PostgreSQL với hỗ trợ multithreading.

## Tính năng

- ✅ Quét và xử lý tự động các thư mục XML
- ✅ Multithreading với 10 luồng mặc định (có thể tùy chỉnh)
- ✅ Trích xuất dữ liệu vào 4 bảng: `thuadat`, `canhan`, `giaychungnhan`, `hoso`
- ✅ Xử lý foreign key constraints tự động
- ✅ Batch insertion với conflict handling
- ✅ Thread-safe statistics tracking

## Yêu cầu

- Python 3.7+
- PostgreSQL database
- Các thư viện trong `requirements_xml_extractor.txt`

## Cài đặt

```bash
pip install -r requirements_xml_extractor.txt
```

## Sử dụng

### Cơ bản

```bash
python extract_xml_to_db.py --xml-dir "G:\So lieu day 04.11"
```

### Với tùy chọn đầy đủ

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

### Các tham số

- `--host`: Database host (mặc định: localhost)
- `--port`: Database port (mặc định: 5432)
- `--database`: Tên database (mặc định: cadastral_db)
- `--user`: Database user (mặc định: postgres)
- `--password`: Database password (mặc định: postgres)
- `--xml-dir`: Thư mục chứa các file XML
- `--threads`: Số lượng worker threads (mặc định: 10)
- `--limit`: Giới hạn số file xử lý (dùng cho testing)

### Sử dụng biến môi trường

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=cadastral_db
export DB_USER=postgres
export DB_PASSWORD=your_password

python extract_xml_to_db.py --xml-dir "G:\So lieu day 04.11"
```

## Cấu trúc Database

Tool tự động tạo các bảng sau:

### thuadat
Thông tin thửa đất với các trường: thuaDatID, maDVHCXa, soHieuToBanDo, dienTich, voID, chongID, etc.

### canhan
Thông tin cá nhân với các trường: caNhanID, hoTen, namSinh, gioiTinh, soGiayTo, etc.

### giaychungnhan
Thông tin giấy chứng nhận với các trường: giayChungNhanID, soVaoSo, ngayCap, maVach, etc.

### hoso
Thông tin hồ sơ với các trường: thanhPhanHoSoID, hoSoDangKySoID, giayChungNhanID, loaiGiayTo, etc.

## Multithreading

Tool sử dụng `ThreadPoolExecutor` để xử lý song song nhiều file XML:
- Mỗi thread có connection database riêng
- Thread-safe statistics tracking
- Tự động commit và đóng connection sau khi xử lý xong

## License

MIT

