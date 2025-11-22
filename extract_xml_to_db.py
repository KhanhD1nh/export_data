#!/usr/bin/env python3
"""
Extract data from XML cadastral files to PostgreSQL database.
Scans only directories named 'xml' and processes all XML files within them.
Populates 4 tables with enhanced fields for cross-referencing:
- thuadat: Land parcel information (with status, validity)
- canhan: Individual person information (with gender, ID number)
- giaychungnhan: Certificate information (with barcode, signer)
- hoso: Document/file information (with archive code, location)
"""

import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict, Any, Optional, Tuple
import os
import sys
from datetime import datetime
from pathlib import Path
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


class ThreadSafeStats:
    """Thread-safe statistics tracking"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self.stats = {
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
    
    def increment(self, key: str, value: int = 1):
        """Thread-safe increment"""
        with self._lock:
            if key in self.stats:
                self.stats[key] += value
    
    def add_stats(self, stats_dict: Dict[str, int]):
        """Thread-safe add multiple stats"""
        with self._lock:
            for key, value in stats_dict.items():
                if key in self.stats:
                    self.stats[key] += value
    
    def get_stats(self) -> Dict[str, int]:
        """Get a copy of current stats"""
        with self._lock:
            return self.stats.copy()
    
    def reset(self):
        """Reset all stats"""
        with self._lock:
            for key in self.stats:
                self.stats[key] = 0


class DatabaseSchema:
    """Handles database schema creation and management"""
    
    @staticmethod
    def create_tables(cursor) -> None:
        """Create all required tables if they don't exist"""
        
        # Table: thuadat
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS thuadat (
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
                phanLoaiDuLieu INT,
                trangThaiDangKy INT,
                hieuLuc BOOLEAN DEFAULT true,
                phienBan INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Table: canhan
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS canhan (
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
                gioiTinh INT,
                soGiayTo VARCHAR(50),
                loaiGiayToTuyThan INT,
                phienBan INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Table: giaychungnhan
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS giaychungnhan (
                giayChungNhanID VARCHAR(255) PRIMARY KEY,
                soVaoSo VARCHAR(100),
                soPhatHanh VARCHAR(100),
                MaGiayChungNhan VARCHAR(255),
                ngayCap TIMESTAMP,
                maVach VARCHAR(50),
                nguoiKy VARCHAR(255),
                soVaoSoCu VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Table: hoso
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hoso (
                thanhPhanHoSoID VARCHAR(255) PRIMARY KEY,
                hoSoDangKySoID VARCHAR(255),
                giayChungNhanID VARCHAR(255),
                loaiGiayTo VARCHAR(255),
                tepTin VARCHAR(255),
                url TEXT,
                maHoSoLuuTru VARCHAR(100),
                maDVHCXa VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("OK Database schema created/verified successfully")
        
        # Add foreign key constraints (if not already exist)
        # Check and add foreign key for thuadat.voID -> canhan.caNhanID
        cursor.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = 'fk_thuadat_vo'
                ) THEN
                    ALTER TABLE thuadat 
                    ADD CONSTRAINT fk_thuadat_vo 
                    FOREIGN KEY (voID) REFERENCES canhan(caNhanID) 
                    ON DELETE SET NULL;
                END IF;
            END $$;
        """)
        
        # Check and add foreign key for thuadat.chongID -> canhan.caNhanID
        cursor.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = 'fk_thuadat_chong'
                ) THEN
                    ALTER TABLE thuadat 
                    ADD CONSTRAINT fk_thuadat_chong 
                    FOREIGN KEY (chongID) REFERENCES canhan(caNhanID) 
                    ON DELETE SET NULL;
                END IF;
            END $$;
        """)
        
        # Check and add foreign key for hoso.giayChungNhanID -> giaychungnhan.giayChungNhanID
        cursor.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = 'fk_hoso_giaychungnhan'
                ) THEN
                    ALTER TABLE hoso 
                    ADD CONSTRAINT fk_hoso_giaychungnhan 
                    FOREIGN KEY (giayChungNhanID) REFERENCES giaychungnhan(giayChungNhanID) 
                    ON DELETE CASCADE;
                END IF;
            END $$;
        """)
        
        print("OK Foreign key constraints created/verified")
        
        # Create indexes for better query performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_thuadat_madv ON thuadat(maDVHCXa);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_thuadat_vochong ON thuadat(voChongID);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_thuadat_vo ON thuadat(voID);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_thuadat_chong ON thuadat(chongID);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_canhan_hoten ON canhan(hoTen);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_canhan_namsinh ON canhan(namSinh);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_giaychungnhan_sovat ON giaychungnhan(soVaoSo);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_giaychungnhan_ngaycap ON giaychungnhan(ngayCap);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_giaychungnhan_mavach ON giaychungnhan(maVach);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_hoso_gcn ON hoso(giayChungNhanID);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_hoso_loaigiayto ON hoso(loaiGiayTo);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_hoso_mahoso ON hoso(maHoSoLuuTru);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_canhan_sogiayto ON canhan(soGiayTo);
        """)
        
        print("OK Indexes created/verified")


class XMLParser:
    """Parse XML files and extract data for database insertion"""
    
    def __init__(self, xml_file_path: str):
        self.xml_file_path = xml_file_path
        self.tree = None
        self.root = None
        self.namespace = {'gml': 'http://www.opengis.net/gml1'}
        
    def parse(self) -> bool:
        """Parse the XML file"""
        try:
            self.tree = ET.parse(self.xml_file_path)
            self.root = self.tree.getroot()
            return True
        except Exception as e:
            print(f"Error parsing {self.xml_file_path}: {e}")
            return False
    
    def _get_text(self, element, tag: str, default: str = None) -> Optional[str]:
        """Safely get text from an XML element"""
        if element is None:
            return default
        child = element.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return default
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to PostgreSQL compatible format"""
        if not date_str:
            return None
        try:
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y']:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[str]:
        """Parse timestamp string to PostgreSQL compatible format"""
        if not timestamp_str:
            return None
        try:
            # Try different timestamp formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                try:
                    dt = datetime.strptime(timestamp_str.strip(), fmt)
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    def _parse_boolean(self, bool_str: str) -> Optional[bool]:
        """Parse boolean string"""
        if not bool_str:
            return None
        return bool_str.strip().lower() in ['true', '1', 'yes']
    
    def extract_thuadat_data(self) -> List[Dict[str, Any]]:
        """Extract ThuaDat data with VoChong relationship"""
        thuadat_list = []
        
        # Get VoChong data first
        vo_chong_map = {}
        vo_chong_collection = self.root.find('.//VoChongCollection')
        if vo_chong_collection:
            for vo_chong in vo_chong_collection.findall('VoChong'):
                vo_chong_id = self._get_text(vo_chong, 'voChongID')
                if vo_chong_id:
                    vo_chong_map[vo_chong_id] = {
                        'voChongID': vo_chong_id,
                        'voID': self._get_text(vo_chong, 'voID'),
                        'chongID': self._get_text(vo_chong, 'chongID')
                    }
        
        # Extract ThuaDat data
        thua_dat_collection = self.root.find('.//ThuaDatCollection')
        if thua_dat_collection:
            for thua_dat in thua_dat_collection.findall('DC_ThuaDat'):
                thua_dat_id = self._get_text(thua_dat, 'thuaDatID')
                if not thua_dat_id:
                    continue
                
                # Try to find matching VoChong via QuyenSuDungDat
                vo_chong_info = {'voChongID': None, 'voID': None, 'chongID': None}
                
                # Look for QuyenSuDungDat with matching thuaDatID
                for quyen_su_dung_dat in self.root.findall('.//QuyenSuDungDat'):
                    if self._get_text(quyen_su_dung_dat, 'thuaDatID') == thua_dat_id:
                        doi_tuong_id = self._get_text(quyen_su_dung_dat, 'doiTuongID')
                        if doi_tuong_id and doi_tuong_id in vo_chong_map:
                            vo_chong_info = vo_chong_map[doi_tuong_id]
                            break
                
                data = {
                    'thuaDatID': thua_dat_id,
                    'maDVHCXa': self._get_text(thua_dat, 'maDVHCXa'),
                    'soHieuToBanDo': self._get_text(thua_dat, 'soHieuToBanDo'),
                    'soThuTuThua': self._get_text(thua_dat, 'soThuTuThua'),
                    'dienTich': self._get_text(thua_dat, 'dienTich'),
                    'dienTichPhapLy': self._get_text(thua_dat, 'dienTichPhapLy'),
                    'diaChiID': self._get_text(thua_dat, 'diaChiID'),
                    'voChongID': vo_chong_info['voChongID'],
                    'voID': vo_chong_info['voID'],
                    'chongID': vo_chong_info['chongID'],
                    'phanLoaiDuLieu': self._get_text(thua_dat, 'phanLoaiDuLieu'),
                    'trangThaiDangKy': self._get_text(thua_dat, 'trangThaiDangKy'),
                    'hieuLuc': self._parse_boolean(self._get_text(thua_dat, 'hieuLuc', 'true')),
                    'phienBan': self._get_text(thua_dat, 'phienBan')
                }
                thuadat_list.append(data)
        
        return thuadat_list
    
    def extract_canhan_data(self) -> List[Dict[str, Any]]:
        """Extract CaNhan (individual person) data with GiayToTuyThan"""
        canhan_list = []
        
        ca_nhan_collection = self.root.find('.//CaNhanCollection')
        if ca_nhan_collection:
            for ca_nhan in ca_nhan_collection.findall('CaNhan'):
                ca_nhan_id = self._get_text(ca_nhan, 'caNhanID')
                if not ca_nhan_id:
                    continue
                
                # Extract first GiayToTuyThan if available
                giay_to_info = {
                    'giayToTuyThanID': None,
                    'tenLoaiGiayToTuyThan': None,
                    'ngayCap': None,
                    'noiCap': None,
                    'maDinhDanhCaNhan': None,
                    'hieuLuc': None,
                    'soGiayTo': None,
                    'loaiGiayToTuyThan': None
                }
                
                giay_to_collection = ca_nhan.find('GiayToTuyThanCollection')
                if giay_to_collection:
                    giay_to = giay_to_collection.find('GiayToTuyThan')
                    if giay_to is not None:
                        giay_to_info = {
                            'giayToTuyThanID': self._get_text(giay_to, 'giayToTuyThanID'),
                            'tenLoaiGiayToTuyThan': self._get_text(giay_to, 'tenLoaiGiayToTuyThan'),
                            'ngayCap': self._parse_date(self._get_text(giay_to, 'ngayCap', '')),
                            'noiCap': self._get_text(giay_to, 'noiCap'),
                            'maDinhDanhCaNhan': self._get_text(giay_to, 'maDinhDanhCaNhan'),
                            'hieuLuc': self._parse_boolean(self._get_text(giay_to, 'hieuLuc', '')),
                            'soGiayTo': self._get_text(giay_to, 'soGiayTo'),
                            'loaiGiayToTuyThan': self._get_text(giay_to, 'loaiGiayToTuyThan')
                        }
                
                data = {
                    'caNhanID': ca_nhan_id,
                    'hoTen': self._get_text(ca_nhan, 'hoTen'),
                    'namSinh': self._get_text(ca_nhan, 'namSinh'),
                    'diaChiID': self._get_text(ca_nhan, 'diaChiID'),
                    'gioiTinh': self._get_text(ca_nhan, 'gioiTinh'),
                    'phienBan': self._get_text(ca_nhan, 'phienBan'),
                    **giay_to_info
                }
                canhan_list.append(data)
        
        return canhan_list
    
    def extract_giaychungnhan_data(self) -> List[Dict[str, Any]]:
        """Extract GiayChungNhan (certificate) data"""
        gcn_list = []
        
        gcn_collection = self.root.find('.//GiayChungNhanCollection')
        if gcn_collection:
            for gcn in gcn_collection.findall('GiayChungNhan'):
                gcn_id = self._get_text(gcn, 'giayChungNhanID')
                if not gcn_id:
                    continue
                
                data = {
                    'giayChungNhanID': gcn_id,
                    'soVaoSo': self._get_text(gcn, 'soVaoSo'),
                    'soPhatHanh': self._get_text(gcn, 'soPhatHanh'),
                    'MaGiayChungNhan': self._get_text(gcn, 'MaGiayChungNhan'),
                    'ngayCap': self._parse_timestamp(self._get_text(gcn, 'ngayCap', '')),
                    'maVach': self._get_text(gcn, 'maVach'),
                    'nguoiKy': self._get_text(gcn, 'nguoiKy'),
                    'soVaoSoCu': self._get_text(gcn, 'soVaoSoCu')
                }
                gcn_list.append(data)
        
        return gcn_list
    
    def extract_hoso_data(self) -> List[Dict[str, Any]]:
        """Extract HoSo (document) data with ThanhPhanHoSo"""
        hoso_list = []
        
        hoso_collection = self.root.find('.//HoSoDangKyDatDaiCollection')
        if hoso_collection:
            for hoso in hoso_collection.findall('HoSoDangKyDatDai'):
                hoso_id = self._get_text(hoso, 'hoSoDangKySoID')
                gcn_id = self._get_text(hoso, 'giayChungNhanID')
                ma_hoso_luu_tru = self._get_text(hoso, 'maHoSoLuuTru')
                ma_dvhc_xa = self._get_text(hoso, 'maDVHCXa')
                
                # Extract each ThanhPhanHoSo entry
                thanh_phan_collection = hoso.find('ThanhPhanHoSoDangKyDatDaiCollection')
                if thanh_phan_collection:
                    for thanh_phan in thanh_phan_collection.findall('ThanhPhanHoSoDangKyDatDai'):
                        thanh_phan_id = self._get_text(thanh_phan, 'thanhPhanHoSoID')
                        if not thanh_phan_id:
                            continue
                        
                        data = {
                            'thanhPhanHoSoID': thanh_phan_id,
                            'hoSoDangKySoID': hoso_id,
                            'giayChungNhanID': gcn_id,
                            'loaiGiayTo': self._get_text(thanh_phan, 'loaiGiayTo'),
                            'tepTin': self._get_text(thanh_phan, 'tepTin'),
                            'url': self._get_text(thanh_phan, 'url'),
                            'maHoSoLuuTru': ma_hoso_luu_tru,
                            'maDVHCXa': ma_dvhc_xa
                        }
                        hoso_list.append(data)
        
        return hoso_list


class DatabaseInserter:
    """Handle database insertions with batch operations"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            print(f"Database connection error: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def insert_thuadat(self, data_list: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Insert ThuaDat data with conflict handling"""
        if not data_list:
            return 0, 0
        
        insert_query = """
            INSERT INTO thuadat (
                thuaDatID, maDVHCXa, soHieuToBanDo, soThuTuThua,
                dienTich, dienTichPhapLy, diaChiID,
                voChongID, voID, chongID,
                phanLoaiDuLieu, trangThaiDangKy, hieuLuc, phienBan
            ) VALUES (
                %(thuaDatID)s, %(maDVHCXa)s, %(soHieuToBanDo)s, %(soThuTuThua)s,
                %(dienTich)s, %(dienTichPhapLy)s, %(diaChiID)s,
                %(voChongID)s, %(voID)s, %(chongID)s,
                %(phanLoaiDuLieu)s, %(trangThaiDangKy)s, %(hieuLuc)s, %(phienBan)s
            )
            ON CONFLICT (thuaDatID) DO NOTHING
        """
        
        try:
            execute_batch(self.cursor, insert_query, data_list, page_size=100)
            inserted = self.cursor.rowcount
            return inserted, len(data_list) - inserted
        except Exception as e:
            print(f"Error inserting thuadat: {e}")
            self.conn.rollback()
            return 0, len(data_list)
    
    def insert_canhan(self, data_list: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Insert CaNhan data with conflict handling"""
        if not data_list:
            return 0, 0
        
        insert_query = """
            INSERT INTO canhan (
                caNhanID, hoTen, namSinh, diaChiID,
                giayToTuyThanID, tenLoaiGiayToTuyThan, ngayCap,
                noiCap, maDinhDanhCaNhan, hieuLuc,
                gioiTinh, soGiayTo, loaiGiayToTuyThan, phienBan
            ) VALUES (
                %(caNhanID)s, %(hoTen)s, %(namSinh)s, %(diaChiID)s,
                %(giayToTuyThanID)s, %(tenLoaiGiayToTuyThan)s, %(ngayCap)s,
                %(noiCap)s, %(maDinhDanhCaNhan)s, %(hieuLuc)s,
                %(gioiTinh)s, %(soGiayTo)s, %(loaiGiayToTuyThan)s, %(phienBan)s
            )
            ON CONFLICT (caNhanID) DO NOTHING
        """
        
        try:
            execute_batch(self.cursor, insert_query, data_list, page_size=100)
            inserted = self.cursor.rowcount
            return inserted, len(data_list) - inserted
        except Exception as e:
            print(f"Error inserting canhan: {e}")
            self.conn.rollback()
            return 0, len(data_list)
    
    def insert_giaychungnhan(self, data_list: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Insert GiayChungNhan data with conflict handling"""
        if not data_list:
            return 0, 0
        
        insert_query = """
            INSERT INTO giaychungnhan (
                giayChungNhanID, soVaoSo, soPhatHanh,
                MaGiayChungNhan, ngayCap,
                maVach, nguoiKy, soVaoSoCu
            ) VALUES (
                %(giayChungNhanID)s, %(soVaoSo)s, %(soPhatHanh)s,
                %(MaGiayChungNhan)s, %(ngayCap)s,
                %(maVach)s, %(nguoiKy)s, %(soVaoSoCu)s
            )
            ON CONFLICT (giayChungNhanID) DO NOTHING
        """
        
        try:
            execute_batch(self.cursor, insert_query, data_list, page_size=100)
            inserted = self.cursor.rowcount
            return inserted, len(data_list) - inserted
        except Exception as e:
            print(f"Error inserting giaychungnhan: {e}")
            self.conn.rollback()
            return 0, len(data_list)
    
    def insert_hoso(self, data_list: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Insert HoSo data with conflict handling and foreign key validation"""
        if not data_list:
            return 0, 0
        
        # Validate giayChungNhanID: set to NULL if not exists in database
        valid_gcn_ids = set()
        unique_gcn_ids = {d.get('giayChungNhanID') for d in data_list if d.get('giayChungNhanID')}
        
        if unique_gcn_ids:
            try:
                # Check which giayChungNhanIDs exist in the database
                placeholders = ','.join(['%s'] * len(unique_gcn_ids))
                check_query = f"SELECT giayChungNhanID FROM giaychungnhan WHERE giayChungNhanID IN ({placeholders})"
                self.cursor.execute(check_query, tuple(unique_gcn_ids))
                valid_gcn_ids = {row[0] for row in self.cursor.fetchall()}
            except Exception as e:
                print(f"Warning: Could not verify giayChungNhanID existence: {e}")
                valid_gcn_ids = unique_gcn_ids
        
        # Set invalid giayChungNhanID to NULL
        for data in data_list:
            gcn_id = data.get('giayChungNhanID')
            if gcn_id and gcn_id not in valid_gcn_ids:
                data['giayChungNhanID'] = None
        
        insert_query = """
            INSERT INTO hoso (
                thanhPhanHoSoID, hoSoDangKySoID, giayChungNhanID,
                loaiGiayTo, tepTin, url,
                maHoSoLuuTru, maDVHCXa
            ) VALUES (
                %(thanhPhanHoSoID)s, %(hoSoDangKySoID)s, %(giayChungNhanID)s,
                %(loaiGiayTo)s, %(tepTin)s, %(url)s,
                %(maHoSoLuuTru)s, %(maDVHCXa)s
            )
            ON CONFLICT (thanhPhanHoSoID) DO NOTHING
        """
        
        try:
            execute_batch(self.cursor, insert_query, data_list, page_size=100)
            inserted = self.cursor.rowcount
            return inserted, len(data_list) - inserted
        except Exception as e:
            print(f"Error inserting hoso: {e}")
            self.conn.rollback()
            return 0, len(data_list)
    
    def commit(self):
        """Commit the transaction"""
        if self.conn:
            self.conn.commit()
    
    def rollback(self):
        """Rollback the transaction"""
        if self.conn:
            self.conn.rollback()


class FileScanner:
    """Scan directory for XML files in folders named 'xml' only"""
    
    def __init__(self, root_directory: str):
        self.root_directory = root_directory
        self.xml_dirs = []
    
    def find_xml_directories(self) -> List[str]:
        """Find all directories named 'xml' without scanning their contents"""
        print(f"Scanning directory: {self.root_directory}")
        print("Looking for 'xml' subdirectories...")
        self.xml_dirs = []
        
        # Optimized: Only scan 2 levels deep to find 'xml' directories
        if not os.path.exists(self.root_directory):
            print(f"Error: Directory not found: {self.root_directory}")
            return self.xml_dirs
        
        # Check immediate subdirectories for 'xml' folder
        try:
            for item in os.listdir(self.root_directory):
                item_path = os.path.join(self.root_directory, item)
                if os.path.isdir(item_path):
                    # Check if this directory has an 'xml' subdirectory
                    xml_dir = os.path.join(item_path, 'xml')
                    if os.path.exists(xml_dir) and os.path.isdir(xml_dir):
                        self.xml_dirs.append((item, xml_dir))
        except Exception as e:
            print(f"Error scanning directory: {e}")
            return self.xml_dirs
        
        print(f"Found {len(self.xml_dirs)} 'xml' directories\n")
        return self.xml_dirs
    
    @staticmethod
    def get_xml_files_in_directory(xml_dir: str) -> List[str]:
        """Get all XML files in a specific directory"""
        xml_files = []
        for root, dirs, files in os.walk(xml_dir):
            for file in files:
                if file.endswith('.xml'):
                    full_path = os.path.join(root, file)
                    xml_files.append(full_path)
        return xml_files


class XMLToDBExtractor:
    """Main orchestrator for XML to database extraction"""
    
    def __init__(self, db_config: Dict[str, Any], xml_directory: str, limit: Optional[int] = None, num_threads: int = 10):
        self.db_config = db_config
        self.xml_directory = xml_directory
        self.limit = limit
        self.num_threads = num_threads
        self.db_inserter = None
        self.stats = ThreadSafeStats()
        self.print_lock = threading.Lock()  # Lock for thread-safe printing
    
    def setup_database(self) -> bool:
        """Setup database connection and create tables"""
        self.db_inserter = DatabaseInserter(self.db_config)
        if not self.db_inserter.connect():
            return False
        
        try:
            DatabaseSchema.create_tables(self.db_inserter.cursor)
            self.db_inserter.commit()
            return True
        except Exception as e:
            print(f"Error setting up database: {e}")
            self.db_inserter.rollback()
            return False
    
    def process_single_file_worker(self, xml_file: str) -> Tuple[bool, Dict[str, int]]:
        """Worker function to process a single XML file in a thread"""
        # Create a new database connection for this thread
        thread_db_inserter = DatabaseInserter(self.db_config)
        if not thread_db_inserter.connect():
            self.stats.increment('files_failed')
            return False, {}
        
        try:
            parser = XMLParser(xml_file)
            if not parser.parse():
                thread_db_inserter.disconnect()
                self.stats.increment('files_failed')
                return False, {}
            
            # Extract all data
            thuadat_data = parser.extract_thuadat_data()
            canhan_data = parser.extract_canhan_data()
            gcn_data = parser.extract_giaychungnhan_data()
            hoso_data = parser.extract_hoso_data()
            
            # Insert data in correct order (respecting foreign key constraints)
            # 1. Insert canhan first (thuadat references this)
            cn_ins, cn_skip = thread_db_inserter.insert_canhan(canhan_data)
            # 2. Insert giaychungnhan (hoso references this)
            gcn_ins, gcn_skip = thread_db_inserter.insert_giaychungnhan(gcn_data)
            # 3. Insert thuadat (references canhan via voID, chongID)
            td_ins, td_skip = thread_db_inserter.insert_thuadat(thuadat_data)
            # 4. Insert hoso (references giaychungnhan)
            hs_ins, hs_skip = thread_db_inserter.insert_hoso(hoso_data)
            
            thread_db_inserter.commit()
            
            # Return stats for this file
            file_stats = {
                'thuadat_inserted': td_ins,
                'thuadat_skipped': td_skip,
                'canhan_inserted': cn_ins,
                'canhan_skipped': cn_skip,
                'giaychungnhan_inserted': gcn_ins,
                'giaychungnhan_skipped': gcn_skip,
                'hoso_inserted': hs_ins,
                'hoso_skipped': hs_skip
            }
            
            self.stats.increment('files_processed')
            self.stats.add_stats(file_stats)
            
            return True, file_stats
            
        except Exception as e:
            with self.print_lock:
                print(f"Error processing {xml_file}: {e}")
            thread_db_inserter.rollback()
            self.stats.increment('files_failed')
            return False, {}
        finally:
            thread_db_inserter.disconnect()
    
    def run(self) -> Dict[str, int]:
        """Main execution method - processes directories on-the-fly"""
        print("=" * 60)
        print("XML to PostgreSQL Extraction Tool")
        print(f"Using {self.num_threads} worker threads")
        print("=" * 60)
        
        # Setup database
        if not self.setup_database():
            print("Failed to setup database. Exiting.")
            return self.stats
        
        # Find XML directories
        scanner = FileScanner(self.xml_directory)
        xml_dirs = scanner.find_xml_directories()
        
        if not xml_dirs:
            print("No 'xml' directories found. Exiting.")
            return self.stats
        
        # Process each directory
        total_files = 0
        files_processed_so_far = 0
        
        for dir_idx, (parent_name, xml_dir) in enumerate(xml_dirs, 1):
            print(f"[{dir_idx}/{len(xml_dirs)}] Processing directory: {parent_name}/xml")
            print("-" * 60)
            
            # Get XML files in this directory
            xml_files = FileScanner.get_xml_files_in_directory(xml_dir)
            print(f"  Found {len(xml_files)} XML files")
            
            if not xml_files:
                print("  Skipping empty directory\n")
                continue
            
            # Apply limit if specified
            if self.limit and self.limit > 0:
                remaining = self.limit - files_processed_so_far
                if remaining <= 0:
                    print(f"\n[!] TEST MODE: Limit of {self.limit} files reached. Stopping.")
                    break
                if len(xml_files) > remaining:
                    xml_files = xml_files[:remaining]
                    print(f"  [!] TEST MODE: Processing only {remaining} files from this directory")
            
            # Process files in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                # Submit all files to the thread pool
                future_to_file = {
                    executor.submit(self.process_single_file_worker, xml_file): xml_file 
                    for xml_file in xml_files
                }
                
                # Process completed tasks as they finish
                completed = 0
                for future in as_completed(future_to_file):
                    xml_file = future_to_file[future]
                    file_name = os.path.basename(xml_file)
                    completed += 1
                    
                    try:
                        success, _ = future.result()
                        with self.print_lock:
                            status = "OK" if success else "FAILED"
                            print(f"  [{completed}/{len(xml_files)}] {file_name}... {status}")
                        if success:
                            files_processed_so_far += 1
                    except Exception as e:
                        with self.print_lock:
                            print(f"  [{completed}/{len(xml_files)}] {file_name}... FAILED: {e}")
                        self.stats.increment('files_failed')
            
            total_files += len(xml_files)
            print(f"  Completed: {parent_name}/xml\n")
        
        # Cleanup
        self.db_inserter.disconnect()
        
        # Print summary
        self.print_summary()
        
        return self.stats.get_stats()
    
    def print_summary(self):
        """Print processing summary"""
        stats = self.stats.get_stats()
        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY")
        print("=" * 60)
        print(f"Files processed:      {stats['files_processed']}")
        print(f"Files failed:         {stats['files_failed']}")
        print(f"\nData inserted:")
        print(f"  ThuaDat:            {stats['thuadat_inserted']} (skipped: {stats['thuadat_skipped']})")
        print(f"  CaNhan:             {stats['canhan_inserted']} (skipped: {stats['canhan_skipped']})")
        print(f"  GiayChungNhan:      {stats['giaychungnhan_inserted']} (skipped: {stats['giaychungnhan_skipped']})")
        print(f"  HoSo:               {stats['hoso_inserted']} (skipped: {stats['hoso_skipped']})")
        print("=" * 60)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Extract XML cadastral data to PostgreSQL database'
    )
    parser.add_argument(
        '--host',
        default=os.getenv('DB_HOST', 'localhost'),
        help='Database host (default: localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.getenv('DB_PORT', '5432')),
        help='Database port (default: 5432)'
    )
    parser.add_argument(
        '--database',
        default=os.getenv('DB_NAME', 'cadastral_db'),
        help='Database name (default: cadastral_db)'
    )
    parser.add_argument(
        '--user',
        default=os.getenv('DB_USER', 'postgres'),
        help='Database user (default: postgres)'
    )
    parser.add_argument(
        '--password',
        default=os.getenv('DB_PASSWORD', 'postgres'),
        help='Database password (default: postgres)'
    )
    parser.add_argument(
        '--xml-dir',
        default=r'G:\So lieu day 04.11',
        help='XML files directory (default: G:\So lieu day 04.11)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of files to process (for testing)'
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=10,
        help='Number of worker threads (default: 10)'
    )
    
    args = parser.parse_args()
    
    # Build database config
    db_config = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }
    
    # Check if XML directory exists
    if not os.path.exists(args.xml_dir):
        print(f"Error: XML directory not found: {args.xml_dir}")
        sys.exit(1)
    
    # Run extraction
    extractor = XMLToDBExtractor(db_config, args.xml_dir, limit=args.limit, num_threads=args.threads)
    stats = extractor.run()
    
    # Exit with error code if any files failed
    final_stats = extractor.stats.get_stats()
    if final_stats['files_failed'] > 0:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == '__main__':
    main()

