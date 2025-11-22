#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script đơn giản: Lấy tên file PDF từ XML và tìm trong ho-so-quet
"""

import os
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

# Thiết lập encoding UTF-8 cho console Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

def get_text(element, tag_name):
    """Safely get text from XML element"""
    elem = element.find(f'.//{tag_name}')
    return elem.text if elem is not None and elem.text else None

def extract_pdf_names_from_xml(xml_dir):
    """Lấy tất cả tên file PDF từ các file XML"""
    pdf_names = set()
    xml_to_pdf = {}  # Lưu mapping từ XML đến PDF
    
    print("🔍 Đang quét file XML...")
    xml_count = 0
    
    for root, dirs, files in os.walk(xml_dir):
        for file in files:
            if file.endswith('.xml'):
                xml_count += 1
                xml_path = os.path.join(root, file)
                
                try:
                    tree = ET.parse(xml_path)
                    xml_root = tree.getroot()
                    
                    # Tìm tất cả url trong ThanhPhanHoSoDangKyDatDai
                    for thanh_phan in xml_root.findall('.//ThanhPhanHoSoDangKyDatDai'):
                        url = get_text(thanh_phan, 'url')
                        if url:
                            # Lấy tên file (phần sau dấu / cuối cùng)
                            pdf_name = url.split('/')[-1] if '/' in url else url
                            if pdf_name.lower().endswith('.pdf'):
                                pdf_names.add(pdf_name)
                                if pdf_name not in xml_to_pdf:
                                    xml_to_pdf[pdf_name] = []
                                xml_to_pdf[pdf_name].append(xml_path)
                
                except Exception as e:
                    pass
                
                if xml_count % 100 == 0:
                    print(f"   Đã quét {xml_count} file XML, tìm thấy {len(pdf_names)} tên PDF unique...")
    
    print(f"✅ Hoàn tất! Quét {xml_count} file XML, tìm thấy {len(pdf_names)} tên PDF unique")
    return pdf_names, xml_to_pdf

def find_pdf_files_in_directory(base_dir):
    """Tìm tất cả file PDF trong thư mục"""
    pdf_files = {}  # tên_file -> danh sách đường dẫn
    
    print(f"\n🔍 Đang quét thư mục: {base_dir}")
    file_count = 0
    
    if not os.path.exists(base_dir):
        print(f"❌ Thư mục không tồn tại: {base_dir}")
        return pdf_files
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith('.pdf'):
                file_count += 1
                full_path = os.path.join(root, file)
                if file not in pdf_files:
                    pdf_files[file] = []
                pdf_files[file].append(full_path)
                
                if file_count % 100 == 0:
                    print(f"   Đã quét {file_count} file PDF...")
    
    print(f"✅ Tìm thấy {file_count} file PDF, {len(pdf_files)} tên file unique")
    return pdf_files

def main():
    xml_dir = r"G:\So lieu day 04.11\2. xa cao phong\xml"
    ho_so_dir = r"G:\So lieu day 04.11\2. xa cao phong\ho-so-quet"
    
    print("=" * 100)
    print("TÌM KIẾM FILE PDF: KHỚP TÊN FILE GIỮA XML VÀ HỒ SƠ QUÉT")
    print("=" * 100)
    print()
    
    # Bước 1: Lấy tên file PDF từ XML
    pdf_names_from_xml, xml_to_pdf = extract_pdf_names_from_xml(xml_dir)
    
    # Bước 2: Tìm file PDF trong ho-so-quet
    pdf_files_in_ho_so = find_pdf_files_in_directory(ho_so_dir)
    
    # Bước 3: Tìm file trùng khớp
    print("\n" + "=" * 100)
    print("KẾT QUẢ KHỚP TÊN FILE")
    print("=" * 100)
    
    matched = []
    for pdf_name in pdf_names_from_xml:
        if pdf_name in pdf_files_in_ho_so:
            matched.append(pdf_name)
    
    if matched:
        print(f"\n✅ TÌM THẤY {len(matched)} FILE KHỚP TÊN:")
        print()
        
        for i, pdf_name in enumerate(matched[:20], 1):  # Hiển thị 20 file đầu
            print(f"[{i}] 📄 {pdf_name}")
            print(f"    📂 Có trong {len(pdf_files_in_ho_so[pdf_name])} vị trí:")
            for path in pdf_files_in_ho_so[pdf_name][:3]:  # Hiển thị 3 vị trí đầu
                print(f"       └─ {path}")
            print(f"    📋 Được tham chiếu từ {len(xml_to_pdf[pdf_name])} file XML")
            print()
            
        if len(matched) > 20:
            print(f"    ... và {len(matched) - 20} file khớp khác")
    else:
        print("\n❌ KHÔNG TÌM THẤY FILE NÀO KHỚP TÊN")
        print()
        print("📊 Thống kê:")
        print(f"   - Tên PDF từ XML:        {len(pdf_names_from_xml)}")
        print(f"   - PDF trong ho-so-quet:  {len(pdf_files_in_ho_so)}")
        print()
        print("💡 Gợi ý:")
        print("   - Kiểm tra xem thư mục ho-so-quet có chứa file PDF không")
        print("   - Có thể tên file trong XML khác với tên file thực tế")
        print()
        
        # Hiển thị mẫu tên file từ XML
        if pdf_names_from_xml:
            print("📋 10 tên file PDF đầu tiên từ XML:")
            for i, name in enumerate(sorted(pdf_names_from_xml)[:10], 1):
                print(f"   {i}. {name}")
        
        print()
        
        # Hiển thị mẫu tên file từ ho-so-quet
        if pdf_files_in_ho_so:
            print("📁 10 tên file PDF đầu tiên trong ho-so-quet:")
            for i, name in enumerate(sorted(pdf_files_in_ho_so.keys())[:10], 1):
                print(f"   {i}. {name}")
    
    print()
    print("=" * 100)
    print("TÓM TẮT")
    print("=" * 100)
    print(f"Tên PDF từ XML:           {len(pdf_names_from_xml)}")
    print(f"PDF trong ho-so-quet:     {len(pdf_files_in_ho_so)}")
    print(f"File khớp tên:            {len(matched)} ✅")
    print(f"Tỷ lệ khớp:               {len(matched)/len(pdf_names_from_xml)*100 if pdf_names_from_xml else 0:.1f}%")
    print("=" * 100)

if __name__ == "__main__":
    main()

