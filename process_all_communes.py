#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script: Xử lý tất cả các xã và tạo báo cáo về file PDF
"""

import os
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
import csv

# Thiết lập encoding UTF-8 cho console Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

def get_text(element, tag_name):
    """Safely get text from XML element"""
    elem = element.find(f'.//{tag_name}')
    return elem.text if elem is not None and elem.text else None

def extract_pdf_info_from_xml(xml_dir):
    """Lấy thông tin PDF từ XML files"""
    pdf_info = []  # Danh sách (xml_file, pdf_url, pdf_name)
    pdf_names = set()
    xml_to_pdf = defaultdict(list)
    
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
                            pdf_name = url.split('/')[-1] if '/' in url else url
                            if pdf_name.lower().endswith('.pdf'):
                                pdf_info.append({
                                    'xml_file': xml_path,
                                    'url': url,
                                    'pdf_name': pdf_name
                                })
                                pdf_names.add(pdf_name)
                                xml_to_pdf[pdf_name].append(xml_path)
                
                except Exception as e:
                    pass
    
    return pdf_info, pdf_names, xml_to_pdf, xml_count

def find_pdf_files_in_directory(base_dir):
    """Tìm tất cả file PDF trong thư mục"""
    pdf_files = defaultdict(list)  # tên_file -> danh sách đường dẫn
    file_count = 0
    
    if not os.path.exists(base_dir):
        return pdf_files, file_count
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith('.pdf'):
                file_count += 1
                full_path = os.path.join(root, file)
                pdf_files[file].append(full_path)
    
    return pdf_files, file_count

def process_commune(commune_dir):
    """Xử lý một xã"""
    commune_name = os.path.basename(commune_dir)
    xml_dir = os.path.join(commune_dir, 'xml')
    ho_so_dir = os.path.join(commune_dir, 'ho-so-quet')
    
    result = {
        'commune_name': commune_name,
        'xml_count': 0,
        'pdf_from_xml_count': 0,
        'pdf_in_ho_so_count': 0,
        'matched_count': 0,
        'unmatched_from_xml': [],
        'unmatched_in_ho_so': [],
        'pdf_info': [],
        'pdf_names_from_xml': set(),
        'pdf_files_in_ho_so': {},
        'matched_files': []
    }
    
    # Kiểm tra thư mục tồn tại
    if not os.path.exists(xml_dir):
        return result
    
    # Bước 1: Lấy thông tin PDF từ XML
    pdf_info, pdf_names, xml_to_pdf, xml_count = extract_pdf_info_from_xml(xml_dir)
    result['xml_count'] = xml_count
    result['pdf_from_xml_count'] = len(pdf_names)
    result['pdf_info'] = pdf_info
    result['pdf_names_from_xml'] = pdf_names
    
    # Bước 2: Tìm file PDF trong ho-so-quet
    if os.path.exists(ho_so_dir):
        pdf_files, file_count = find_pdf_files_in_directory(ho_so_dir)
        result['pdf_in_ho_so_count'] = file_count
        result['pdf_files_in_ho_so'] = pdf_files
        
        # Bước 3: Tìm file trùng khớp
        matched = []
        for pdf_name in pdf_names:
            if pdf_name in pdf_files:
                matched.append(pdf_name)
                result['matched_files'].append({
                    'pdf_name': pdf_name,
                    'xml_files': xml_to_pdf[pdf_name],
                    'ho_so_paths': pdf_files[pdf_name]
                })
        
        result['matched_count'] = len(matched)
        
        # File không khớp từ XML
        result['unmatched_from_xml'] = list(pdf_names - set(pdf_files.keys()))
        
        # File không khớp trong ho-so-quet
        result['unmatched_in_ho_so'] = list(set(pdf_files.keys()) - pdf_names)
    
    return result

def generate_report(all_results, output_dir):
    """Tạo báo cáo tổng hợp"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Báo cáo tổng quát
    summary_file = os.path.join(output_dir, f'bao_cao_tong_hop_{timestamp}.txt')
    
    # Báo cáo chi tiết CSV
    detail_file = os.path.join(output_dir, f'bao_cao_chi_tiet_{timestamp}.csv')
    
    # Báo cáo file không khớp
    unmatched_file = os.path.join(output_dir, f'file_khong_khop_{timestamp}.csv')
    
    # 1. Báo cáo tổng quát
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("=" * 100 + "\n")
        f.write("BÁO CÁO TỔNG HỢP - XỬ LÝ FILE PDF TẤT CẢ CÁC XÃ\n")
        f.write(f"Thời gian tạo: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 100 + "\n\n")
        
        total_communes = len(all_results)
        total_xml = sum(r['xml_count'] for r in all_results)
        total_pdf_from_xml = sum(r['pdf_from_xml_count'] for r in all_results)
        total_pdf_in_ho_so = sum(r['pdf_in_ho_so_count'] for r in all_results)
        total_matched = sum(r['matched_count'] for r in all_results)
        
        f.write(f"📊 THỐNG KÊ TỔNG HỢP:\n")
        f.write(f"   Số xã xử lý:              {total_communes}\n")
        f.write(f"   Tổng file XML:            {total_xml:,}\n")
        f.write(f"   Tổng PDF từ XML:          {total_pdf_from_xml:,}\n")
        f.write(f"   Tổng PDF trong ho-so-quet: {total_pdf_in_ho_so:,}\n")
        f.write(f"   Tổng file khớp:           {total_matched:,} ✅\n")
        if total_pdf_from_xml > 0:
            f.write(f"   Tỷ lệ khớp:               {total_matched/total_pdf_from_xml*100:.1f}%\n")
        f.write("\n")
        
        f.write("=" * 100 + "\n")
        f.write("CHI TIẾT TỪNG XÃ:\n")
        f.write("=" * 100 + "\n\n")
        
        for idx, result in enumerate(all_results, 1):
            f.write(f"[{idx}] XÃ: {result['commune_name']}\n")
            f.write(f"     File XML:            {result['xml_count']}\n")
            f.write(f"     PDF từ XML:          {result['pdf_from_xml_count']}\n")
            f.write(f"     PDF trong ho-so-quet: {result['pdf_in_ho_so_count']}\n")
            f.write(f"     File khớp:           {result['matched_count']} ✅\n")
            if result['pdf_from_xml_count'] > 0:
                match_rate = result['matched_count']/result['pdf_from_xml_count']*100
                f.write(f"     Tỷ lệ khớp:          {match_rate:.1f}%\n")
            f.write("\n")
    
    # 2. Báo cáo chi tiết CSV
    with open(detail_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Xã', 'Tên PDF', 'URL trong XML', 'File XML', 'Đường dẫn ho-so-quet', 'Trạng thái'])
        
        for result in all_results:
            commune_name = result['commune_name']
            
            # Ghi file khớp
            for matched in result['matched_files']:
                pdf_name = matched['pdf_name']
                for xml_file in matched['xml_files']:
                    for ho_so_path in matched['ho_so_paths']:
                        # Tìm URL từ pdf_info
                        url = ''
                        for info in result['pdf_info']:
                            if info['pdf_name'] == pdf_name and info['xml_file'] == xml_file:
                                url = info['url']
                                break
                        
                        writer.writerow([
                            commune_name,
                            pdf_name,
                            url,
                            xml_file,
                            ho_so_path,
                            'Khớp ✅'
                        ])
    
    # 3. Báo cáo file không khớp
    with open(unmatched_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Xã', 'Tên PDF', 'URL trong XML', 'File XML', 'Loại', 'Ghi chú'])
        
        for result in all_results:
            commune_name = result['commune_name']
            
            # File từ XML không tìm thấy trong ho-so-quet
            for pdf_name in result['unmatched_from_xml']:
                for info in result['pdf_info']:
                    if info['pdf_name'] == pdf_name:
                        writer.writerow([
                            commune_name,
                            pdf_name,
                            info['url'],
                            info['xml_file'],
                            'Không tìm thấy trong ho-so-quet',
                            'PDF được tham chiếu trong XML nhưng không có trong thư mục ho-so-quet'
                        ])
            
            # File trong ho-so-quet không được tham chiếu trong XML
            for pdf_name in result['unmatched_in_ho_so']:
                if pdf_name in result['pdf_files_in_ho_so']:
                    for path in result['pdf_files_in_ho_so'][pdf_name]:
                        writer.writerow([
                            commune_name,
                            pdf_name,
                            '',
                            '',
                            'Không được tham chiếu trong XML',
                            f'PDF tồn tại trong ho-so-quet tại: {path}'
                        ])
    
    return summary_file, detail_file, unmatched_file

def main():
    base_dir = r"G:\So lieu day 04.11"
    
    print("=" * 100)
    print("XỬ LÝ TẤT CẢ CÁC XÃ - TẠO BÁO CÁO FILE PDF")
    print("=" * 100)
    print()
    
    if not os.path.exists(base_dir):
        print(f"❌ Thư mục không tồn tại: {base_dir}")
        return
    
    # Tìm tất cả các xã (thư mục con)
    print("🔍 Đang quét các xã...")
    communes = []
    for item in os.listdir(base_dir):
        item_path = os.path.join(base_dir, item)
        if os.path.isdir(item_path):
            # Kiểm tra có thư mục xml không
            xml_dir = os.path.join(item_path, 'xml')
            if os.path.exists(xml_dir):
                communes.append(item_path)
    
    print(f"✅ Tìm thấy {len(communes)} xã\n")
    
    if not communes:
        print("❌ Không tìm thấy xã nào có thư mục xml")
        return
    
    # Xử lý từng xã
    all_results = []
    for idx, commune_dir in enumerate(communes, 1):
        commune_name = os.path.basename(commune_dir)
        print(f"[{idx}/{len(communes)}] Đang xử lý: {commune_name}...")
        
        result = process_commune(commune_dir)
        all_results.append(result)
        
        print(f"   ✅ XML: {result['xml_count']}, "
              f"PDF từ XML: {result['pdf_from_xml_count']}, "
              f"PDF trong ho-so: {result['pdf_in_ho_so_count']}, "
              f"Khớp: {result['matched_count']}")
    
    print()
    print("=" * 100)
    print("TẠO BÁO CÁO...")
    print("=" * 100)
    
    # Tạo thư mục báo cáo
    output_dir = os.path.join(os.getcwd(), 'bao_cao_pdf')
    os.makedirs(output_dir, exist_ok=True)
    
    summary_file, detail_file, unmatched_file = generate_report(all_results, output_dir)
    
    print(f"\n✅ Đã tạo các báo cáo:")
    print(f"   📄 Báo cáo tổng hợp:     {summary_file}")
    print(f"   📊 Báo cáo chi tiết:     {detail_file}")
    print(f"   ⚠️  File không khớp:      {unmatched_file}")
    print()
    
    # Hiển thị tóm tắt
    print("=" * 100)
    print("TÓM TẮT")
    print("=" * 100)
    total_communes = len(all_results)
    total_xml = sum(r['xml_count'] for r in all_results)
    total_pdf_from_xml = sum(r['pdf_from_xml_count'] for r in all_results)
    total_pdf_in_ho_so = sum(r['pdf_in_ho_so_count'] for r in all_results)
    total_matched = sum(r['matched_count'] for r in all_results)
    
    print(f"Số xã xử lý:              {total_communes}")
    print(f"Tổng file XML:            {total_xml:,}")
    print(f"Tổng PDF từ XML:          {total_pdf_from_xml:,}")
    print(f"Tổng PDF trong ho-so-quet: {total_pdf_in_ho_so:,}")
    print(f"Tổng file khớp:           {total_matched:,} ✅")
    if total_pdf_from_xml > 0:
        print(f"Tỷ lệ khớp:               {total_matched/total_pdf_from_xml*100:.1f}%")
    print("=" * 100)

if __name__ == "__main__":
    main()

