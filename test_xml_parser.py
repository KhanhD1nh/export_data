#!/usr/bin/env python3
"""
Quick test script to verify XML parsing functionality without database
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extract_xml_to_db import XMLParser


def test_xml_file(xml_path: str):
    """Test parsing a single XML file"""
    print(f"Testing XML file: {xml_path}")
    print("=" * 70)
    
    # Create parser
    parser = XMLParser(xml_path)
    
    # Parse the file
    if not parser.parse():
        print("❌ Failed to parse XML file")
        return False
    
    print("✓ XML file parsed successfully\n")
    
    # Extract ThuaDat data
    print("ThuaDat Data:")
    print("-" * 70)
    thuadat_data = parser.extract_thuadat_data()
    for idx, data in enumerate(thuadat_data, 1):
        print(f"  Record {idx}:")
        for key, value in data.items():
            if value:
                print(f"    {key}: {value}")
    print(f"Total records: {len(thuadat_data)}\n")
    
    # Extract CaNhan data
    print("CaNhan Data:")
    print("-" * 70)
    canhan_data = parser.extract_canhan_data()
    for idx, data in enumerate(canhan_data, 1):
        print(f"  Record {idx}:")
        for key, value in data.items():
            if value:
                print(f"    {key}: {value}")
    print(f"Total records: {len(canhan_data)}\n")
    
    # Extract GiayChungNhan data
    print("GiayChungNhan Data:")
    print("-" * 70)
    gcn_data = parser.extract_giaychungnhan_data()
    for idx, data in enumerate(gcn_data, 1):
        print(f"  Record {idx}:")
        for key, value in data.items():
            if value:
                print(f"    {key}: {value}")
    print(f"Total records: {len(gcn_data)}\n")
    
    # Extract HoSo data
    print("HoSo Data:")
    print("-" * 70)
    hoso_data = parser.extract_hoso_data()
    for idx, data in enumerate(hoso_data, 1):
        print(f"  Record {idx}:")
        for key, value in data.items():
            if value:
                print(f"    {key}: {value}")
    print(f"Total records: {len(hoso_data)}\n")
    
    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"ThuaDat records:      {len(thuadat_data)}")
    print(f"CaNhan records:       {len(canhan_data)}")
    print(f"GiayChungNhan records: {len(gcn_data)}")
    print(f"HoSo records:         {len(hoso_data)}")
    print("=" * 70)
    
    return True


if __name__ == '__main__':
    # Test with the first XML file found in the directory
    import glob
    
    xml_dir = r"G:\So lieu day 04.11"
    
    if len(sys.argv) > 1:
        sample_xml = sys.argv[1]
    else:
        # Find first XML file in directory
        xml_files = glob.glob(os.path.join(xml_dir, "**", "*.xml"), recursive=True)
        if not xml_files:
            print(f"Error: No XML files found in: {xml_dir}")
            sys.exit(1)
        sample_xml = xml_files[0]
        print(f"Testing with first XML file found: {sample_xml}\n")
    
    if not os.path.exists(sample_xml):
        print(f"Error: XML file not found: {sample_xml}")
        sys.exit(1)
    
    success = test_xml_file(sample_xml)
    sys.exit(0 if success else 1)

