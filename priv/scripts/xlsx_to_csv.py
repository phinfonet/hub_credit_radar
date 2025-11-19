#!/usr/bin/env python3
"""
Convert XLSX to CSV with memory efficiency.

This script uses openpyxl in read-only mode to convert XLSX files to CSV
without loading the entire file into memory. It properly handles inline strings
which are commonly used in Anbima exports.

Usage:
    python3 xlsx_to_csv.py input.xlsx output.csv
"""

import sys
import csv
from openpyxl import load_workbook


def xlsx_to_csv(xlsx_path, csv_path):
    """Convert XLSX to CSV efficiently."""
    # Load workbook in read-only mode for memory efficiency
    wb = load_workbook(xlsx_path, read_only=True, data_only=False)
    ws = wb.active

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write all rows
        for row in ws.rows:
            # Extract cell values, handling both formulas and inline strings
            values = [cell.value if cell.value is not None else '' for cell in row]
            writer.writerow(values)

    wb.close()
    print(f"Converted {xlsx_path} to {csv_path}")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 xlsx_to_csv.py input.xlsx output.csv", file=sys.stderr)
        sys.exit(1)

    xlsx_path = sys.argv[1]
    csv_path = sys.argv[2]

    try:
        xlsx_to_csv(xlsx_path, csv_path)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
