#!/usr/bin/env python3
"""
Sample Data Generator for Pipeline Benchmarks

Generates realistic CSV files with sales data for testing both pipelines.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import argparse
from datetime import datetime, timedelta
import random


def parse_size(size_str: str) -> int:
    """Convert size string (e.g., '1GB', '500MB') to bytes"""
    size_str = size_str.upper().strip()
    
    multipliers = {
        'KB': 1024,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3,
    }
    
    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            number = float(size_str[:-len(suffix)])
            return int(number * multiplier)
    
    raise ValueError(f"Invalid size format: {size_str}. Use format like '1GB', '500MB', '100KB'")


def generate_sample_data(num_rows: int, start_date: datetime) -> pd.DataFrame:
    """Generate a sample dataset with realistic sales data"""
    
    # Product IDs (1000 unique products)
    product_ids = [f"PROD_{i:05d}" for i in range(1, 1001)]
    
    # Generate data
    data = {
        'date': [
            (start_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            for _ in range(num_rows)
        ],
        'product_id': np.random.choice(product_ids, num_rows),
        'quantity': np.random.randint(1, 100, num_rows),
        'price': np.round(np.random.uniform(10.0, 1000.0, num_rows), 2),
        'customer_id': [f"CUST_{random.randint(1, 10000):06d}" for _ in range(num_rows)],
        'region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], num_rows),
        'category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Books', 'Home'], num_rows),
    }
    
    df = pd.DataFrame(data)
    
    # Add some intentional data quality issues (5% of rows)
    num_bad_rows = int(num_rows * 0.05)
    bad_indices = np.random.choice(num_rows, num_bad_rows, replace=False)
    
    # Missing values
    df.loc[bad_indices[:num_bad_rows//3], 'product_id'] = None
    
    # Invalid quantities
    df.loc[bad_indices[num_bad_rows//3:2*num_bad_rows//3], 'quantity'] = -1
    
    # Invalid prices
    df.loc[bad_indices[2*num_bad_rows//3:], 'price'] = 0
    
    return df


def estimate_rows_for_size(target_bytes: int) -> int:
    """Estimate number of rows needed to reach target file size"""
    # Average row size is approximately 100 bytes in CSV format
    avg_row_size = 100
    return int(target_bytes / avg_row_size)


def generate_dataset(target_size: str, output_dir: str, num_files: int = None):
    """Generate dataset split across multiple CSV files"""
    
    print(f"\n{'='*60}")
    print(f"Generating Dataset: {target_size}")
    print(f"{'='*60}\n")
    
    # Parse target size
    target_bytes = parse_size(target_size)
    target_mb = target_bytes / (1024 ** 2)
    
    # Determine number of files based on size
    if num_files is None:
        if target_mb < 100:
            num_files = 5
        elif target_mb < 1000:
            num_files = 20
        elif target_mb < 10000:
            num_files = 50
        else:
            num_files = 200
    
    print(f"Target size: {target_mb:.2f} MB")
    print(f"Number of files: {num_files}")
    
    # Calculate rows per file
    total_rows = estimate_rows_for_size(target_bytes)
    rows_per_file = total_rows // num_files
    
    print(f"Estimated total rows: {total_rows:,}")
    print(f"Rows per file: {rows_per_file:,}\n")
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate files
    start_date = datetime(2023, 1, 1)
    total_size = 0
    
    for i in range(num_files):
        print(f"Generating file {i+1}/{num_files}...", end=' ')
        
        # Generate data
        df = generate_sample_data(rows_per_file, start_date)
        
        # Save to CSV
        file_path = output_path / f"sales_data_{i+1:04d}.csv"
        df.to_csv(file_path, index=False)
        
        # Track size
        file_size = file_path.stat().st_size
        total_size += file_size
        
        print(f"✓ ({file_size / (1024**2):.2f} MB)")
    
    print(f"\n{'='*60}")
    print(f"Dataset Generation Complete")
    print(f"{'='*60}")
    print(f"Total files: {num_files}")
    print(f"Total size: {total_size / (1024**2):.2f} MB ({total_size / (1024**3):.2f} GB)")
    print(f"Output directory: {output_path.absolute()}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample CSV data for pipeline benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 1GB of data
  python generate_data.py --size 1GB --output data/

  # Generate 100MB of data with 10 files
  python generate_data.py --size 100MB --output data/ --files 10

  # Generate 50GB of data (production scale)
  python generate_data.py --size 50GB --output data/
        """
    )
    
    parser.add_argument(
        '--size',
        required=True,
        help='Target dataset size (e.g., 100MB, 1GB, 50GB)'
    )
    
    parser.add_argument(
        '--output',
        default='data',
        help='Output directory for CSV files (default: data)'
    )
    
    parser.add_argument(
        '--files',
        type=int,
        help='Number of CSV files to generate (auto-calculated if not specified)'
    )
    
    args = parser.parse_args()
    
    try:
        generate_dataset(args.size, args.output, args.files)
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
