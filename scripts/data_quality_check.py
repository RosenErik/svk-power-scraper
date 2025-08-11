#!/usr/bin/env python3
"""
data_quality_check.py - Data quality checking and reporting for SVK data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
import sys
import logging
from typing import Dict, List, Tuple


class DataQualityChecker:
    """Check and report on SVK data quality."""
    
    def __init__(self, data_file: str = "data/svk_master_data.csv"):
        """Initialize the quality checker."""
        self.data_file = Path(data_file)
        self.report_dir = Path("reports")
        self.report_dir.mkdir(exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Load data
        self.df = self.load_data()
        
        # Quality metrics
        self.metrics = {
            'total_records': 0,
            'date_range': {},
            'missing_dates': [],
            'duplicate_records': 0,
            'missing_values': {},
            'data_gaps': [],
            'coverage_percentage': 0,
            'issues_found': False,
            'checks_passed': [],
            'checks_failed': []
        }
        
    def load_data(self) -> pd.DataFrame:
        """Load the data file."""
        if not self.data_file.exists():
            self.logger.error(f"Data file not found: {self.data_file}")
            return pd.DataFrame()
            
        try:
            df = pd.read_csv(self.data_file, encoding='utf-8-sig')
            self.logger.info(f"Loaded {len(df)} records from {self.data_file}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            return pd.DataFrame()
            
    def check_date_continuity(self) -> List[str]:
        """Check for missing dates in the dataset."""
        if self.df.empty or 'Date' not in self.df.columns:
            return []
            
        try:
            # Convert dates
            dates = pd.to_datetime(self.df['Date'].unique())
            
            if len(dates) == 0:
                return []
                
            # Create expected date range
            min_date = dates.min()
            max_date = dates.max()
            expected_dates = pd.date_range(start=min_date, end=max_date, freq='D')
            
            # Find missing dates
            missing_dates = sorted(set(expected_dates) - set(dates))
            
            return [d.strftime('%Y-%m-%d') for d in missing_dates]
            
        except Exception as e:
            self.logger.error(f"Error checking date continuity: {e}")
            return []
            
    def check_duplicates(self) -> int:
        """Check for duplicate records."""
        if self.df.empty:
            return 0
            
        try:
            # Check duplicates based on Date and Timme
            if 'Date' in self.df.columns and 'Timme' in self.df.columns:
                duplicates = self.df.duplicated(subset=['Date', 'Timme'], keep=False)
                return int(duplicates.sum())
            return 0
            
        except Exception as e:
            self.logger.error(f"Error checking duplicates: {e}")
            return 0
            
    def check_missing_values(self) -> Dict[str, int]:
        """Check for missing values in each column."""
        if self.df.empty:
            return {}
            
        missing = {}
        for col in self.df.columns:
            null_count = self.df[col].isnull().sum()
            if null_count > 0:
                missing[col] = int(null_count)
                
        return missing
        
    def check_data_gaps(self, threshold_hours: int = 24) -> List[Dict]:
        """Find gaps in hourly data larger than threshold."""
        gaps = []
        
        if self.df.empty or 'Date' not in self.df.columns:
            return gaps
            
        try:
            # Group by date and count hours
            date_counts = self.df.groupby('Date').size()
            
            # Check for dates with incomplete hours (should have 24 records per day)
            for date, count in date_counts.items():
                if count < 24:
                    gaps.append({
                        'date': date,
                        'hours_found': int(count),
                        'hours_missing': 24 - int(count)
                    })
                    
            return gaps
            
        except Exception as e:
            self.logger.error(f"Error checking data gaps: {e}")
            return []
            
    def check_value_ranges(self) -> Dict[str, Dict]:
        """Check if values are within reasonable ranges."""
        ranges = {}
        
        if self.df.empty:
            return ranges
            
        # Check numeric columns
        numeric_cols = ['Prognos (MW)', 'Förbrukning (MW)']
        
        for col in numeric_cols:
            if col in self.df.columns:
                try:
                    # Convert to numeric
                    values = pd.to_numeric(self.df[col], errors='coerce')
                    values = values.dropna()
                    
                    if len(values) > 0:
                        ranges[col] = {
                            'min': float(values.min()),
                            'max': float(values.max()),
                            'mean': float(values.mean()),
                            'std': float(values.std()),
                            'outliers': int((values < 0).sum()),  # Negative values
                            'suspicious': int((values > 50000).sum())  # Very high values
                        }
                except Exception as e:
                    self.logger.error(f"Error checking range for {col}: {e}")
                    
        return ranges
        
    def calculate_coverage(self, days_back: int = 30) -> float:
        """Calculate data coverage percentage for recent period."""
        if self.df.empty or 'Date' not in self.df.columns:
            return 0.0
            
        try:
            # Get date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Expected dates
            expected_dates = pd.date_range(start=start_date, end=end_date, freq='D')
            expected_records = len(expected_dates) * 24  # 24 hours per day
            
            # Actual dates in range
            df_dates = pd.to_datetime(self.df['Date'])
            df_in_range = self.df[(df_dates >= start_date) & (df_dates <= end_date)]
            actual_records = len(df_in_range)
            
            if expected_records > 0:
                coverage = (actual_records / expected_records) * 100
                return round(coverage, 2)
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Error calculating coverage: {e}")
            return 0.0
            
    def run_all_checks(self) -> Dict:
        """Run all quality checks."""
        self.logger.info("Starting data quality checks...")
        
        if self.df.empty:
            self.metrics['issues_found'] = True
            self.metrics['checks_failed'].append("No data found")
            return self.metrics
            
        # Basic metrics
        self.metrics['total_records'] = len(self.df)
        
        if 'Date' in self.df.columns:
            self.metrics['date_range'] = {
                'start': self.df['Date'].min(),
                'end': self.df['Date'].max()
            }
            
        # Run checks
        self.logger.info("Checking date continuity...")
        self.metrics['missing_dates'] = self.check_date_continuity()
        if self.metrics['missing_dates']:
            self.metrics['checks_failed'].append(f"Missing {len(self.metrics['missing_dates'])} dates")
            self.metrics['issues_found'] = True
        else:
            self.metrics['checks_passed'].append("Date continuity")
            
        self.logger.info("Checking for duplicates...")
        self.metrics['duplicate_records'] = self.check_duplicates()
        if self.metrics['duplicate_records'] > 0:
            self.metrics['checks_failed'].append(f"Found {self.metrics['duplicate_records']} duplicates")
            self.metrics['issues_found'] = True
        else:
            self.metrics['checks_passed'].append("No duplicates")
            
        self.logger.info("Checking missing values...")
        self.metrics['missing_values'] = self.check_missing_values()
        if self.metrics['missing_values']:
            self.metrics['checks_failed'].append("Missing values detected")
            self.metrics['issues_found'] = True
        else:
            self.metrics['checks_passed'].append("No missing values")
            
        self.logger.info("Checking data gaps...")
        self.metrics['data_gaps'] = self.check_data_gaps()
        if self.metrics['data_gaps']:
            self.metrics['checks_failed'].append(f"Found {len(self.metrics['data_gaps'])} incomplete days")
            self.metrics['issues_found'] = True
        else:
            self.metrics['checks_passed'].append("No data gaps")
            
        self.logger.info("Checking value ranges...")
        self.metrics['value_ranges'] = self.check_value_ranges()
        
        self.logger.info("Calculating coverage...")
        self.metrics['coverage_percentage'] = self.calculate_coverage()
        if self.metrics['coverage_percentage'] < 90:
            self.metrics['checks_failed'].append(f"Low coverage: {self.metrics['coverage_percentage']}%")
            self.metrics['issues_found'] = True
        else:
            self.metrics['checks_passed'].append(f"Good coverage: {self.metrics['coverage_percentage']}%")
            
        return self.metrics
        
    def generate_html_report(self) -> str:
        """Generate an HTML report of the quality checks."""
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>SVK Data Quality Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }}
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .metric {{
            display: inline-block;
            margin: 10px 20px;
        }}
        .metric-value {{
            font-size: 24px;
            font-weight: bold;
            color: #4CAF50;
        }}
        .metric-label {{
            color: #666;
            font-size: 14px;
        }}
        .issue {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 10px;
            margin: 10px 0;
        }}
        .success {{
            background: #d4edda;
            border-left: 4px solid #28a745;
            padding: 10px;
            margin: 10px 0;
        }}
        .section {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{
            text-align: left;
            padding: 8px;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        .status-good {{
            color: #28a745;
        }}
        .status-bad {{
            color: #dc3545;
        }}
    </style>
</head>
<body>
    <h1>📊 SVK Data Quality Report</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <div class="summary">
        <h2>Summary</h2>
        <div class="metric">
            <div class="metric-value">{self.metrics['total_records']:,}</div>
            <div class="metric-label">Total Records</div>
        </div>
        <div class="metric">
            <div class="metric-value">{self.metrics['coverage_percentage']}%</div>
            <div class="metric-label">Data Coverage (30 days)</div>
        </div>
        <div class="metric">
            <div class="metric-value class="{'status-bad' if self.metrics['issues_found'] else 'status-good'}"">
                {'Issues Found' if self.metrics['issues_found'] else 'All Checks Passed'}
            </div>
            <div class="metric-label">Status</div>
        </div>
    </div>
"""
        
        # Add passed checks
        if self.metrics['checks_passed']:
            html += '<div class="section"><h2>✅ Passed Checks</h2>'
            for check in self.metrics['checks_passed']:
                html += f'<div class="success">{check}</div>'
            html += '</div>'
            
        # Add failed checks
        if self.metrics['checks_failed']:
            html += '<div class="section"><h2>⚠️ Failed Checks</h2>'
            for check in self.metrics['checks_failed']:
                html += f'<div class="issue">{check}</div>'
            html += '</div>'
            
        # Add missing dates details
        if self.metrics['missing_dates']:
            html += '<div class="section"><h2>📅 Missing Dates</h2>'
            html += '<p>The following dates have no data:</p><ul>'
            for date in self.metrics['missing_dates'][:20]:  # Show first 20
                html += f'<li>{date}</li>'
            if len(self.metrics['missing_dates']) > 20:
                html += f'<li>... and {len(self.metrics["missing_dates"]) - 20} more</li>'
            html += '</ul></div>'
            
        # Add value ranges
        if 'value_ranges' in self.metrics and self.metrics['value_ranges']:
            html += '<div class="section"><h2>📈 Value Ranges</h2>'
            html += '<table><tr><th>Column</th><th>Min</th><th>Max</th><th>Mean</th><th>Outliers</th></tr>'
            for col, stats in self.metrics['value_ranges'].items():
                html += f"""<tr>
                    <td>{col}</td>
                    <td>{stats['min']:.2f}</td>
                    <td>{stats['max']:.2f}</td>
                    <td>{stats['mean']:.2f}</td>
                    <td>{stats['outliers']}</td>
                </tr>"""
            html += '</table></div>'
            
        html += '</body></html>'
        
        return html
        
    def save_reports(self):
        """Save all reports."""
        # Save JSON summary
        summary_file = self.report_dir / "data_quality_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(self.metrics, f, indent=2, default=str)
        self.logger.info(f"Saved JSON summary to {summary_file}")
        
        # Save HTML report
        html_file = self.report_dir / "data_quality_report.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(self.generate_html_report())
        self.logger.info(f"Saved HTML report to {html_file}")
        
        # Save detailed CSV of issues
        if self.metrics['missing_dates']:
            missing_df = pd.DataFrame({'missing_date': self.metrics['missing_dates']})
            missing_file = self.report_dir / "missing_dates.csv"
            missing_df.to_csv(missing_file, index=False)
            self.logger.info(f"Saved missing dates to {missing_file}")
            

def main():
    """Main function to run quality checks."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Check SVK data quality')
    parser.add_argument('--data-file', default='data/svk_master_data.csv',
                       help='Path to data file')
    parser.add_argument('--output-dir', default='reports',
                       help='Output directory for reports')
    
    args = parser.parse_args()
    
    # Run quality checks
    checker = DataQualityChecker(data_file=args.data_file)
    
    if checker.df.empty:
        print("❌ No data found to check")
        sys.exit(1)
        
    metrics = checker.run_all_checks()
    
    # Print summary
    print("\n" + "="*60)
    print("DATA QUALITY SUMMARY")
    print("="*60)
    print(f"Total Records: {metrics['total_records']:,}")
    print(f"Date Range: {metrics['date_range'].get('start', 'N/A')} to {metrics['date_range'].get('end', 'N/A')}")
    print(f"Coverage (30 days): {metrics['coverage_percentage']}%")
    print(f"Missing Dates: {len(metrics['missing_dates'])}")
    print(f"Duplicate Records: {metrics['duplicate_records']}")
    print(f"Status: {'❌ Issues Found' if metrics['issues_found'] else '✅ All Checks Passed'}")
    
    # Save reports
    checker.save_reports()
    
    # Exit with error code if issues found
    if metrics['issues_found']:
        print("\n⚠️  Quality issues detected. Check reports for details.")
        sys.exit(1)
    else:
        print("\n✅ All quality checks passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()