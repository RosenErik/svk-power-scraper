#!/usr/bin/env python3
"""
github_actions_runner.py - GitHub Actions optimized runner for SVK Scraper
===========================================================================
This script handles data scraping, merging, and management in GitHub Actions.
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import logging
import hashlib

# Add scripts directory to path if needed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the scraper
from svk_scraper import SVKPowerScraper


class GitHubActionsDataManager:
    """Data manager optimized for GitHub Actions."""
    
    def __init__(self, data_dir: str = "data", logs_dir: str = "logs"):
        self.data_dir = Path(data_dir)
        self.logs_dir = Path(logs_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self.setup_logging()
        
        # File paths
        self.master_file = self.data_dir / "svk_master_data.csv"
        self.state_file = self.data_dir / "scraper_state.json"
        self.backup_dir = self.data_dir / "backups"
        self.backup_dir.mkdir(exist_ok=True)
        
    def setup_logging(self):
        """Setup GitHub Actions compatible logging."""
        log_file = self.logs_dir / f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Configure logging to write to both file and stdout
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # GitHub Actions annotations
        self.logger.info("::group::SVK Scraper Initialization")
        
    def load_master_data(self) -> pd.DataFrame:
        """Load existing master data."""
        if self.master_file.exists():
            df = pd.read_csv(self.master_file, encoding='utf-8-sig')
            self.logger.info(f"✅ Loaded {len(df)} existing records")
            return df
        else:
            self.logger.info("📄 No existing data found, starting fresh")
            return pd.DataFrame()
            
    def save_master_data(self, df: pd.DataFrame):
        """Save master data with backup."""
        # Create backup if master exists
        if self.master_file.exists():
            backup_file = self.backup_dir / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df_existing = pd.read_csv(self.master_file, encoding='utf-8-sig')
            df_existing.to_csv(backup_file, index=False, encoding='utf-8-sig')
            self.logger.info(f"💾 Created backup: {backup_file.name}")
            
            # Keep only last 5 backups
            backups = sorted(self.backup_dir.glob("backup_*.csv"))
            if len(backups) > 5:
                for old_backup in backups[:-5]:
                    old_backup.unlink()
                    
        # Save master file
        df.to_csv(self.master_file, index=False, encoding='utf-8-sig')
        self.logger.info(f"✅ Saved {len(df)} records to master file")
        
        # Update state
        self.update_state(df)
        
    def update_state(self, df: pd.DataFrame):
        """Update scraper state for monitoring."""
        state = {
            'last_update': datetime.now().isoformat(),
            'total_records': len(df),
            'date_range': {
                'start': df['Date'].min() if not df.empty else None,
                'end': df['Date'].max() if not df.empty else None
            },
            'github_run_number': os.environ.get('GITHUB_RUN_NUMBER', 'local'),
            'github_run_id': os.environ.get('GITHUB_RUN_ID', 'local')
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
            
    def merge_dataframes(self, df_existing: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
        """Merge dataframes with deduplication."""
        if df_existing.empty:
            return df_new
        if df_new.empty:
            return df_existing
            
        # Combine and deduplicate
        df_merged = pd.concat([df_existing, df_new], ignore_index=True)
        
        # Remove duplicates based on Date and Timme
        df_merged = df_merged.drop_duplicates(subset=['Date', 'Timme'], keep='last')
        
        # Sort by date
        df_merged = df_merged.sort_values(['Date', 'Timme'])
        
        self.logger.info(f"📊 Merged: {len(df_existing)} + {len(df_new)} → {len(df_merged)} records")
        
        return df_merged
        
    def get_missing_dates(self, df: pd.DataFrame, days_back: int = 30) -> list:
        """Find missing dates in recent history."""
        if df.empty:
            return []
            
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        existing_dates = pd.to_datetime(df['Date'].unique())
        missing_dates = sorted(set(date_range) - set(existing_dates))
        
        return [d.strftime('%Y-%m-%d') for d in missing_dates]


def run_daily_scrape(manager: GitHubActionsDataManager, days: int = 3):
    """Run daily scrape."""
    print("::group::Daily Scrape")
    manager.logger.info(f"🚀 Starting daily scrape for {days} days")
    
    try:
        # Load existing data
        df_existing = manager.load_master_data()
        
        # Run scraper
        with SVKPowerScraper(headless=True) as scraper:
            df_new = scraper.scrape_multiple_days(num_days=days)
            
        if not df_new.empty:
            # Save raw data
            raw_file = manager.data_dir / f"raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df_new.to_csv(raw_file, index=False, encoding='utf-8-sig')
            
            # Merge and save
            df_merged = manager.merge_dataframes(df_existing, df_new)
            manager.save_master_data(df_merged)
            
            # Output summary for GitHub Actions
            print(f"::notice title=Daily Scrape Success::Added {len(df_new)} new records")
            manager.logger.info(f"✅ Daily scrape completed successfully")
        else:
            print("::warning title=No Data::No new data retrieved")
            manager.logger.warning("⚠️ No data retrieved")
            
    except Exception as e:
        print(f"::error title=Scrape Failed::{str(e)}")
        manager.logger.error(f"❌ Error in daily scrape: {e}")
        raise
    finally:
        print("::endgroup::")


def run_weekly_scrape(manager: GitHubActionsDataManager, days: int = 30):
    """Run weekly comprehensive scrape to fill gaps."""
    print("::group::Weekly Comprehensive Scrape")
    manager.logger.info(f"🔍 Starting weekly scrape - checking {days} days for gaps")
    
    try:
        # Load existing data
        df_existing = manager.load_master_data()
        
        # Find missing dates
        missing_dates = manager.get_missing_dates(df_existing, days)
        
        if missing_dates:
            manager.logger.info(f"📅 Found {len(missing_dates)} missing dates")
            print(f"::warning title=Missing Dates::Found {len(missing_dates)} dates with missing data")
            
            # Limit to 5 dates per run to avoid timeout
            dates_to_scrape = missing_dates[:5]
            
            for date in dates_to_scrape:
                try:
                    manager.logger.info(f"Scraping {date}...")
                    with SVKPowerScraper(headless=True) as scraper:
                        df_day = scraper.scrape_multiple_days(num_days=1, start_date=date)
                        
                    if not df_day.empty:
                        df_existing = manager.merge_dataframes(df_existing, df_day)
                        manager.logger.info(f"✅ Retrieved data for {date}")
                        
                except Exception as e:
                    manager.logger.error(f"❌ Failed to scrape {date}: {e}")
                    
            # Save updated data
            manager.save_master_data(df_existing)
            print(f"::notice title=Weekly Scrape Complete::Filled {len(dates_to_scrape)} missing dates")
            
        else:
            manager.logger.info("✅ No missing dates found")
            print("::notice title=Data Complete::No missing dates in the last 30 days")
            
    except Exception as e:
        print(f"::error title=Weekly Scrape Failed::{str(e)}")
        manager.logger.error(f"❌ Error in weekly scrape: {e}")
        raise
    finally:
        print("::endgroup::")


def run_custom_scrape(manager: GitHubActionsDataManager, days: int, start_date: str = None):
    """Run custom scrape with specified parameters."""
    print("::group::Custom Scrape")
    
    if start_date:
        manager.logger.info(f"🎯 Custom scrape: {days} days from {start_date}")
    else:
        manager.logger.info(f"🎯 Custom scrape: last {days} days")
        
    try:
        # Load existing data
        df_existing = manager.load_master_data()
        
        # Run scraper
        with SVKPowerScraper(headless=True) as scraper:
            df_new = scraper.scrape_multiple_days(num_days=days, start_date=start_date)
            
        if not df_new.empty:
            # Merge and save
            df_merged = manager.merge_dataframes(df_existing, df_new)
            manager.save_master_data(df_merged)
            
            print(f"::notice title=Custom Scrape Success::Retrieved {len(df_new)} records")
        else:
            print("::warning title=No Data::No data retrieved")
            
    except Exception as e:
        print(f"::error title=Custom Scrape Failed::{str(e)}")
        manager.logger.error(f"❌ Error in custom scrape: {e}")
        raise
    finally:
        print("::endgroup::")


def generate_summary(manager: GitHubActionsDataManager):
    """Generate summary for GitHub Actions."""
    print("::group::Summary")
    
    df = manager.load_master_data()
    
    if not df.empty:
        summary = f"""
## 📊 SVK Data Summary

- **Total Records:** {len(df):,}
- **Date Range:** {df['Date'].min()} to {df['Date'].max()}
- **Unique Dates:** {df['Date'].nunique()}
- **Last Update:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Recent Data Coverage
"""
        
        # Check last 7 days coverage
        last_week = pd.date_range(end=datetime.now(), periods=7, freq='D')
        for date in last_week:
            date_str = date.strftime('%Y-%m-%d')
            has_data = date_str in df['Date'].values
            status = "✅" if has_data else "❌"
            summary += f"- {date_str}: {status}\n"
            
        print(summary)
        
        # Write to GitHub Actions summary
        summary_file = os.environ.get('GITHUB_STEP_SUMMARY', '')
        if summary_file:
            with open(summary_file, 'a') as f:
                f.write(summary)
                
    print("::endgroup::")


def main():
    parser = argparse.ArgumentParser(description='GitHub Actions SVK Scraper Runner')
    parser.add_argument('action', choices=['daily', 'weekly', 'custom'], 
                       help='Type of scrape to run')
    parser.add_argument('--days', type=int, default=7, 
                       help='Number of days to scrape')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date for custom scrape (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Initialize manager
    manager = GitHubActionsDataManager()
    
    # Run appropriate action
    if args.action == 'daily':
        run_daily_scrape(manager, days=3)
    elif args.action == 'weekly':
        run_weekly_scrape(manager, days=30)
    elif args.action == 'custom':
        run_custom_scrape(manager, days=args.days, start_date=args.start_date)
        
    # Generate summary
    generate_summary(manager)
    
    manager.logger.info("✅ All tasks completed")


if __name__ == "__main__":
    main()