# SVK Power Data Scraper

Automated scraper for Swedish power system data from SVK (Svenska kraftnät).

## 📊 Data Access

### Latest Data
Always-updated dataset available at:
```
https://raw.githubusercontent.com/RosenErik/svk-power-scraper/main/data/svk_master_data.csv
```

### Quick Usage
```python
import pandas as pd

url = "https://raw.githubusercontent.com/RosenErik/svk-power-scraper/main/data/svk_master_data.csv"
df = pd.read_csv(url)
```

## 📁 Repository Structure

```
├── .github/workflows/     # GitHub Actions automation
├── data/                  # Scraped data storage
│   └── svk_master_data.csv   # Main consolidated dataset
├── scripts/               # Scraper and utility scripts
├── logs/                  # Execution logs
└── reports/              # Data quality reports
```

## 🔄 Update Schedule

- **Daily**: 06:00 UTC - Scrapes last 3 days
- **Weekly**: Sundays 03:00 UTC - Fills gaps in last 30 days
- **Manual**: Trigger via GitHub Actions UI anytime

## 📈 Data Structure

| Column | Description | Type | Example |
|--------|-------------|------|---------|
| Date | Date of measurement | YYYY-MM-DD | 2025-08-11 |
| Timme | Hour period | HH:MM - HH:MM | 14:00 - 15:00 |
| Prognos (MW) | Forecasted power | Float | 12843.4 |
| Förbrukning (MW) | Actual consumption | Float | 12757.0 |
| DateTime | Combined timestamp | YYYY-MM-DD HH:MM | 2025-08-11 14:00 |

## 🚀 Manual Execution

### Run Specific Date Range
1. Go to Actions tab
2. Select "SVK Power Data Scraper"
3. Click "Run workflow"
4. Choose:
   - Type: `custom`
   - Days: Number of days
   - Start date: YYYY-MM-DD

### Local Development
```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/svk-power-scraper.git
cd svk-power-scraper

# Install dependencies
pip install -r requirements.txt

# Run scraper
python scripts/github_actions_runner.py daily

# Check data quality
python scripts/data_quality_check.py
```

## 📊 Data Quality

Weekly automated checks for:
- Missing dates
- Duplicate records
- Data gaps
- Value range validation
- Coverage percentage

View latest report: [Data Quality Report](reports/data_quality_report.html)

## 🛠️ Configuration

### Modify Schedule
Edit `.github/workflows/svk-scraper.yml`:
```yaml
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 06:00 UTC
```

### Change Region
Edit `scripts/svk_scraper.py`:
- Default: Stockholm (SE3)
- Options: SE1 (Luleå), SE2 (Sundsvall), SE4 (Malmö)

## 📜 License

This project is licensed under MIT License - see [LICENSE](LICENSE) file.

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## 📧 Contact

For issues or questions, please open a GitHub issue.

---

**Data Source**: [Svenska kraftnät](https://www.svk.se/om-kraftsystemet/kontrollrummet/)