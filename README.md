# Backup Rotator

Automated backup file rotation system with Telegram notifications using Grandfather-Father-Son (GFS) retention strategy.

## Features

- **Configurable GFS Rotation Strategy**: Per-project retention policies (daily, weekly, monthly backups)
- **Multiple Datetime Format Support**: Flexible parsing of backup filenames with configurable patterns
- **Telegram Notifications**:
  - Low disk space alerts (with daily reminders until resolved)
  - Missing backup file alerts
  - Undersized file warnings
  - Deletion failure alerts
  - Weekly summary reports
- **Disk Space Monitoring**: Monitors `/backups` directory with hysteresis to prevent alert flapping
- **State Persistence**: Tracks deletions, alerts, and reports to avoid duplicates
- **Monthly Rotating Logs**: Automatically rotates logs monthly, keeping 12 months
- **Dry-Run Mode**: Safe default mode that simulates deletions without actually removing files
- **Built-in Scheduler**: Automated weekly reports using configurable schedule

## Installation

### Prerequisites

Install [uv](https://github.com/astral-sh/uv) (fast Python package installer):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install Backup Rotator

1. Clone the repository:
```bash
git clone https://github.com/dmytro-samoylenko/backup-rotator.git
cd backup-rotator
```

2. Create virtual environment and install dependencies:
```bash
uv venv
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate  # On Windows

uv pip install -e .
```

## Configuration

1. Copy and edit `config.yaml`:
```bash
cp config.yaml config.yaml.local
nano config.yaml.local
```

2. Configure Telegram bot:
   - Create a bot via [@BotFather](https://t.me/botfather)
   - Get your chat ID via [@userinfobot](https://t.me/userinfobot)
   - Update `telegram.bot_token` and `telegram.chat_id` in config

3. Configure projects:
   - Set project IDs matching your `/backups/{project_id}` directory structure
   - Define filename patterns using regex
   - Set retention policies (or use defaults)
   - Configure expected backup intervals and minimum file sizes

## Usage

### Dry-Run Mode (Default)
Test the rotation logic without deleting files:
```bash
python run.py --config config.yaml.local
```

### Execute Mode
Actually delete backup files according to retention policy:
```bash
python run.py --config config.yaml.local --execute
```

### Run Once
Run rotation once and exit (no scheduler):
```bash
python run.py --config config.yaml.local --once
```

### Send Weekly Report Now
Immediately send a weekly summary:
```bash
python run.py --config config.yaml.local --send-weekly-now
```

### Command-Line Options

- `--config PATH`: Path to configuration file (default: `config.yaml`)
- `--execute`: Enable actual file deletion (default is dry-run)
- `--once`: Run once without starting scheduler
- `--send-weekly-now`: Send weekly summary immediately and exit

## Configuration Reference

See `config.yaml` for a complete example with comments.

### Global Settings

- `telegram`: Bot token and chat ID
- `backups.base_path`: Base directory for backups (e.g., `/backups`)
- `disk.threshold_percent`: Alert when free space drops below this percentage
- `disk.margin_percent`: Clear alert when space rises above threshold + margin
- `logging.directory`: Log file directory
- `logging.level`: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `weekly_report.day`: Day for weekly report (monday-sunday)
- `weekly_report.time`: Time for weekly report (HH:MM format)
- `datetime_formats`: List of datetime format patterns (tried in order)
- `default_retention`: Default GFS retention policy

### Per-Project Settings

- `id`: Project identifier (matches directory name in `/backups`)
- `name`: Human-readable project name
- `filename_pattern`: Regex pattern for backup files
- `expected_interval_hours`: Expected time between backups
- `min_file_size_mb`: Minimum file size (files below this are flagged)
- `retention`: Optional override for retention policy

## GFS Rotation Strategy

The Grandfather-Father-Son strategy keeps:

1. **Daily backups**: Last N days (default: 7)
2. **Weekly backups**: Last N weeks (default: 4) - one per calendar week, preferring Monday
3. **Monthly backups**: Last N months (default: 12) - one per calendar month, preferring first of month

Backups are categorized by calendar boundaries:
- Weeks start on Monday (ISO calendar)
- Months are calendar months

## State Management

The system maintains a `state.json` file to track:
- Last deletion timestamp per project
- Deleted files count/size (for weekly summaries)
- Last weekly report timestamp
- Disk alert status

This prevents duplicate notifications and provides accurate weekly summaries.

## Logging

Logs are written to `logs/backup-rotator.log` with:
- Monthly rotation
- 12 months retention
- Both file and console output
- Configurable log level

## Production Deployment

### Using systemd

1. Install the application:
```bash
# Clone/copy to /opt
sudo mkdir -p /opt/backup-rotator
sudo cp -r /path/to/backup-rotator/* /opt/backup-rotator/

# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
cd /opt/backup-rotator
uv venv
uv pip install -e .

# Configure
sudo cp config.yaml config.yaml.local
sudo nano config.yaml.local
# Update Telegram credentials and project settings
```

2. Install systemd service:
```bash
sudo cp backup-rotator.service /etc/systemd/system/
sudo systemctl daemon-reload
```

3. Enable and start:
```bash
sudo systemctl enable backup-rotator
sudo systemctl start backup-rotator
```

4. Check status:
```bash
sudo systemctl status backup-rotator
sudo journalctl -u backup-rotator -f
```

**Note**: The service runs as `root` by default since it needs access to `/backups`. Adjust `User` and `Group` in the service file if your backup directory has different permissions.

### Using cron (run once mode)

Add to crontab:
```cron
# Run backup rotation daily at 3 AM
0 3 * * * cd /opt/backup-rotator && source .venv/bin/activate && python run.py --config /etc/backup-rotator/config.yaml --execute --once
```

## Troubleshooting

### Check logs
```bash
tail -f logs/backup-rotator.log
```

### Test configuration
```bash
python run.py --config config.yaml.local --once
```

### Validate regex patterns
Use Python to test your filename patterns:
```python
import re
pattern = r'^\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}\.sql\.gz\.gpg$'
filename = '2025-11-25_20:00:01.sql.gz.gpg'
print(bool(re.match(pattern, filename)))
```

## License

MIT
