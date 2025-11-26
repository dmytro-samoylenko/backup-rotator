"""Backup file scanner with datetime parsing and validation."""

import os
import re
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass

from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackupFile:
    """Represents a backup file."""

    path: Path
    filename: str
    timestamp: datetime
    size_bytes: int
    project_id: str

    def __hash__(self):
        """Make BackupFile hashable using path as unique identifier."""
        return hash(str(self.path))

    def __eq__(self, other):
        """Compare BackupFile instances by path."""
        if not isinstance(other, BackupFile):
            return False
        return self.path == other.path

    @property
    def size_mb(self) -> float:
        """Get file size in MB."""
        return self.size_bytes / (1024 * 1024)


class BackupScanner:
    """Scans and validates backup files."""

    def __init__(self, base_path: str, datetime_formats: list[str]):
        """Initialize backup scanner.

        Args:
            base_path: Base path for backups
            datetime_formats: List of datetime format patterns (in priority order)
        """
        self.base_path = Path(base_path)
        self.datetime_formats = datetime_formats

    def _parse_datetime_from_filename(self, filename: str) -> Optional[datetime]:
        """Parse datetime from filename using configured formats.

        Args:
            filename: Backup filename

        Returns:
            Parsed datetime or None if parsing fails
        """
        # Try each format in priority order
        for fmt in self.datetime_formats:
            try:
                # Extract datetime-like string from filename
                # Try to match the pattern in the filename
                dt = datetime.strptime(filename, fmt)
                return dt
            except ValueError:
                # Try to find pattern within filename
                try:
                    # Look for datetime pattern in the filename
                    pattern = (
                        fmt.replace("%Y", r"\d{4}")
                        .replace("%m", r"\d{2}")
                        .replace("%d", r"\d{2}")
                    )
                    pattern = (
                        pattern.replace("%H", r"\d{2}")
                        .replace("%M", r"\d{2}")
                        .replace("%S", r"\d{2}")
                    )

                    # Extract potential datetime strings
                    for part in filename.split("."):
                        for subpart in part.split("_"):
                            try:
                                dt = datetime.strptime(subpart, fmt)
                                return dt
                            except ValueError:
                                # Try with underscores combined
                                pass

                        try:
                            dt = datetime.strptime(part, fmt)
                            return dt
                        except ValueError:
                            pass

                    # Try the whole filename without extension
                    base_name = filename.split(".")[0]
                    try:
                        dt = datetime.strptime(base_name, fmt)
                        return dt
                    except ValueError:
                        pass

                except Exception:
                    continue

        # If all format attempts fail, try dateutil as last resort
        try:
            dt = dateutil_parser.parse(filename, fuzzy=True)
            return dt
        except Exception:
            pass

        return None

    def scan_project_backups(
        self, project_id: str, filename_pattern: str, min_file_size_mb: float
    ) -> tuple[list[BackupFile], list[tuple[str, float]]]:
        """Scan backups for a specific project.

        Args:
            project_id: Project ID
            filename_pattern: Regex pattern for backup files
            min_file_size_mb: Minimum file size in MB

        Returns:
            Tuple of (valid_backups, undersized_files)
            undersized_files is list of (filename, size_mb) tuples
        """
        project_path = self.base_path / project_id

        if not project_path.exists():
            logger.warning(f"Project directory not found: {project_path}")
            return [], []

        if not project_path.is_dir():
            logger.warning(f"Project path is not a directory: {project_path}")
            return [], []

        valid_backups = []
        undersized_files = []
        pattern = re.compile(filename_pattern)
        min_size_bytes = min_file_size_mb * 1024 * 1024

        logger.debug(f"Scanning project {project_id} at {project_path}")

        for item in project_path.iterdir():
            if not item.is_file():
                continue

            filename = item.name

            # Check if filename matches pattern
            if not pattern.match(filename):
                logger.debug(f"File {filename} does not match pattern")
                continue

            # Get file size
            try:
                size_bytes = item.stat().st_size
            except OSError as e:
                logger.error(f"Failed to get size of {item}: {e}")
                continue

            size_mb = size_bytes / (1024 * 1024)

            # Check minimum size
            if size_bytes < min_size_bytes:
                logger.warning(
                    f"File {filename} is undersized: {size_mb:.2f} MB < {min_file_size_mb:.2f} MB"
                )
                undersized_files.append((filename, size_mb))
                continue

            # Parse datetime from filename
            timestamp = self._parse_datetime_from_filename(filename)
            if timestamp is None:
                logger.warning(f"Failed to parse datetime from filename: {filename}")
                continue

            backup_file = BackupFile(
                path=item,
                filename=filename,
                timestamp=timestamp,
                size_bytes=size_bytes,
                project_id=project_id,
            )

            valid_backups.append(backup_file)

        logger.info(
            f"Project {project_id}: Found {len(valid_backups)} valid backups, "
            f"{len(undersized_files)} undersized files"
        )

        return valid_backups, undersized_files

    def detect_stale_backups(
        self, backups: list[BackupFile], expected_interval_hours: int
    ) -> bool:
        """Detect if backups are stale (no recent backups).

        Args:
            backups: List of backup files
            expected_interval_hours: Expected backup interval in hours

        Returns:
            True if backups are stale (missing recent backup), False otherwise
        """
        if not backups:
            logger.warning("No backups found - backups are stale")
            return True

        # Find newest backup
        newest_backup = max(backups, key=lambda b: b.timestamp)
        now = datetime.now()

        # Check if newest backup is older than expected interval
        age_hours = (now - newest_backup.timestamp).total_seconds() / 3600

        if age_hours > expected_interval_hours:
            logger.warning(
                f"Newest backup is {age_hours:.1f} hours old, "
                f"expected interval is {expected_interval_hours} hours"
            )
            return True

        return False

    def get_backup_statistics(
        self, backups: list[BackupFile]
    ) -> tuple[int, Optional[datetime], Optional[datetime], float]:
        """Get statistics for backup files.

        Args:
            backups: List of backup files

        Returns:
            Tuple of (count, oldest_timestamp, newest_timestamp, total_size_mb)
        """
        if not backups:
            return 0, None, None, 0.0

        count = len(backups)
        oldest = min(backups, key=lambda b: b.timestamp).timestamp
        newest = max(backups, key=lambda b: b.timestamp).timestamp
        total_size_mb = sum(b.size_mb for b in backups)

        return count, oldest, newest, total_size_mb
