"""Backup rotation engine using Grandfather-Father-Son (GFS) strategy."""

import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

from .backup_scanner import BackupFile

logger = logging.getLogger(__name__)


class RotationEngine:
    """Implements GFS rotation strategy with calendar-based bucketing."""

    def __init__(
        self, daily_count: int = 7, weekly_count: int = 4, monthly_count: int = 12
    ):
        """Initialize rotation engine.

        Args:
            daily_count: Number of daily backups to keep
            weekly_count: Number of weekly backups to keep (Mondays)
            monthly_count: Number of monthly backups to keep (first of month)
        """
        self.daily_count = daily_count
        self.weekly_count = weekly_count
        self.monthly_count = monthly_count

    def _is_monday(self, dt: datetime) -> bool:
        """Check if datetime is a Monday.

        Args:
            dt: Datetime to check

        Returns:
            True if Monday (weekday 0), False otherwise
        """
        return dt.weekday() == 0

    def _is_first_of_month(self, dt: datetime) -> bool:
        """Check if datetime is the first day of month.

        Args:
            dt: Datetime to check

        Returns:
            True if first day of month, False otherwise
        """
        return dt.day == 1

    def _get_calendar_week_key(self, dt: datetime) -> tuple[int, int]:
        """Get calendar week key (year, week_number).

        Args:
            dt: Datetime

        Returns:
            Tuple of (iso_year, iso_week)
        """
        iso_calendar = dt.isocalendar()
        return (iso_calendar[0], iso_calendar[1])  # (year, week)

    def _get_calendar_month_key(self, dt: datetime) -> tuple[int, int]:
        """Get calendar month key (year, month).

        Args:
            dt: Datetime

        Returns:
            Tuple of (year, month)
        """
        return (dt.year, dt.month)

    def calculate_deletions(self, backups: list[BackupFile]) -> list[BackupFile]:
        """Calculate which backups should be deleted based on GFS strategy.

        The strategy works as follows:
        1. Keep the most recent N daily backups
        2. Keep the most recent N weekly backups (one per calendar week, preferring Monday)
        3. Keep the most recent N monthly backups (one per calendar month, preferring first of month)

        Args:
            backups: List of backup files

        Returns:
            List of backup files to delete
        """
        if not backups:
            return []

        # Sort backups by timestamp (newest first)
        sorted_backups = sorted(backups, key=lambda b: b.timestamp, reverse=True)

        # Track which backups to keep
        keep_backups = set()

        # 1. Keep daily backups (most recent N)
        daily_backups = sorted_backups[: self.daily_count]
        keep_backups.update(daily_backups)
        logger.debug(f"Keeping {len(daily_backups)} daily backups")

        # 2. Keep weekly backups (one per calendar week, prefer Monday)
        weekly_buckets = defaultdict(list)
        for backup in sorted_backups:
            week_key = self._get_calendar_week_key(backup.timestamp)
            weekly_buckets[week_key].append(backup)

        # Sort week keys (newest first)
        sorted_weeks = sorted(weekly_buckets.keys(), reverse=True)[: self.weekly_count]

        for week_key in sorted_weeks:
            week_backups = weekly_buckets[week_key]

            # Prefer Monday backups
            monday_backups = [b for b in week_backups if self._is_monday(b.timestamp)]
            if monday_backups:
                # Keep the newest Monday backup for this week
                keep_backups.add(max(monday_backups, key=lambda b: b.timestamp))
            else:
                # Keep the newest backup for this week
                keep_backups.add(max(week_backups, key=lambda b: b.timestamp))

        logger.debug(
            f"Keeping {len([b for b in keep_backups if b not in daily_backups])} additional weekly backups"
        )

        # 3. Keep monthly backups (one per calendar month, prefer first of month)
        monthly_buckets = defaultdict(list)
        for backup in sorted_backups:
            month_key = self._get_calendar_month_key(backup.timestamp)
            monthly_buckets[month_key].append(backup)

        # Sort month keys (newest first)
        sorted_months = sorted(monthly_buckets.keys(), reverse=True)[
            : self.monthly_count
        ]

        for month_key in sorted_months:
            month_backups = monthly_buckets[month_key]

            # Prefer first-of-month backups
            first_of_month_backups = [
                b for b in month_backups if self._is_first_of_month(b.timestamp)
            ]
            if first_of_month_backups:
                # Keep the newest first-of-month backup for this month
                keep_backups.add(max(first_of_month_backups, key=lambda b: b.timestamp))
            else:
                # Keep the newest backup for this month
                keep_backups.add(max(month_backups, key=lambda b: b.timestamp))

        logger.debug(f"Total backups to keep: {len(keep_backups)}")

        # Calculate deletions (backups not in keep set)
        deletions = [b for b in sorted_backups if b not in keep_backups]

        logger.info(
            f"Calculated {len(deletions)} backups for deletion out of {len(backups)} total"
        )

        return deletions


def create_rotation_engine(
    daily: Optional[int] = None,
    weekly: Optional[int] = None,
    monthly: Optional[int] = None,
    default_daily: int = 7,
    default_weekly: int = 4,
    default_monthly: int = 12,
) -> RotationEngine:
    """Create rotation engine with optional overrides.

    Args:
        daily: Override for daily count (None to use default)
        weekly: Override for weekly count (None to use default)
        monthly: Override for monthly count (None to use default)
        default_daily: Default daily count
        default_weekly: Default weekly count
        default_monthly: Default monthly count

    Returns:
        RotationEngine instance
    """
    return RotationEngine(
        daily_count=daily if daily is not None else default_daily,
        weekly_count=weekly if weekly is not None else default_weekly,
        monthly_count=monthly if monthly is not None else default_monthly,
    )
