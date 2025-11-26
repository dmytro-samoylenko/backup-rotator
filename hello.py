#!/usr/bin/env python3
"""Backup Rotator - Automated backup file rotation with Telegram notifications.

This script manages backup file rotation across multiple projects using a configurable
Grandfather-Father-Son (GFS) retention strategy, monitors disk space, and sends
Telegram notifications for alerts and weekly summaries.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import schedule

from src.config_validator import load_and_validate_config
from src.state_manager import StateManager
from src.logger import setup_logger
from src.telegram_notifier import create_notifier, BackupStats
from src.backup_scanner import BackupScanner
from src.rotation_engine import create_rotation_engine
from src.disk_monitor import create_disk_monitor


logger = logging.getLogger("backup-rotator")


class BackupRotator:
    """Main backup rotator orchestration class."""

    def __init__(self, config_path: str, dry_run: bool = True):
        """Initialize backup rotator.

        Args:
            config_path: Path to configuration file
            dry_run: If True, don't actually delete files
        """
        self.dry_run = dry_run

        # Load and validate configuration
        logger.info(f"Loading configuration from {config_path}")
        self.config = load_and_validate_config(config_path)

        # Setup components
        self.state_manager = StateManager()
        self.notifier = create_notifier(
            self.config.telegram.bot_token, self.config.telegram.chat_id
        )
        self.scanner = BackupScanner(
            self.config.backups.base_path, self.config.datetime_formats
        )
        self.disk_monitor = create_disk_monitor(
            self.config.backups.base_path,
            self.config.disk.threshold_percent,
            self.config.disk.margin_percent,
        )

        logger.info(f"Backup rotator initialized (dry_run={dry_run})")

    async def process_project(self, project_config) -> BackupStats:
        """Process a single project.

        Args:
            project_config: Project configuration

        Returns:
            BackupStats for the project
        """
        project_id = project_config.id
        project_name = project_config.name

        logger.info(f"Processing project: {project_name} (ID: {project_id})")

        # Scan backups
        valid_backups, undersized_files = self.scanner.scan_project_backups(
            project_id, project_config.filename_pattern, project_config.min_file_size_mb
        )

        # Send alerts for undersized files
        for filename, size_mb in undersized_files:
            await self.notifier.send_undersized_file_alert(
                project_id,
                project_name,
                filename,
                size_mb,
                project_config.min_file_size_mb,
            )

        # Check for stale backups
        is_stale = self.scanner.detect_stale_backups(
            valid_backups, project_config.expected_interval_hours
        )

        if is_stale:
            newest_backup_time = None
            if valid_backups:
                newest_backup = max(valid_backups, key=lambda b: b.timestamp)
                newest_backup_time = newest_backup.timestamp.isoformat()

            await self.notifier.send_missing_backup_alert(
                project_id,
                project_name,
                project_config.expected_interval_hours,
                newest_backup_time,
            )

        # Calculate rotation
        retention = project_config.retention or self.config.default_retention
        rotation_engine = create_rotation_engine(
            daily=retention.daily, weekly=retention.weekly, monthly=retention.monthly
        )

        deletions = rotation_engine.calculate_deletions(valid_backups)

        # Execute deletions
        deleted_count = 0
        deleted_size = 0

        for backup in deletions:
            try:
                if self.dry_run:
                    logger.info(f"[DRY RUN] Would delete: {backup.filename}")
                else:
                    logger.info(f"Deleting: {backup.filename}")
                    backup.path.unlink()

                deleted_count += 1
                deleted_size += backup.size_bytes

                # Record deletion in state
                if not self.dry_run:
                    self.state_manager.record_deletion(project_id, backup.size_bytes)

            except Exception as e:
                error_msg = f"Failed to delete {backup.filename}: {e}"
                logger.error(error_msg)
                await self.notifier.send_deletion_failure_alert(
                    project_id, project_name, backup.filename, str(e)
                )

        # Get statistics
        count, oldest, newest, total_size_mb = self.scanner.get_backup_statistics(
            valid_backups
        )

        # Get state for deleted stats
        project_state = self.state_manager.get_project_state(project_id)

        stats = BackupStats(
            project_id=project_id,
            project_name=project_name,
            total_count=count - deleted_count,  # Current count after deletions
            oldest_backup=oldest.isoformat() if oldest else None,
            newest_backup=newest.isoformat() if newest else None,
            total_size_mb=total_size_mb - (deleted_size / (1024 * 1024)),
            deleted_count=project_state.deleted_files_count
            if not self.dry_run
            else deleted_count,
            deleted_size_mb=project_state.deleted_files_size_bytes / (1024 * 1024)
            if not self.dry_run
            else deleted_size / (1024 * 1024),
        )

        logger.info(f"Project {project_name} processed: {deleted_count} files deleted")

        return stats

    async def check_disk_space(self) -> None:
        """Check disk space and send alerts if needed."""
        should_alert, free_percent, free_gb, total_gb = self.disk_monitor.should_alert()

        if should_alert:
            # Check if we should send alert today
            if self.state_manager.should_send_disk_alert():
                await self.notifier.send_low_disk_alert(free_percent, free_gb, total_gb)
                self.state_manager.mark_disk_alert_sent()
        else:
            # Check if alert is resolved
            if self.disk_monitor.is_resolved():
                self.state_manager.clear_disk_alert()

    async def send_weekly_summary(self) -> None:
        """Send weekly summary report."""
        logger.info("Generating weekly summary")

        all_stats = []

        # Process each project to get current stats
        for project_config in self.config.projects:
            try:
                stats = await self.process_project(project_config)
                all_stats.append(stats)
            except Exception as e:
                logger.error(
                    f"Failed to process project {project_config.id} for weekly summary: {e}"
                )
                await self.notifier.send_general_error(
                    str(e), f"Weekly summary - project {project_config.id}"
                )

        # Send summary
        await self.notifier.send_weekly_summary(all_stats)

        # Mark report as sent and reset deletion counters
        self.state_manager.mark_weekly_report_sent()
        self.state_manager.reset_all_deletion_stats()

        logger.info("Weekly summary sent")

    async def run_once(self) -> None:
        """Run backup rotation once."""
        logger.info("Starting backup rotation")

        try:
            # Process each project
            for project_config in self.config.projects:
                try:
                    await self.process_project(project_config)
                except Exception as e:
                    logger.error(f"Failed to process project {project_config.id}: {e}")
                    await self.notifier.send_general_error(
                        str(e), f"Processing project {project_config.id}"
                    )

            # Check disk space
            await self.check_disk_space()

            logger.info("Backup rotation completed")

        except Exception as e:
            logger.error(f"Unexpected error during backup rotation: {e}")
            await self.notifier.send_general_error(str(e), "Backup rotation")

    def schedule_weekly_report(self) -> None:
        """Schedule weekly report job."""
        day = self.config.weekly_report.day
        time = self.config.weekly_report.time

        # Map day name to schedule method
        day_mapping = {
            "monday": schedule.every().monday,
            "tuesday": schedule.every().tuesday,
            "wednesday": schedule.every().wednesday,
            "thursday": schedule.every().thursday,
            "friday": schedule.every().friday,
            "saturday": schedule.every().saturday,
            "sunday": schedule.every().sunday,
        }

        job = (
            day_mapping[day]
            .at(time)
            .do(lambda: asyncio.run(self.send_weekly_summary()))
        )

        logger.info(f"Scheduled weekly report for {day.capitalize()} at {time}")

    async def run_with_scheduler(self) -> None:
        """Run with built-in scheduler."""
        logger.info("Starting backup rotator with scheduler")

        # Schedule weekly report
        self.schedule_weekly_report()

        # Run initial rotation
        await self.run_once()

        # Run scheduler loop
        logger.info("Entering scheduler loop (press Ctrl+C to exit)")

        try:
            while True:
                schedule.run_pending()
                await asyncio.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Shutting down...")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backup Rotator - Automated backup file rotation with Telegram notifications"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute deletions (default is dry-run mode)",
    )
    parser.add_argument(
        "--once", action="store_true", help="Run once and exit (don't start scheduler)"
    )
    parser.add_argument(
        "--send-weekly-now",
        action="store_true",
        help="Send weekly summary immediately and exit",
    )

    args = parser.parse_args()

    # Determine dry-run mode
    dry_run = not args.execute

    try:
        # Setup logger first (before creating BackupRotator)
        # We'll use default config initially, then reload with actual config
        temp_logger = setup_logger()

        # Create rotator instance
        rotator = BackupRotator(args.config, dry_run=dry_run)

        # Re-setup logger with actual config
        global logger
        logger = setup_logger(
            log_directory=rotator.config.logging.directory,
            log_level=rotator.config.logging.level,
        )

        if dry_run:
            logger.warning("Running in DRY RUN mode - no files will be deleted")
            logger.warning("Use --execute flag to actually delete files")

        # Run based on mode
        if args.send_weekly_now:
            asyncio.run(rotator.send_weekly_summary())
        elif args.once:
            asyncio.run(rotator.run_once())
        else:
            asyncio.run(rotator.run_with_scheduler())

        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
