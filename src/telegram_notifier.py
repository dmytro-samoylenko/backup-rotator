"""Telegram notification service with retry logic."""

import asyncio
import logging
from typing import Optional
from dataclasses import dataclass

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)


@dataclass
class BackupStats:
    """Statistics for a single project's backups."""

    project_id: str
    project_name: str
    total_count: int
    oldest_backup: Optional[str]  # ISO timestamp
    newest_backup: Optional[str]  # ISO timestamp
    total_size_mb: float
    deleted_count: int = 0
    deleted_size_mb: float = 0.0


class TelegramNotifier:
    """Handles Telegram notifications with retry logic."""

    def __init__(self, bot_token: str, chat_id: str):
        """Initialize Telegram notifier.

        Args:
            bot_token: Telegram bot token
            chat_id: Telegram chat ID
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def _send_message(self, text: str, parse_mode: str = "Markdown") -> None:
        """Send message to Telegram with retry logic.

        Args:
            text: Message text
            parse_mode: Parse mode (Markdown or HTML)

        Raises:
            httpx.HTTPError: If all retry attempts fail
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                self.api_url,
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True,
                },
            )
            response.raise_for_status()
            logger.debug(f"Telegram message sent successfully")

    async def send_low_disk_alert(
        self, free_percent: float, free_gb: float, total_gb: float
    ) -> None:
        """Send low disk space alert.

        Args:
            free_percent: Free space percentage
            free_gb: Free space in GB
            total_gb: Total space in GB
        """
        text = (
            "🚨 *LOW DISK SPACE ALERT*\n\n"
            f"Free space on `/backups`: *{free_percent:.1f}%*\n"
            f"Available: {free_gb:.2f} GB / {total_gb:.2f} GB\n\n"
            "⚠️ This alert will be sent daily until resolved."
        )
        try:
            await self._send_message(text)
            logger.info(f"Low disk space alert sent: {free_percent:.1f}% free")
        except Exception as e:
            logger.error(f"Failed to send low disk alert: {e}")

    async def send_missing_backup_alert(
        self,
        project_id: str,
        project_name: str,
        expected_interval_hours: int,
        last_backup_time: Optional[str],
    ) -> None:
        """Send missing backup alert.

        Args:
            project_id: Project ID
            project_name: Project name
            expected_interval_hours: Expected backup interval
            last_backup_time: Last backup timestamp (ISO format)
        """
        last_backup_str = last_backup_time if last_backup_time else "Never"
        text = (
            f"⚠️ *MISSING BACKUP ALERT*\n\n"
            f"Project: *{project_name}* (ID: {project_id})\n"
            f"Expected interval: {expected_interval_hours} hours\n"
            f"Last backup: {last_backup_str}\n\n"
            "No recent backup files detected."
        )
        try:
            await self._send_message(text)
            logger.info(f"Missing backup alert sent for project {project_id}")
        except Exception as e:
            logger.error(f"Failed to send missing backup alert for {project_id}: {e}")

    async def send_undersized_file_alert(
        self,
        project_id: str,
        project_name: str,
        filename: str,
        file_size_mb: float,
        min_size_mb: float,
    ) -> None:
        """Send undersized file alert.

        Args:
            project_id: Project ID
            project_name: Project name
            filename: Backup filename
            file_size_mb: Actual file size in MB
            min_size_mb: Minimum expected size in MB
        """
        text = (
            f"⚠️ *UNDERSIZED BACKUP FILE*\n\n"
            f"Project: *{project_name}* (ID: {project_id})\n"
            f"File: `{filename}`\n"
            f"Size: {file_size_mb:.2f} MB (expected ≥ {min_size_mb:.2f} MB)\n\n"
            "This file is excluded from rotation."
        )
        try:
            await self._send_message(text)
            logger.info(
                f"Undersized file alert sent for {filename} in project {project_id}"
            )
        except Exception as e:
            logger.error(f"Failed to send undersized file alert: {e}")

    async def send_deletion_failure_alert(
        self, project_id: str, project_name: str, filename: str, error: str
    ) -> None:
        """Send deletion failure alert.

        Args:
            project_id: Project ID
            project_name: Project name
            filename: File that failed to delete
            error: Error message
        """
        text = (
            f"❌ *DELETION FAILURE*\n\n"
            f"Project: *{project_name}* (ID: {project_id})\n"
            f"File: `{filename}`\n"
            f"Error: {error}\n"
        )
        try:
            await self._send_message(text)
            logger.info(
                f"Deletion failure alert sent for {filename} in project {project_id}"
            )
        except Exception as e:
            logger.error(f"Failed to send deletion failure alert: {e}")

    async def send_weekly_summary(self, stats: list[BackupStats]) -> None:
        """Send weekly summary report.

        Args:
            stats: List of backup statistics for each project
        """
        if not stats:
            text = "📊 *WEEKLY BACKUP SUMMARY*\n\nNo projects configured."
        else:
            text = "📊 *WEEKLY BACKUP SUMMARY*\n\n"

            for stat in stats:
                text += f"*{stat.project_name}* (ID: {stat.project_id})\n"
                text += f"├ Current backups: {stat.total_count}\n"
                text += f"├ Total size: {stat.total_size_mb:.2f} MB\n"

                if stat.oldest_backup:
                    text += f"├ Oldest: {stat.oldest_backup}\n"
                if stat.newest_backup:
                    text += f"├ Newest: {stat.newest_backup}\n"

                if stat.deleted_count > 0:
                    text += f"├ Deleted this week: {stat.deleted_count} files\n"
                    text += f"└ Space freed: {stat.deleted_size_mb:.2f} MB\n"
                else:
                    text += f"└ No deletions this week\n"

                text += "\n"

        try:
            await self._send_message(text)
            logger.info("Weekly summary sent successfully")
        except Exception as e:
            logger.error(f"Failed to send weekly summary: {e}")

    async def send_general_error(self, error_message: str, context: str = "") -> None:
        """Send general error notification.

        Args:
            error_message: Error message
            context: Additional context
        """
        text = f"❌ *ERROR*\n\n"
        if context:
            text += f"Context: {context}\n"
        text += f"Error: {error_message}"

        try:
            await self._send_message(text)
            logger.info("General error notification sent")
        except Exception as e:
            logger.error(f"Failed to send general error notification: {e}")


def create_notifier(bot_token: str, chat_id: str) -> TelegramNotifier:
    """Create Telegram notifier instance.

    Args:
        bot_token: Telegram bot token
        chat_id: Telegram chat ID

    Returns:
        TelegramNotifier instance
    """
    return TelegramNotifier(bot_token, chat_id)
