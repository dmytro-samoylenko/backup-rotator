"""Disk space monitoring with hysteresis."""

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


class DiskMonitor:
    """Monitors disk space with hysteresis to prevent alert flapping."""

    def __init__(self, path: str, threshold_percent: float, margin_percent: float):
        """Initialize disk monitor.

        Args:
            path: Path to monitor (e.g., /backups)
            threshold_percent: Alert when free space drops below this percentage
            margin_percent: Clear alert when free space rises above threshold + margin
        """
        self.path = Path(path)
        self.threshold_percent = threshold_percent
        self.margin_percent = margin_percent
        self.resolution_threshold_percent = threshold_percent + margin_percent

    def get_disk_usage(self) -> tuple[float, float, float, float]:
        """Get disk usage statistics.

        Returns:
            Tuple of (free_percent, free_gb, used_gb, total_gb)

        Raises:
            FileNotFoundError: If path doesn't exist
            OSError: If disk usage cannot be determined
        """
        if not self.path.exists():
            raise FileNotFoundError(f"Path does not exist: {self.path}")

        usage = shutil.disk_usage(self.path)

        total_gb = usage.total / (1024**3)
        used_gb = usage.used / (1024**3)
        free_gb = usage.free / (1024**3)
        free_percent = (usage.free / usage.total) * 100

        return free_percent, free_gb, used_gb, total_gb

    def should_alert(self) -> tuple[bool, float, float, float]:
        """Check if disk space alert should be triggered.

        Returns:
            Tuple of (should_alert, free_percent, free_gb, total_gb)
        """
        try:
            free_percent, free_gb, used_gb, total_gb = self.get_disk_usage()

            should_alert = free_percent < self.threshold_percent

            if should_alert:
                logger.warning(
                    f"Disk space below threshold: {free_percent:.1f}% free "
                    f"(threshold: {self.threshold_percent}%)"
                )
            else:
                logger.debug(f"Disk space OK: {free_percent:.1f}% free")

            return should_alert, free_percent, free_gb, total_gb

        except Exception as e:
            logger.error(f"Failed to check disk usage: {e}")
            # Don't alert on errors to avoid false positives
            return False, 0.0, 0.0, 0.0

    def is_resolved(self) -> bool:
        """Check if disk space issue is resolved (with hysteresis).

        Returns:
            True if free space is above resolution threshold, False otherwise
        """
        try:
            free_percent, _, _, _ = self.get_disk_usage()

            is_resolved = free_percent > self.resolution_threshold_percent

            if is_resolved:
                logger.info(
                    f"Disk space recovered: {free_percent:.1f}% free "
                    f"(resolution threshold: {self.resolution_threshold_percent}%)"
                )

            return is_resolved

        except Exception as e:
            logger.error(f"Failed to check disk resolution: {e}")
            return False


def create_disk_monitor(
    path: str, threshold_percent: float, margin_percent: float
) -> DiskMonitor:
    """Create disk monitor instance.

    Args:
        path: Path to monitor
        threshold_percent: Alert threshold
        margin_percent: Resolution margin

    Returns:
        DiskMonitor instance
    """
    return DiskMonitor(path, threshold_percent, margin_percent)
