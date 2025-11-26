"""State management for tracking deletions, alerts, and reports."""

import json
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Optional
from dataclasses import dataclass, asdict, field


logger = logging.getLogger(__name__)


@dataclass
class ProjectState:
    """State tracking for a single project."""

    project_id: str
    last_deletion_timestamp: Optional[str] = None  # ISO format
    deleted_files_count: int = 0
    deleted_files_size_bytes: int = 0

    def reset_deletion_stats(self) -> None:
        """Reset deletion statistics (called after weekly report)."""
        self.deleted_files_count = 0
        self.deleted_files_size_bytes = 0


@dataclass
class GlobalState:
    """Global state tracking."""

    last_weekly_report_timestamp: Optional[str] = None  # ISO format
    disk_alert_last_sent_date: Optional[str] = None  # YYYY-MM-DD format
    projects: dict[str, ProjectState] = field(default_factory=dict)

    def get_project_state(self, project_id: str) -> ProjectState:
        """Get or create project state."""
        if project_id not in self.projects:
            self.projects[project_id] = ProjectState(project_id=project_id)
        return self.projects[project_id]

    def reset_all_deletion_stats(self) -> None:
        """Reset deletion statistics for all projects."""
        for project_state in self.projects.values():
            project_state.reset_deletion_stats()

    def should_send_disk_alert(self) -> bool:
        """Check if disk alert should be sent today."""
        today = date.today().isoformat()
        return self.disk_alert_last_sent_date != today

    def mark_disk_alert_sent(self) -> None:
        """Mark that disk alert was sent today."""
        self.disk_alert_last_sent_date = date.today().isoformat()

    def clear_disk_alert(self) -> None:
        """Clear disk alert state (when space is recovered)."""
        self.disk_alert_last_sent_date = None

    def record_deletion(self, project_id: str, file_size_bytes: int) -> None:
        """Record a file deletion."""
        project_state = self.get_project_state(project_id)
        project_state.deleted_files_count += 1
        project_state.deleted_files_size_bytes += file_size_bytes
        project_state.last_deletion_timestamp = datetime.now().isoformat()

    def mark_weekly_report_sent(self) -> None:
        """Mark that weekly report was sent."""
        self.last_weekly_report_timestamp = datetime.now().isoformat()


class StateManager:
    """Manages persistent state storage."""

    def __init__(self, state_file_path: str = "state.json"):
        """Initialize state manager.

        Args:
            state_file_path: Path to state JSON file
        """
        self.state_file_path = Path(state_file_path)
        self.state: GlobalState = self._load_state()

    def _load_state(self) -> GlobalState:
        """Load state from file or return empty state.

        Returns:
            GlobalState object
        """
        if not self.state_file_path.exists():
            logger.info(
                f"State file not found: {self.state_file_path}. Starting with empty state."
            )
            return GlobalState()

        try:
            with open(self.state_file_path, "r") as f:
                data = json.load(f)

            # Reconstruct GlobalState from dict
            projects_data = data.get("projects", {})
            projects = {
                pid: ProjectState(**pstate_dict)
                for pid, pstate_dict in projects_data.items()
            }

            return GlobalState(
                last_weekly_report_timestamp=data.get("last_weekly_report_timestamp"),
                disk_alert_last_sent_date=data.get("disk_alert_last_sent_date"),
                projects=projects,
            )
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse state file: {e}. Starting with empty state.")
            return GlobalState()
        except Exception as e:
            logger.error(
                f"Unexpected error loading state: {e}. Starting with empty state."
            )
            return GlobalState()

    def save_state(self) -> None:
        """Save current state to file."""
        try:
            # Convert to dict
            state_dict = {
                "last_weekly_report_timestamp": self.state.last_weekly_report_timestamp,
                "disk_alert_last_sent_date": self.state.disk_alert_last_sent_date,
                "projects": {
                    pid: asdict(pstate) for pid, pstate in self.state.projects.items()
                },
            }

            # Ensure directory exists
            self.state_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write atomically (write to temp file, then rename)
            temp_file = self.state_file_path.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(state_dict, f, indent=2)

            temp_file.replace(self.state_file_path)
            logger.debug(f"State saved to {self.state_file_path}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def get_project_state(self, project_id: str) -> ProjectState:
        """Get project state."""
        return self.state.get_project_state(project_id)

    def record_deletion(self, project_id: str, file_size_bytes: int) -> None:
        """Record a file deletion and save state."""
        self.state.record_deletion(project_id, file_size_bytes)
        self.save_state()

    def should_send_disk_alert(self) -> bool:
        """Check if disk alert should be sent today."""
        return self.state.should_send_disk_alert()

    def mark_disk_alert_sent(self) -> None:
        """Mark disk alert as sent and save state."""
        self.state.mark_disk_alert_sent()
        self.save_state()

    def clear_disk_alert(self) -> None:
        """Clear disk alert state and save."""
        self.state.clear_disk_alert()
        self.save_state()

    def mark_weekly_report_sent(self) -> None:
        """Mark weekly report as sent and save state."""
        self.state.mark_weekly_report_sent()
        self.save_state()

    def reset_all_deletion_stats(self) -> None:
        """Reset deletion statistics for all projects and save."""
        self.state.reset_all_deletion_stats()
        self.save_state()
