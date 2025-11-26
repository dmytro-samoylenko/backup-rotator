"""Configuration validation using Pydantic models."""

from typing import Optional
from pydantic import BaseModel, Field, field_validator
import re


class TelegramConfig(BaseModel):
    """Telegram notification configuration."""

    bot_token: str = Field(..., min_length=1, description="Telegram bot token")
    chat_id: str = Field(..., min_length=1, description="Telegram chat ID")


class BackupsConfig(BaseModel):
    """Backups directory configuration."""

    base_path: str = Field(..., min_length=1, description="Base path for backups")


class DiskConfig(BaseModel):
    """Disk monitoring configuration."""

    threshold_percent: float = Field(
        ..., ge=0, le=100, description="Disk space threshold percentage"
    )
    margin_percent: float = Field(
        ..., ge=0, le=100, description="Margin for alert resolution"
    )


class LoggingConfig(BaseModel):
    """Logging configuration."""

    directory: str = Field(default="logs", description="Directory for log files")
    level: str = Field(default="INFO", description="Logging level")

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Validate logging level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(
                f"Invalid logging level. Must be one of: {', '.join(valid_levels)}"
            )
        return v.upper()


class RotationScheduleConfig(BaseModel):
    """Rotation schedule configuration."""

    frequency: str = Field(
        default="daily",
        description="Rotation frequency: hourly, daily, weekly, or HH:MM format",
    )
    time: str = Field(
        default="03:00",
        pattern=r"^\d{2}:\d{2}$",
        description="Time for daily/custom rotations in HH:MM format",
    )

    @field_validator("frequency")
    @classmethod
    def validate_frequency(cls, v: str) -> str:
        """Validate rotation frequency."""
        valid_frequencies = ["hourly", "daily", "weekly"]
        # Check if it's a valid frequency or a time pattern (HH:MM)
        if v.lower() not in valid_frequencies:
            # Check if it matches HH:MM pattern
            import re

            if not re.match(r"^\d{2}:\d{2}$", v):
                raise ValueError(
                    f"Invalid frequency. Must be one of: {', '.join(valid_frequencies)} or HH:MM format"
                )
        return v.lower() if v.lower() in valid_frequencies else v


class WeeklyReportConfig(BaseModel):
    """Weekly report scheduling configuration."""

    day: str = Field(..., description="Day of week for weekly report")
    time: str = Field(..., pattern=r"^\d{2}:\d{2}$", description="Time in HH:MM format")

    @field_validator("day")
    @classmethod
    def validate_day(cls, v: str) -> str:
        """Validate day of week."""
        valid_days = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]
        if v.lower() not in valid_days:
            raise ValueError(f"Invalid day. Must be one of: {', '.join(valid_days)}")
        return v.lower()


class RetentionPolicy(BaseModel):
    """Backup retention policy configuration."""

    daily: int = Field(..., ge=0, description="Number of daily backups to keep")
    weekly: int = Field(..., ge=0, description="Number of weekly backups to keep")
    monthly: int = Field(..., ge=0, description="Number of monthly backups to keep")


class ProjectConfig(BaseModel):
    """Per-project configuration."""

    id: str = Field(..., min_length=1, description="Project ID")
    name: str = Field(..., min_length=1, description="Project name")
    filename_pattern: str = Field(
        ..., min_length=1, description="Regex pattern for backup files"
    )
    expected_interval_hours: int = Field(
        ..., gt=0, description="Expected backup interval in hours"
    )
    min_file_size_mb: float = Field(
        default=1.0, ge=0, description="Minimum file size in MB"
    )
    retention: Optional[RetentionPolicy] = Field(
        None, description="Custom retention policy"
    )

    @field_validator("filename_pattern")
    @classmethod
    def validate_pattern(cls, v: str) -> str:
        """Validate regex pattern."""
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
        return v


class Config(BaseModel):
    """Main configuration model."""

    telegram: TelegramConfig
    backups: BackupsConfig
    disk: DiskConfig
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    rotation_schedule: RotationScheduleConfig = Field(
        default_factory=RotationScheduleConfig
    )
    weekly_report: WeeklyReportConfig
    datetime_formats: list[str] = Field(
        ..., min_length=1, description="List of datetime format patterns"
    )
    default_retention: RetentionPolicy
    projects: list[ProjectConfig] = Field(
        ..., min_length=1, description="List of projects"
    )

    @field_validator("datetime_formats")
    @classmethod
    def validate_datetime_formats(cls, v: list[str]) -> list[str]:
        """Validate datetime format strings."""
        if not v:
            raise ValueError("At least one datetime format must be specified")
        # Basic validation - try to use each format
        from datetime import datetime

        test_date = datetime.now()
        for fmt in v:
            try:
                test_date.strftime(fmt)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid datetime format '{fmt}': {e}")
        return v

    @field_validator("projects")
    @classmethod
    def validate_unique_project_ids(cls, v: list[ProjectConfig]) -> list[ProjectConfig]:
        """Validate that project IDs are unique."""
        ids = [p.id for p in v]
        if len(ids) != len(set(ids)):
            raise ValueError("Project IDs must be unique")
        return v


def load_and_validate_config(config_path: str) -> Config:
    """Load and validate configuration from YAML file.

    Args:
        config_path: Path to configuration YAML file

    Returns:
        Validated Config object

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If configuration is invalid
        yaml.YAMLError: If YAML parsing fails
    """
    import yaml
    from pathlib import Path

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, "r") as f:
        config_data = yaml.safe_load(f)

    if not config_data:
        raise ValueError("Configuration file is empty")

    # Validate and return
    return Config(**config_data)
