"""
Migration Rules Module

Contains Spark migration rules and before/after examples for
version upgrades and best practice implementations.
"""

from .format_string_rule import FormatStringMigration

__all__ = ['FormatStringMigration']