'''
File description:
Package marker for the NCDT cleaner project.

This module exists so Python treats `ncdt_cleaner` as an importable package.
It intentionally keeps the public surface small and points readers toward the
CLI-driven workflow as the main entrypoint for the project.
'''

__all__ = [
    "cli",
]
