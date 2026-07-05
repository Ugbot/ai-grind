"""Shared build/package-manager engine.

One normalized vocabulary (models), execution helpers (exec/jsrun), output
parsers (parsers/jsdeps), Polars analysis frames (analysis), and bounded
summaries (formatters) shared by the Maven, Gradle, npm, pnpm, yarn and Cargo
backends. Backends contribute only their tool-specific console parsers; the
normalized shapes and the query surface live here.
"""

from __future__ import annotations
