"""Data sources. Each module exposes INSTRUCTIONS and register(mcp)."""

from . import at_realtime, brand, ecan

SOURCES = [brand, at_realtime, ecan]
