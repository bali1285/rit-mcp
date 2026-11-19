"""Ritchies brand kit.

One tool, `rit_brand`, so any AI making a report, chart, page or slide for
Ritchies can pick up the logo, colours and house rules without anyone
attaching them. The logo files live in `assets/`.
"""

from __future__ import annotations

import base64
from pathlib import Path

ASSETS = Path(__file__).resolve().parent.parent / "assets"


def _data_uri(name: str) -> str | None:
    path = ASSETS / name
    if not path.is_file():
        return None
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()


LOGO = _data_uri("ritchies-logo.png")
ICON = _data_uri("ritchies-icon.png")

BRAND = {
    "name": "Ritchies Transport",
    "colours": {
        "blue": {"hex": "#045293", "use": "Primary. Headings, header bars, key numbers, links."},
        "blue_dark": {"hex": "#033A69", "use": "Text on light backgrounds, reference lines."},
        "red": {"hex": "#EE3224", "use": "Accent. Ritchies' own series in charts, call-outs. Use sparingly."},
        "grey": {"hex": "#9FB3C8", "use": "Competitors / everyone else in charts."},
        "ink": {"hex": "#0F1F33", "use": "Body text."},
        "muted": {"hex": "#5B6B7D", "use": "Captions, axis labels, footnotes."},
        "background": {"hex": "#F5F7FA", "use": "Page background behind white cards."},
    },
    "fonts": {
        "primary": "Inter (Google Fonts), fallback system-ui, Segoe UI, sans-serif",
        "weights": "800 for big numbers and titles, 600-700 for headings, 400 for body",
    },
    "logo": {
        "full": {"width_px": 600, "height_px": 62, "data_uri": LOGO,
                 "use": "Top-left of reports and pages, on white. Show at 24-32 px tall."},
        "icon": {"width_px": 120, "height_px": 62, "data_uri": ICON,
                 "use": "Small spaces: favicons, slide corners, chart watermarks."},
        "rules": [
            "Always on a white or very light background. Never recolour, stretch or add effects.",
            "Leave clear space around it at least the height of the circle.",
        ],
    },
    "charts": [
        "Ritchies is always red (#EE3224); every competitor or comparison group is grey (#9FB3C8).",
        "Use blue (#045293) for neutral single-series charts and headline numbers.",
        "Highlight one thing per chart. Put the finding in the chart title, e.g. 'North Shore is the gap'.",
        "Label values directly on bars rather than relying on a legend.",
    ],
    "writing": [
        "Kiwi English (colour, organisation). Plain, direct, short sentences.",
        "Lead with the answer and its number, then the detail.",
        "Say where the data came from and its time range in a footnote.",
    ],
}

INSTRUCTIONS = (
    "rit_brand: before making any report, chart, page, slide or document for Ritchies, "
    "call rit_brand and use its logo (data URI, embed it directly), colours and rules. "
    "Nobody needs to attach the logo."
)


def rit_brand(include_logo: bool = True) -> dict:
    """Ritchies Transport brand kit: logo (full and icon, as PNG data URIs you
    can put straight into an <img src> or a slide), brand colours with when to
    use each, fonts, and chart and writing rules. Call this before making any
    report, chart, page or deck for Ritchies. Set include_logo=false if you only
    need the colours."""
    if include_logo:
        return BRAND
    trimmed = {**BRAND, "logo": {k: ({kk: vv for kk, vv in v.items() if kk != "data_uri"} if isinstance(v, dict) else v)
                                 for k, v in BRAND["logo"].items()}}
    return trimmed


TOOLS = {"rit_brand": rit_brand}


def register(mcp) -> None:
    for name, fn in TOOLS.items():
        mcp.tool(name=name)(fn)
