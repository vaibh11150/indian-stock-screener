"""
XBRL parser for financial result XML files from BSE/NSE.

XBRL uses XML with specific taxonomies. Indian filings use:
- BSE taxonomy (based on Ind-AS / Companies Act Schedule III)
- MCA taxonomy (for MCA filings)

This parser handles:
1. Different taxonomy versions across years
2. Namespaced tags
3. Context references (period, entity)
4. Unit references (INR, shares)
"""

from datetime import date
from typing import Any, Optional
from lxml import etree

from config.logging_config import get_logger
from scrapers.utils.normalizer import normalize_field

logger = get_logger(__name__)

# Namespace map for Indian XBRL taxonomies
NAMESPACES = {
    "xbrli": "http://www.xbrl.org/2003/instance",
    "in-bse-fin": "http://www.bseindia.com/xbrl/fin",
    "in-bse-shp": "http://www.bseindia.com/xbrl/shp",
    "in-bse-cg": "http://www.bseindia.com/xbrl/cg",
    "xlink": "http://www.w3.org/1999/xlink",
    "iso4217": "http://www.xbrl.org/2003/iso4217",
    "in-gaap": "http://www.xbrl.org/in/gaap",
    "link": "http://www.xbrl.org/2003/linkbase",
}


def parse_xbrl_financial_result(xml_content: bytes) -> dict[str, Any]:
    """
    Parse an XBRL financial result XML file into a structured dict.

    Args:
        xml_content: Raw XML bytes

    Returns:
        Parsed data including:
        {
            'entity': 'INE002A01018',
            'period_start': date(2024, 4, 1),
            'period_end': date(2024, 6, 30),
            'is_audited': False,
            'nature': 'standalone' or 'consolidated',
            'contexts': {...},
            'items': {
                'revenue': 152345.00,
                'other_income': 2345.00,
                ...
            },
            'raw_items': {...}  # Original field names before normalization
        }
    """
    try:
        root = etree.fromstring(xml_content)
    except etree.XMLSyntaxError as e:
        logger.error(f"Failed to parse XBRL XML: {e}")
        return {"error": str(e)}

    result = {
        "entity": None,
        "period_start": None,
        "period_end": None,
        "is_audited": False,
        "nature": "standalone",
        "contexts": {},
        "items": {},
        "raw_items": {},
    }

    # 1. Extract contexts (period and entity information)
    contexts = _extract_contexts(root)
    result["contexts"] = contexts

    # Find the main context (usually the current period)
    main_context = _find_main_context(contexts)
    if main_context:
        result["period_start"] = main_context.get("start")
        result["period_end"] = main_context.get("end") or main_context.get("instant")
        result["entity"] = main_context.get("entity")

    # 2. Extract all facts (financial line items)
    items, raw_items = _extract_facts(root, main_context)
    result["items"] = items
    result["raw_items"] = raw_items

    # 3. Determine if audited
    result["is_audited"] = _check_if_audited(raw_items)

    # 4. Determine nature (standalone vs consolidated)
    result["nature"] = _determine_nature(root, raw_items)

    return result


def _extract_contexts(root: etree._Element) -> dict[str, dict]:
    """Extract all context elements from XBRL document."""
    contexts = {}

    # Try with namespace
    context_elements = root.findall(".//xbrli:context", NAMESPACES)

    # Fall back to finding all context elements without namespace
    if not context_elements:
        context_elements = root.findall(".//{*}context")

    for ctx in context_elements:
        ctx_id = ctx.get("id")
        if not ctx_id:
            continue

        context_data = {"id": ctx_id}

        # Extract entity identifier
        entity_el = ctx.find(".//xbrli:identifier", NAMESPACES) or ctx.find(".//{*}identifier")
        if entity_el is not None and entity_el.text:
            context_data["entity"] = entity_el.text.strip()

        # Extract period information
        period_el = ctx.find(".//xbrli:period", NAMESPACES) or ctx.find(".//{*}period")
        if period_el is not None:
            # Duration period (start and end dates)
            start_el = period_el.find("xbrli:startDate", NAMESPACES) or period_el.find(
                "{*}startDate"
            )
            end_el = period_el.find("xbrli:endDate", NAMESPACES) or period_el.find("{*}endDate")

            if start_el is not None and start_el.text:
                context_data["start"] = _parse_date(start_el.text)
            if end_el is not None and end_el.text:
                context_data["end"] = _parse_date(end_el.text)

            # Instant period (single date)
            instant_el = period_el.find("xbrli:instant", NAMESPACES) or period_el.find(
                "{*}instant"
            )
            if instant_el is not None and instant_el.text:
                context_data["instant"] = _parse_date(instant_el.text)

        # Extract segment/scenario info (for standalone vs consolidated)
        segment_el = ctx.find(".//xbrli:segment", NAMESPACES) or ctx.find(".//{*}segment")
        if segment_el is not None:
            context_data["segment"] = etree.tostring(segment_el, encoding="unicode")

        contexts[ctx_id] = context_data

    return contexts


def _find_main_context(contexts: dict[str, dict]) -> Optional[dict]:
    """
    Find the main context (current reporting period).

    Heuristics:
    1. Prefer contexts with both start and end dates (duration)
    2. Prefer the most recent period
    3. Prefer contexts without segment qualifiers
    """
    duration_contexts = [
        ctx for ctx in contexts.values() if ctx.get("start") and ctx.get("end")
    ]

    if duration_contexts:
        # Sort by end date descending
        duration_contexts.sort(key=lambda x: x.get("end") or date.min, reverse=True)
        return duration_contexts[0]

    # Fall back to instant contexts
    instant_contexts = [ctx for ctx in contexts.values() if ctx.get("instant")]
    if instant_contexts:
        instant_contexts.sort(key=lambda x: x.get("instant") or date.min, reverse=True)
        return instant_contexts[0]

    return None


def _extract_facts(
    root: etree._Element, main_context: Optional[dict]
) -> tuple[dict[str, float], dict[str, Any]]:
    """
    Extract all facts (financial line items) from XBRL document.

    Returns:
        Tuple of (normalized_items, raw_items)
    """
    normalized_items = {}
    raw_items = {}

    main_context_id = main_context.get("id") if main_context else None

    for element in root.iter():
        tag = element.tag
        if "}" in tag:
            ns, local_name = tag.split("}", 1)
            ns = ns.lstrip("{")
        else:
            continue  # Skip elements without namespace

        # Skip XBRL infrastructure elements
        if ns in [
            "http://www.xbrl.org/2003/instance",
            "http://www.xbrl.org/2003/linkbase",
            "http://www.w3.org/1999/xlink",
        ]:
            continue

        # Get context reference
        ctx_ref = element.get("contextRef")
        value = element.text

        if not value or not value.strip():
            continue

        # Try to parse as numeric value
        try:
            # Handle scaling factors
            decimals = element.get("decimals")
            scale = element.get("scale")

            numeric_value = float(value.strip().replace(",", ""))

            # Apply scaling if present
            if scale:
                numeric_value = numeric_value * (10 ** int(scale))

            # Store raw item
            raw_items[local_name] = numeric_value

            # Store with context prefix for context-specific values
            if ctx_ref:
                raw_items[f"{local_name}_{ctx_ref}"] = numeric_value

                # If this is from the main context, also normalize it
                if ctx_ref == main_context_id or main_context_id is None:
                    canonical = normalize_field(local_name)
                    if canonical:
                        normalized_items[canonical] = numeric_value

        except (ValueError, TypeError):
            # Not a numeric value - store as string
            raw_items[local_name] = value.strip()

    return normalized_items, raw_items


def _parse_date(date_str: str) -> Optional[date]:
    """Parse a date string from XBRL (YYYY-MM-DD format)."""
    if not date_str:
        return None
    try:
        from dateutil.parser import parse

        return parse(date_str).date()
    except Exception:
        return None


def _check_if_audited(raw_items: dict) -> bool:
    """Check if the filing is audited based on XBRL fields."""
    audit_indicators = [
        "AuditedUnaudited",
        "WhetherResultsAreAuditedOrUnaudited",
        "NatureOfReportStandaloneConsolidated",
    ]

    for indicator in audit_indicators:
        value = raw_items.get(indicator, "")
        if isinstance(value, str):
            if "audited" in value.lower() and "unaudited" not in value.lower():
                return True

    return False


def _determine_nature(root: etree._Element, raw_items: dict) -> str:
    """Determine if the filing is standalone or consolidated."""
    # Check explicit nature fields
    nature_indicators = [
        "NatureOfReportStandaloneConsolidated",
        "StandaloneConsolidated",
        "TypeOfReport",
    ]

    for indicator in nature_indicators:
        value = raw_items.get(indicator, "")
        if isinstance(value, str):
            if "consolidated" in value.lower():
                return "consolidated"
            if "standalone" in value.lower():
                return "standalone"

    # Check for minority interest (indicates consolidated)
    if raw_items.get("MinorityInterest") or raw_items.get("NonControllingInterests"):
        return "consolidated"

    return "standalone"


def extract_multiple_periods(xml_content: bytes) -> list[dict[str, Any]]:
    """
    Extract data for multiple periods from a single XBRL file.

    Some filings contain comparative data for multiple periods.

    Args:
        xml_content: Raw XML bytes

    Returns:
        List of parsed results, one per period
    """
    try:
        root = etree.fromstring(xml_content)
    except etree.XMLSyntaxError as e:
        logger.error(f"Failed to parse XBRL XML: {e}")
        return []

    contexts = _extract_contexts(root)

    # Group contexts by period
    periods = {}
    for ctx_id, ctx_data in contexts.items():
        period_key = None
        if ctx_data.get("end"):
            period_key = ctx_data["end"]
        elif ctx_data.get("instant"):
            period_key = ctx_data["instant"]

        if period_key:
            if period_key not in periods:
                periods[period_key] = []
            periods[period_key].append(ctx_data)

    results = []
    for period_date, period_contexts in sorted(periods.items(), reverse=True):
        # Use the first context for this period as the main context
        main_ctx = period_contexts[0]
        items, raw_items = _extract_facts(root, main_ctx)

        if items:  # Only include periods with actual data
            results.append(
                {
                    "period_end": period_date,
                    "period_start": main_ctx.get("start"),
                    "entity": main_ctx.get("entity"),
                    "items": items,
                    "raw_items": raw_items,
                    "is_audited": _check_if_audited(raw_items),
                    "nature": _determine_nature(root, raw_items),
                }
            )

    return results
