from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any

from lxml import etree
from pypdf import PdfReader, PdfWriter


@dataclass
class XfaContext:
    reader: PdfReader
    datasets_stream: Any
    xml_root: etree._Element
    data_root: etree._Element
    field_paths: list[str]


def local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def slugify_ascii(value: str, fallback: str) -> str:
    raw = normalize_text(value) or fallback
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_text = "".join(c for c in normalized if not unicodedata.combining(c))
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", ascii_text).strip("_")
    return cleaned or fallback


def parse_date(value: str | None) -> datetime | None:
    text = normalize_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def format_date(value: str | None) -> str:
    dt = parse_date(value)
    return dt.strftime("%d.%m.%Y") if dt else normalize_text(value)


def format_month_year(value: str | None) -> str:
    dt = parse_date(value)
    return dt.strftime("%m.%Y") if dt else ""


def format_euro(value: Any) -> str:
    number = float(value or 0)
    return f"{number:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def format_percent(value: Any, *, with_symbol: bool) -> str:
    number = float(value or 0)
    base = f"{number:.1f}".replace(".", ",")
    return f"{base} %" if with_symbol else base


def _segment_name(element: etree._Element, siblings: list[etree._Element]) -> str:
    name = local_name(element.tag)
    same = [s for s in siblings if local_name(s.tag) == name]
    if len(same) <= 1:
        return name
    idx = same.index(element)
    return f"{name}[{idx}]"


def _leaf_paths(node: etree._Element, prefix: str = "") -> list[str]:
    children = [c for c in node if isinstance(c.tag, str)]
    if not children:
        return [prefix]

    results: list[str] = []
    for child in children:
        seg = _segment_name(child, children)
        child_path = f"{prefix}/{seg}" if prefix else seg
        results.extend(_leaf_paths(child, child_path))
    return results


def _split_segment(segment: str) -> tuple[str, int | None]:
    match = re.match(r"^(.+?)\[(\d+)\]$", segment)
    if not match:
        return segment, None
    return match.group(1), int(match.group(2))


def find_node(data_root: etree._Element, path: str) -> etree._Element | None:
    segments = [s for s in path.split("/") if s]
    if not segments:
        return None
    if local_name(data_root.tag) != _split_segment(segments[0])[0]:
        return None

    current = data_root
    for seg in segments[1:]:
        name, index = _split_segment(seg)
        candidates = [
            child
            for child in current
            if isinstance(child.tag, str) and local_name(child.tag) == name
        ]
        if not candidates:
            return None
        if index is None:
            current = candidates[0]
        elif index < len(candidates):
            current = candidates[index]
        else:
            return None
    return current


def get_node_text(data_root: etree._Element, path: str) -> str:
    node = find_node(data_root, path)
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def set_node_text(data_root: etree._Element, path: str, value: Any) -> bool:
    node = find_node(data_root, path)
    if node is None:
        return False
    node.text = normalize_text(value)
    return True


def _get_xfa_datasets_stream(reader: PdfReader):
    root = reader.trailer.get("/Root")
    if root is None:
        return None
    acro = root.get("/AcroForm")
    if acro is None:
        return None
    acro = acro.get_object() if hasattr(acro, "get_object") else acro
    xfa = acro.get("/XFA")
    if xfa is None:
        return None
    xfa = xfa.get_object() if hasattr(xfa, "get_object") else xfa

    if isinstance(xfa, list):
        for i in range(0, len(xfa), 2):
            if i + 1 >= len(xfa):
                continue
            part_name = str(xfa[i])
            part_obj = xfa[i + 1]
            part_obj = part_obj.get_object() if hasattr(part_obj, "get_object") else part_obj
            if part_name == "datasets":
                return part_obj
    return None


def _find_xfa_data_root(xml_root: etree._Element) -> etree._Element:
    data_node = None
    for node in xml_root.iter():
        if isinstance(node.tag, str) and local_name(node.tag) == "data":
            data_node = node
            break
    if data_node is None:
        raise ValueError("<xfa:data> im datasets XML nicht gefunden.")

    for child in data_node:
        if isinstance(child.tag, str):
            return child
    raise ValueError("Kein Daten-Root unter <xfa:data> gefunden.")


def load_xfa_context(pdf_bytes: bytes) -> XfaContext | None:
    reader = PdfReader(BytesIO(pdf_bytes))
    datasets_stream = _get_xfa_datasets_stream(reader)
    if datasets_stream is None:
        return None
    xml_bytes = datasets_stream.get_data()
    xml_root = etree.fromstring(xml_bytes)
    data_root = _find_xfa_data_root(xml_root)
    root_name = local_name(data_root.tag)
    leaf_paths = _leaf_paths(data_root, root_name)
    return XfaContext(reader, datasets_stream, xml_root, data_root, leaf_paths)


def _contains_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def _extract_pdf_acronym(data_root: etree._Element, field_paths: list[str]) -> str | None:
    keywords = ["akronym", "projektakronym", "projektname", "kurzbezeichnung"]
    for path in field_paths:
        low = path.lower()
        if _contains_any(low, keywords):
            val = get_node_text(data_root, path)
            if val:
                return val
    return None


def build_field_value_map(payload: dict[str, Any], existing_field_paths: list[str]) -> dict[str, dict[str, str]]:
    project = payload.get("project") or {}
    company = payload.get("company") or {}
    funding = company.get("funding") or {}
    computed = company.get("computed") or {}

    project_name = normalize_text(project.get("name"))
    company_name = normalize_text(company.get("name"))
    start = format_date(project.get("startDate"))
    end = format_date(project.get("endDate"))
    duration = normalize_text(project.get("durationMonths"))

    value_map: dict[str, dict[str, str]] = {}

    for path in existing_field_paths:
        low = path.lower()
        source = ""
        value = ""

        if "akronym" in low or "projektname" in low:
            source, value = "project.name", project_name
        elif "firma" in low or "unternehmensname" in low:
            source, value = "company.name", company_name
        elif _contains_any(low, ["beginn", "start", "von"]):
            source, value = "project.startDate", start
        elif _contains_any(low, ["ende", "bis"]):
            source, value = "project.endDate", end
        elif "dauer" in low:
            source, value = "project.durationMonths", duration
        elif "personalkosten" in low:
            source, value = "company.computed.personnelCost", format_euro(computed.get("personnelCost", 0))
        elif "projektsumme" in low:
            source, value = "company.computed.projectSum", format_euro(computed.get("projectSum", 0))
        elif "foerdersumme" in low or "fördersumme" in low:
            source, value = "company.computed.fundingSum", format_euro(computed.get("fundingSum", 0))
        elif "foerderquote" in low or "förderquote" in low:
            with_symbol = "pct" in low or "prozent" in low
            source, value = "company.funding.ratePct", format_percent(funding.get("ratePct", 0), with_symbol=with_symbol)
        elif "zuschlag" in low or "gemeinkosten" in low or "zuschlagsfaktor" in low:
            with_symbol = "pct" in low or "prozent" in low
            source, value = "company.funding.surchargePct", format_percent(funding.get("surchargePct", 0), with_symbol=with_symbol)
        elif "max" in low and ("foerder" in low or "förder" in low or "projekt" in low) and ("summe" in low or "betrag" in low):
            source, value = "company.funding.maxProjectSum", format_euro(funding.get("maxProjectSum", 0))
        elif "real" in low and "zuschlag" in low:
            with_symbol = "pct" in low or "prozent" in low
            source, value = "company.computed.realSurchargePct", format_percent(computed.get("realSurchargePct", 0), with_symbol=with_symbol)
        elif "verschenkt" in low or "differenz" in low:
            source, value = "company.computed.verschenkt", format_euro(computed.get("verschenkt", 0))

        if source and normalize_text(value):
            value_map[path] = {"value": normalize_text(value), "source": source}

    # deterministic, opt-in AP mapping only if explicit field names exist
    work_packages = payload.get("workPackages") or []
    for idx, wp in enumerate(work_packages):
        n1 = idx + 1
        candidates = {
            f"ap{n1}_nr": normalize_text(wp.get("nr")),
            f"ap{n1}_bezeichnung": normalize_text(wp.get("title")),
            f"ap{n1}_pm": normalize_text(wp.get("pm")),
        }
        for path in existing_field_paths:
            low = path.lower()
            for key, val in candidates.items():
                if key in low and val:
                    value_map[path] = {"value": val, "source": f"workPackages[{idx}]"}

    return value_map


def build_mapping_preview(payload: dict[str, Any], existing_field_paths: list[str]) -> list[dict[str, str]]:
    mapped = build_field_value_map(payload, existing_field_paths)
    preview: list[dict[str, str]] = []
    for path in existing_field_paths:
        if path in mapped:
            preview.append({
                "path": path,
                "value": mapped[path]["value"],
                "source": mapped[path]["source"],
                "status": "willFill",
            })
        else:
            preview.append({"path": path, "value": "", "source": "", "status": "skipped"})
    return preview


def analyze_pdf(pdf_bytes: bytes, payload: dict[str, Any]) -> dict[str, Any]:
    context = load_xfa_context(pdf_bytes)
    project_name = normalize_text((payload.get("project") or {}).get("name")) or None
    if context is None:
        return {
            "isXfa": False,
            "pdfAcronym": None,
            "projectName": project_name,
            "acronymMismatch": None,
            "fields": [],
            "mappingPreview": [],
        }

    pdf_acronym = _extract_pdf_acronym(context.data_root, context.field_paths)
    mismatch = None
    if pdf_acronym and project_name:
        mismatch = pdf_acronym.strip().casefold() != project_name.strip().casefold()

    return {
        "isXfa": True,
        "pdfAcronym": pdf_acronym,
        "projectName": project_name,
        "acronymMismatch": mismatch,
        "fields": context.field_paths,
        "mappingPreview": build_mapping_preview(payload, context.field_paths),
    }


def fill_pdf(pdf_bytes: bytes, payload: dict[str, Any], confirm_mismatch: bool = False) -> tuple[bytes, dict[str, Any]]:
    context = load_xfa_context(pdf_bytes)
    if context is None:
        raise ValueError("Die hochgeladene PDF enthält kein XFA datasets Formular.")

    project_name = normalize_text((payload.get("project") or {}).get("name"))
    pdf_acronym = _extract_pdf_acronym(context.data_root, context.field_paths)
    mismatch = bool(pdf_acronym and project_name and pdf_acronym.strip().casefold() != project_name.strip().casefold())

    if mismatch and not confirm_mismatch:
        raise PermissionError("ACRONYM_MISMATCH")

    value_map = build_field_value_map(payload, context.field_paths)
    filled_count = 0
    for path, mapping in value_map.items():
        if set_node_text(context.data_root, path, mapping["value"]):
            filled_count += 1

    xml_bytes = etree.tostring(context.xml_root, encoding="utf-8", xml_declaration=True)
    context.datasets_stream.set_data(xml_bytes)

    writer = PdfWriter()
    writer.clone_document_from_reader(context.reader)
    output = BytesIO()
    writer.write(output)

    project = slugify_ascii(project_name, "Projekt")
    company = slugify_ascii(normalize_text((payload.get("company") or {}).get("name")), "Unternehmen")

    return output.getvalue(), {
        "pdfAcronym": pdf_acronym,
        "projectName": project_name or None,
        "acronymMismatch": mismatch,
        "downloadName": f"{project}_{company}_Mantelbogen.pdf",
        "filledCount": filled_count,
    }
