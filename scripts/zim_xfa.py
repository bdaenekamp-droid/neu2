#!/usr/bin/env python3
import argparse
import json
import re
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime

from pypdf import PdfReader, PdfWriter


def local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def sanitize_filename(value: str, fallback: str) -> str:
    raw = (value or "").strip() or fallback
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_text = "".join(c for c in normalized if not unicodedata.combining(c))
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", ascii_text).strip("_")
    return cleaned or fallback


def format_currency(value) -> str:
    number = float(value or 0)
    base = f"{number:,.2f}"
    return base.replace(",", "X").replace(".", ",").replace("X", ".")


def format_percent(value) -> str:
    number = float(value or 0)
    return f"{number:.1f}".replace(".", ",")


def parse_date(value: str):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    return None


def format_date(value: str) -> str:
    dt = parse_date(value)
    return dt.strftime("%d.%m.%Y") if dt else (value or "")


def format_month_year(value: str) -> str:
    dt = parse_date(value)
    return dt.strftime("%m.%Y") if dt else ""


def normalize_pdf_name(name: str | None) -> str:
    if not name:
        return ""
    return str(name).replace("/", "")


def get_acroform(reader: PdfReader):
    root = reader.trailer.get("/Root")
    if root is None:
        return None
    acro = root.get("/AcroForm")
    if acro is None:
        return None
    return acro.get_object() if hasattr(acro, "get_object") else acro


def get_xfa_parts(reader: PdfReader):
    acro = get_acroform(reader)
    if acro is None:
        return {}
    xfa = acro.get("/XFA")
    if xfa is None:
        return {}
    xfa = xfa.get_object() if hasattr(xfa, "get_object") else xfa

    parts = {}
    if isinstance(xfa, list):
        for i in range(0, len(xfa), 2):
            if i + 1 >= len(xfa):
                continue
            name = str(xfa[i])
            obj = xfa[i + 1]
            obj = obj.get_object() if hasattr(obj, "get_object") else obj
            parts[name] = obj
    return parts


def get_xfa_datasets_stream(reader: PdfReader):
    parts = get_xfa_parts(reader)
    stream = parts.get("datasets")
    if stream is None:
        raise ValueError("XFA datasets stream nicht gefunden.")
    return stream


def find_formular(root: ET.Element):
    data_node = None
    for node in root.iter():
        if local_name(node.tag) == "data":
            data_node = node
            break
    if data_node is None:
        raise ValueError("<xfa:data> im datasets XML nicht gefunden.")

    for child in data_node:
        if local_name(child.tag) == "formular":
            return child
    for child in data_node:
        if isinstance(child.tag, str):
            return child
    raise ValueError("Kein Datensatz unter <xfa:data> gefunden.")


def list_paths(node: ET.Element, prefix=""):
    name = local_name(node.tag)
    current = f"{prefix}/{name}" if prefix else name
    children = [c for c in list(node) if isinstance(c.tag, str)]
    if not children:
        return [current]
    result = []
    for child in children:
        result.extend(list_paths(child, current))
    return result


def find_node(formular: ET.Element, path: str):
    segments = [s for s in path.split("/") if s]
    if not segments:
        return None
    current = formular
    if local_name(current.tag) != segments[0]:
        return None
    for segment in segments[1:]:
        match = None
        for child in current:
            if isinstance(child.tag, str) and local_name(child.tag) == segment:
                match = child
                break
        if match is None:
            return None
        current = match
    return current


def get_value(formular: ET.Element, path: str) -> str:
    node = find_node(formular, path)
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def set_value(formular: ET.Element, path: str, value):
    node = find_node(formular, path)
    if node is None:
        return False
    node.text = "" if value is None else str(value)
    return True


def build_candidates(payload):
    project = payload.get("project", {})
    company = payload.get("company", {})
    company_funding = company.get("funding", {})
    company_computed = company.get("computed", {})

    start_date = project.get("startDate", "")
    end_date = project.get("endDate", "")

    candidates = {
        "akronym": project.get("name", ""),
        "firma": company.get("name", ""),
        "personalkosten": format_currency(company_computed.get("personnelCost", 0)),
        "projektsumme": format_currency(company_computed.get("projectSum", 0)),
        "foerdersumme": format_currency(company_computed.get("fundingSum", 0)),
        "foerderquote": format_percent(company_funding.get("ratePct", 0)),
        "zuschlag": format_percent(company_funding.get("surchargePct", 0)),
        "realerzuschlag": format_percent(company_computed.get("realSurchargePct", 0)),
        "maximalbetrag": format_currency(company_funding.get("maxProjectSum", 0)),
        "verschenkt": format_currency(company_computed.get("verschenkt", 0)),
        "laufzeit": str(project.get("durationMonths", "")),
        "projektstart": format_date(start_date),
        "projektende": format_date(end_date),
        "monat_von": format_month_year(start_date),
        "monat_bis": format_month_year(end_date),
    }

    for year, amount in (company.get("yearlyFundingSums", {}) or {}).items():
        candidates[f"jahr_{year}"] = format_currency(amount)

    return candidates


def map_fields(paths, payload):
    candidates = build_candidates(payload)
    mappings = []
    unmapped = []

    for path in paths:
        leaf = path.split("/")[-1].lower()
        full = path.lower()

        chosen_key = None
        if leaf == "akronym":
            chosen_key = "akronym"
        elif leaf == "firma" or full.endswith("/teil_1_allg/firma"):
            chosen_key = "firma"
        elif re.search(r"(projektstart|startdatum|lvon|jahr_von)", full):
            chosen_key = "projektstart"
        elif re.search(r"(projektende|endedatum|lbis|jahr_bis)", full):
            chosen_key = "projektende"
        elif re.search(r"(laufzeit|dauer|jahr_akt|monat)", full):
            chosen_key = "laufzeit"
        elif "personalkosten" in full:
            chosen_key = "personalkosten"
        elif re.search(r"(projektsumme|gesamtkosten|projektkosten)", full):
            chosen_key = "projektsumme"
        elif re.search(r"(foerdersumme|fördersumme|zuwendung)", full):
            chosen_key = "foerdersumme"
        elif re.search(r"(foerderquote|förderquote)", full):
            chosen_key = "foerderquote"
        elif "zuschlag" in full:
            chosen_key = "zuschlag"
        elif "maximal" in full:
            chosen_key = "maximalbetrag"
        elif "verschenkt" in full:
            chosen_key = "verschenkt"
        else:
            for year_key in candidates:
                if year_key.startswith("jahr_") and year_key in full:
                    chosen_key = year_key
                    break

        if chosen_key and chosen_key in candidates:
            mappings.append({"path": path, "key": chosen_key, "value": candidates[chosen_key]})
        else:
            unmapped.append(path)

    return mappings, unmapped


def detect_pdf_javascript(reader: PdfReader) -> bool:
    root = reader.trailer.get("/Root")
    names = root.get("/Names") if root else None
    names = names.get_object() if hasattr(names, "get_object") else names
    if not names:
        return False
    return names.get("/JavaScript") is not None


def infer_field_type(name: str, ft: str | None, options):
    lower = (name or "").lower()
    if ft == "Btn":
        if isinstance(options, list) and len(options) > 1:
            return "radio"
        return "checkbox"
    if ft == "Ch":
        return "select"
    if ft == "Tx":
        if any(token in lower for token in ["bem", "beschr", "text", "addr", "anschrift"]):
            return "textarea"
        return "text"
    if ft == "Sig":
        return "signature"
    if re.search(r"(datum|date|_von|_bis|geb)", lower):
        return "date"
    if re.search(r"(mail|email)", lower):
        return "email"
    if re.search(r"(nr|zahl|betrag|sum|kosten|pm|quote|proz|jahr)", lower):
        return "number"
    return "text"


def to_storage_key(raw: str) -> str:
    return "pdf::" + re.sub(r"[^\w]+", "_", raw or "").strip("_")


def extract_widget_info(reader: PdfReader):
    by_name = {}
    for page_index, page in enumerate(reader.pages):
        annots = page.get("/Annots") or []
        for annot_ref in annots:
            annot = annot_ref.get_object() if hasattr(annot_ref, "get_object") else annot_ref
            if normalize_pdf_name(annot.get("/Subtype")) != "Widget":
                continue
            name = annot.get("/T")
            if not name:
                parent = annot.get("/Parent")
                parent = parent.get_object() if hasattr(parent, "get_object") else parent
                name = parent.get("/T") if parent else None
            if not name:
                continue
            rect = annot.get("/Rect")
            normalized_name = str(name)
            entry = by_name.setdefault(normalized_name, {"pages": set(), "rects": []})
            entry["pages"].add(page_index + 1)
            if isinstance(rect, list) and len(rect) == 4:
                entry["rects"].append([float(rect[0]), float(rect[1]), float(rect[2]), float(rect[3])])

    return {
        name: {"pages": sorted(list(data["pages"])), "rects": data["rects"]}
        for name, data in by_name.items()
    }


def extract_acro_fields(reader: PdfReader, widget_info):
    acro = get_acroform(reader)
    if acro is None:
        return []

    fields = acro.get("/Fields") or []
    result = []

    def walk(field_ref, parents):
        field = field_ref.get_object() if hasattr(field_ref, "get_object") else field_ref
        t_name = field.get("/T")
        full_parts = parents + ([str(t_name)] if t_name else [])
        kids = field.get("/Kids") or []
        ft = normalize_pdf_name(field.get("/FT")) if field.get("/FT") else None

        if ft or not kids:
            full_name = ".".join([part for part in full_parts if part])
            opts = field.get("/Opt")
            normalized_opts = []
            if isinstance(opts, list):
                for opt in opts:
                    if isinstance(opt, list) and opt:
                        normalized_opts.append(str(opt[-1]))
                    else:
                        normalized_opts.append(str(opt))
            flags = int(field.get("/Ff") or 0)
            required = bool(flags & (1 << 1))
            widget = widget_info.get(full_name) or widget_info.get(str(t_name or "")) or {"pages": [], "rects": []}
            result.append({
                "id": full_name or str(t_name or f"field_{len(result)+1}"),
                "name": str(t_name or full_name or ""),
                "fullName": full_name or str(t_name or ""),
                "label": str(field.get("/TU") or t_name or full_name or "Feld"),
                "type": infer_field_type(full_name or str(t_name or ""), ft, normalized_opts),
                "pdfFieldType": ft,
                "value": "" if field.get("/V") is None else str(field.get("/V")),
                "defaultValue": "" if field.get("/DV") is None else str(field.get("/DV")),
                "required": required,
                "options": normalized_opts,
                "page": widget["pages"][0] if widget["pages"] else None,
                "pages": widget["pages"],
                "rect": widget["rects"][0] if widget["rects"] else None,
                "rects": widget["rects"],
                "group": f"Seite {widget['pages'][0]}" if widget["pages"] else "AcroForm",
                "parent": ".".join(parents) if parents else None,
                "source": "AcroForm",
                "storageKey": to_storage_key(full_name or str(t_name or "")),
            })

        for kid in kids:
            walk(kid, full_parts)

    for field in fields:
        walk(field, [])

    unique = {}
    for field in result:
        unique[field["id"]] = field
    return list(unique.values())


def extract_xfa_schema(reader: PdfReader, formular_paths):
    parts = get_xfa_parts(reader)
    template_part = parts.get("template")
    if template_part is None:
        return []

    template_xml = template_part.get_data()
    root = ET.fromstring(template_xml)
    fields = []

    def walk(node, parent_segments, group_name):
        tag = local_name(node.tag)
        if tag == "subform":
            name = node.attrib.get("name") or group_name
            next_group = name or group_name
            for child in list(node):
                if isinstance(child.tag, str):
                    walk(child, parent_segments, next_group)
            return

        if tag == "field":
            name = node.attrib.get("name") or f"xfa_field_{len(fields)+1}"
            full = "/".join([segment for segment in [*parent_segments, name] if segment])
            label_node = node.find(".//{*}caption/{*}value/{*}text")
            raw_label = (label_node.text or "").strip() if label_node is not None and label_node.text else ""
            ui_type = "text"
            for child in node.iter():
                child_name = local_name(child.tag)
                if child_name in {"checkButton", "choiceList", "dateTimeEdit", "numericEdit", "textEdit", "passwordEdit"}:
                    ui_type = {
                        "checkButton": "checkbox",
                        "choiceList": "select",
                        "dateTimeEdit": "date",
                        "numericEdit": "number",
                        "textEdit": "text",
                        "passwordEdit": "text",
                    }[child_name]
                    break
            fields.append({
                "id": full,
                "name": name,
                "fullName": full,
                "label": raw_label or name,
                "type": ui_type,
                "required": False,
                "options": [],
                "page": None,
                "rect": None,
                "group": group_name or "XFA",
                "parent": "/".join(parent_segments) if parent_segments else None,
                "source": "XFA",
                "storageKey": to_storage_key(full),
            })
            return

        for child in list(node):
            if isinstance(child.tag, str):
                walk(child, parent_segments, group_name)

    walk(root, ["formular"], "XFA")

    by_name = {entry["fullName"]: entry for entry in fields}
    for path in formular_paths:
        if path in by_name:
            continue
        leaf = path.split("/")[-1]
        by_name[path] = {
            "id": path,
            "name": leaf,
            "fullName": path,
            "label": leaf,
            "type": infer_field_type(path, None, None),
            "required": False,
            "options": [],
            "page": None,
            "rect": None,
            "group": path.split("/")[1] if len(path.split("/")) > 1 else "XFA",
            "parent": "/".join(path.split("/")[:-1]) or None,
            "source": "XFA-datasets",
            "storageKey": to_storage_key(path),
        }

    return list(by_name.values())


def read_pdf_context(input_path, payload):
    reader = PdfReader(input_path)
    page_count = len(reader.pages)
    has_acro = get_acroform(reader) is not None
    xfa_parts = get_xfa_parts(reader)
    has_xfa = bool(xfa_parts)

    formular = None
    root = None
    datasets_stream = None
    all_paths = []
    mappings = []
    unmapped = []
    pdf_akronym = None

    if has_xfa and "datasets" in xfa_parts:
        datasets_stream = xfa_parts["datasets"]
        xml_bytes = datasets_stream.get_data()
        root = ET.fromstring(xml_bytes)
        formular = find_formular(root)
        all_paths = list_paths(formular)
        mappings, unmapped = map_fields(all_paths, payload)
        pdf_akronym = get_value(formular, "formular/akronym") or get_value(formular, "akronym")

    project_name = (payload.get("project", {}) or {}).get("name", "")
    mismatch = bool(pdf_akronym and project_name and pdf_akronym.strip().lower() != project_name.strip().lower())

    widget_info = extract_widget_info(reader)
    acro_fields = extract_acro_fields(reader, widget_info) if has_acro else []
    xfa_fields = extract_xfa_schema(reader, all_paths) if has_xfa else []

    merged = {}
    for field in acro_fields + xfa_fields:
        merged[field["storageKey"]] = field
    fields = list(merged.values())

    fields_per_page = {}
    source_counts = {}
    for field in fields:
        page_label = str(field.get("page") or "unknown")
        fields_per_page[page_label] = fields_per_page.get(page_label, 0) + 1
        source = field.get("source") or "unknown"
        source_counts[source] = source_counts.get(source, 0) + 1

    analysis = {
        "pdfLoaded": True,
        "pageCount": page_count,
        "hasAcroForm": has_acro,
        "hasXfa": has_xfa,
        "hasJavaScript": detect_pdf_javascript(reader),
        "hasNestedFieldTree": any((field.get("parent") for field in acro_fields)),
        "hasWidgetAnnotations": bool(widget_info),
        "acroFieldCount": len(acro_fields),
        "xfaFieldCount": len(xfa_fields),
        "totalFieldCount": len(fields),
        "fieldsPerPage": fields_per_page,
        "fieldSources": source_counts,
        "unmappedFields": unmapped,
    }

    return {
        "reader": reader,
        "datasets_stream": datasets_stream,
        "root": root,
        "formular": formular,
        "paths": all_paths,
        "mappings": mappings,
        "unmapped": unmapped,
        "pdfAkronym": pdf_akronym,
        "projectName": project_name,
        "mismatch": mismatch,
        "analysis": analysis,
        "fields": fields,
    }


def write_filled_pdf(context, output_path):
    if context["formular"] is None:
        raise ValueError("Die hochgeladene PDF enthält kein XFA datasets Formular.")

    for entry in context["mappings"]:
        set_value(context["formular"], entry["path"], entry["value"])

    new_xml = ET.tostring(context["root"], encoding="utf-8", xml_declaration=True)
    context["datasets_stream"].set_data(new_xml)

    writer = PdfWriter()
    writer.clone_document_from_reader(context["reader"])
    with open(output_path, "wb") as f:
        writer.write(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["analyze", "fill"])
    parser.add_argument("--input", required=True)
    parser.add_argument("--payload", required=True)
    parser.add_argument("--output")
    parser.add_argument("--confirm-mismatch", default="false")
    args = parser.parse_args()

    payload = json.loads(args.payload)
    context = read_pdf_context(args.input, payload)

    if args.action == "analyze":
        print(json.dumps({
            "pageCount": context["analysis"]["pageCount"],
            "leafPaths": context["paths"],
            "mappings": context["mappings"],
            "unmappedPaths": context["unmapped"],
            "pdfAkronym": context["pdfAkronym"],
            "projectName": context["projectName"],
            "mismatch": context["mismatch"],
            "analysis": context["analysis"],
            "fields": context["fields"],
        }))
        return

    confirm_mismatch = args.confirm_mismatch.lower() == "true"
    if context["mismatch"] and not confirm_mismatch:
        print(json.dumps({
            "error": "Akronym in PDF passt nicht zum Projekt.",
            "pdfAkronym": context["pdfAkronym"],
            "projectName": context["projectName"],
            "mismatch": True,
        }))
        return

    if not args.output:
        raise ValueError("--output ist für fill erforderlich")

    write_filled_pdf(context, args.output)

    company_name = ((payload.get("company") or {}).get("name") or "Unternehmen")
    project_name = ((payload.get("project") or {}).get("name") or "Projekt")
    download_name = f"{sanitize_filename(project_name, 'Projekt')}_{sanitize_filename(company_name, 'Unternehmen')}_Mantelbogen.pdf"

    print(json.dumps({
        "pdfAkronym": context["pdfAkronym"],
        "projectName": context["projectName"],
        "mismatch": context["mismatch"],
        "downloadName": download_name,
        "filledCount": len(context["mappings"]),
    }))


if __name__ == "__main__":
    main()
