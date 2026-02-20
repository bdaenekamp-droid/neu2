#!/usr/bin/env python3
import argparse
import io
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


def get_xfa_datasets_stream(reader: PdfReader):
    acro = reader.trailer["/Root"].get("/AcroForm")
    if acro is None:
        raise ValueError("PDF enthält kein /AcroForm.")
    acro = acro.get_object()
    xfa = acro.get("/XFA")
    if xfa is None:
        raise ValueError("PDF enthält kein /XFA.")

    if hasattr(xfa, "get_object"):
        xfa = xfa.get_object()

    if isinstance(xfa, list):
        for i in range(0, len(xfa), 2):
            name = str(xfa[i])
            if i + 1 >= len(xfa):
                continue
            obj = xfa[i + 1]
            if hasattr(obj, "get_object"):
                obj = obj.get_object()
            if name == "datasets":
                return obj
    raise ValueError("XFA datasets stream nicht gefunden.")


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
    raise ValueError("<formular> unter <xfa:data> nicht gefunden.")


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


def read_pdf_context(input_path, payload):
    reader = PdfReader(input_path)
    datasets_stream = get_xfa_datasets_stream(reader)
    xml_bytes = datasets_stream.get_data()
    root = ET.fromstring(xml_bytes)
    formular = find_formular(root)
    all_paths = list_paths(formular)
    mappings, unmapped = map_fields(all_paths, payload)
    pdf_akronym = get_value(formular, "formular/akronym") or get_value(formular, "akronym")
    project_name = (payload.get("project", {}) or {}).get("name", "")
    mismatch = bool(pdf_akronym and project_name and pdf_akronym.strip().lower() != project_name.strip().lower())
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
    }


def write_filled_pdf(context, output_path):
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
            "leafPaths": context["paths"],
            "mappings": context["mappings"],
            "unmappedPaths": context["unmapped"],
            "pdfAkronym": context["pdfAkronym"],
            "projectName": context["projectName"],
            "mismatch": context["mismatch"],
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
