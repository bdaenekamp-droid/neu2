(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  root.ProjectCsv = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  const SCHEMA_VERSION = "1";
  const DELIMITER = ";";
  const HEADERS = [
    "schema_version", "project_id", "project_name", "section", "entity_type",
    "entity_id", "parent_id", "company_id", "company_name", "work_package_number",
    "employee_name", "field_key", "field_label", "value", "value_type", "unit",
    "sort_order", "value_origin",
  ];
  const VALUE_TYPES = new Set(["string", "number", "boolean", "null", "json", "date", "datetime"]);
  const ENTITY_TYPES = new Set(["metadata", "project", "company", "work_package", "employee", "assignment", "funding_parameter", "zim_field", "setting"]);
  const LABELS = {
    name: "Name", title: "Bezeichnung", annualSalary: "Jahresgehalt", projectPercent: "Projekteinsatz",
    weeklyHours: "Wochenstunden", projectEntryDate: "Projekteintritt", startDate: "Projektstart",
    durationMonths: "Projektdauer", pm: "Personenmonate", start: "Beginn", ende: "Ende",
    createdAt: "Erstellungsdatum", lastModified: "Änderungsdatum", color: "Farbe",
  };
  const UNITS = { annualSalary: "EUR", projectPercent: "Prozent", weeklyHours: "Stunden/Woche", durationMonths: "Monate", pm: "PM", startDate: "Datum", projectEntryDate: "Datum" };

  const own = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
  const pointerEscape = (part) => `${part}`.replace(/~/g, "~0").replace(/\//g, "~1");
  const pointerUnescape = (part) => part.replace(/~1/g, "/").replace(/~0/g, "~");
  const pathToPointer = (path) => `/${path.map(pointerEscape).join("/")}`;
  const parsePointer = (pointer) => pointer.slice(1).split("/").map(pointerUnescape);
  const csvEscape = (value) => {
    const text = value == null ? "" : `${value}`;
    return /[;"\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  };

  function parseCsv(text) {
    const source = `${text || ""}`.replace(/^\uFEFF/, "");
    const rows = [];
    let row = [], value = "", quoted = false;
    for (let i = 0; i < source.length; i += 1) {
      const char = source[i];
      if (quoted) {
        if (char === '"' && source[i + 1] === '"') { value += '"'; i += 1; }
        else if (char === '"') quoted = false;
        else value += char;
      } else if (char === '"') quoted = true;
      else if (char === DELIMITER) { row.push(value); value = ""; }
      else if (char === "\n") { row.push(value.replace(/\r$/, "")); rows.push(row); row = []; value = ""; }
      else value += char;
    }
    if (quoted) throw new Error("Die CSV endet innerhalb eines Anführungszeichens.");
    if (value || row.length) { row.push(value.replace(/\r$/, "")); rows.push(row); }
    return rows;
  }

  function contextFor(path, payload) {
    const stateOffset = path[0] === "state" ? 1 : 0;
    const top = path[stateOffset];
    const companyIndex = ["companyNames", "employeesByCompany", "companyData", "companyHistoryByCompany"].includes(top) ? Number(path[stateOffset + 1]) : null;
    const companyName = Number.isInteger(companyIndex) ? payload.state?.companyNames?.[companyIndex] || `Unternehmen ${companyIndex + 1}` : "";
    let section = "Settings", entityType = "setting", entityId = payload.projectId, parentId = "", wpNumber = "", employeeName = "";
    if (path[0] !== "state") { section = "Project"; entityType = "project"; }
    else if (top === "workPackages") {
      section = "WorkPackages"; entityType = "work_package";
      const wpIndex = Number(path[2]); const isSub = path[3] === "sub"; const subIndex = isSub ? Number(path[4]) : null;
      const wp = payload.state.workPackages?.[wpIndex]; const entity = isSub ? wp?.sub?.[subIndex] : wp;
      entityId = entity?.id || `work-package-${wpIndex}${isSub ? `-${subIndex}` : ""}`;
      parentId = isSub ? wp?.id || "" : ""; wpNumber = isSub ? `${wpIndex + 1}.${subIndex + 1}` : `${wpIndex + 1}`;
      const pmCompanyIndex = path[path.length - 2] === "pm" ? Number(path[path.length - 1]) : null;
      if (Number.isInteger(pmCompanyIndex)) return { section, entityType, entityId, parentId, wpNumber, companyId: `company-${pmCompanyIndex + 1}`, companyName: payload.state.companyNames?.[pmCompanyIndex] || "", employeeName };
    } else if (top === "employeesByCompany") {
      section = "Employees"; entityType = "employee"; const employeeIndex = Number(path[3]);
      const employee = payload.state.employeesByCompany?.[companyIndex]?.[employeeIndex]; employeeName = employee?.name || "";
      entityId = employee?.id || employee?.nummer || `employee-${companyIndex + 1}-${employeeIndex + 1}`;
    } else if (top === "companyNames" || top === "companyData") {
      section = "Companies"; entityType = "company"; entityId = `company-${companyIndex + 1}`;
      if (path.includes("tabellenEintraege")) { section = "WorkPackageAssignments"; entityType = "assignment"; entityId = `${path[path.indexOf("tabellenEintraege") + 1] || "assignment"}@company-${companyIndex + 1}`; }
      if (path.includes("zimMantelboegen")) { section = "ZimForms"; entityType = "zim_field"; }
      if (/foerder|funding|zuschuss/i.test(path.join("."))) { section = "Funding"; entityType = "funding_parameter"; }
    } else if (top === "companyHistoryByCompany") { section = "ProjectPlanning"; entityType = "setting"; entityId = `company-${companyIndex + 1}-history`; }
    else { section = "Project"; entityType = "project"; }
    return { section, entityType, entityId, parentId, wpNumber, companyId: Number.isInteger(companyIndex) ? `company-${companyIndex + 1}` : "", companyName, employeeName };
  }

  function typedValue(value, key) {
    if (value === null) return { value: "", type: "null" };
    if (typeof value === "boolean") return { value: value ? "true" : "false", type: "boolean" };
    if (typeof value === "number") return { value: `${value}`, type: "number" };
    if (typeof value === "string") {
      const type = /date$/i.test(key) ? "date" : /At$/.test(key) ? "datetime" : "string";
      return { value, type };
    }
    return { value: JSON.stringify(value), type: "json" };
  }

  function serializeProjectToCsv(input) {
    if (!input || typeof input !== "object" || !input.state) throw new Error("Kein vollständiges Projekt zum Exportieren vorhanden.");
    const payload = { projectId: `${input.projectId || input.name || ""}`, name: `${input.name || input.projectId || ""}`, createdAt: input.createdAt || "", lastModified: input.lastModified || "", state: input.state };
    if (!payload.projectId || !payload.name) throw new Error("Projekt-ID und Projektname sind erforderlich.");
    const records = [];
    function visit(value, path) {
      if (value && typeof value === "object") {
        // A small container marker preserves the exact distinction between arrays
        // and objects (including objects whose legitimate keys are numeric years).
        if (path.length) records.push({ path, value: Array.isArray(value) ? [] : {}, container: true });
        const keys = Object.keys(value);
        keys.forEach((key) => visit(value[key], path.concat(key)));
      } else records.push({ path, value, container: false });
    }
    visit(payload, []);
    const rows = records.map((record, index) => {
      const key = record.path.at(-1) || "project"; const context = contextFor(record.path, payload);
      const typed = record.container ? { value: JSON.stringify(record.value), type: "json" } : typedValue(record.value, key);
      return [SCHEMA_VERSION, payload.projectId, payload.name, context.section, context.entityType, context.entityId, context.parentId, context.companyId, context.companyName, context.wpNumber, context.employeeName, pathToPointer(record.path), LABELS[key] || key.replace(/([a-z])([A-Z])/g, "$1 $2"), typed.value, typed.type, UNITS[key] || "", `${index + 1}`, "input"];
    });
    return `\uFEFF${HEADERS.map(csvEscape).join(DELIMITER)}\r\n${rows.map((row) => row.map(csvEscape).join(DELIMITER)).join("\r\n")}\r\n`;
  }

  function decodeValue(row, line) {
    if (!VALUE_TYPES.has(row.value_type)) throw new Error(`Zeile ${line}: Unbekannter value_type „${row.value_type}“.`);
    if (row.value_type === "null") return null;
    if (row.value_type === "boolean") { if (!/^(true|false)$/.test(row.value)) throw new Error(`Zeile ${line}: Ungültiger Boolean.`); return row.value === "true"; }
    if (row.value_type === "number") { const number = Number(row.value); if (!Number.isFinite(number)) throw new Error(`Zeile ${line}: Ungültige Zahl.`); return number; }
    if (row.value_type === "json") { try { return JSON.parse(row.value); } catch (_) { throw new Error(`Zeile ${line}: Ungültiger JSON-Einzelwert.`); } }
    return row.value;
  }

  function deserializeProjectFromCsv(text) {
    const matrix = parseCsv(text);
    if (matrix.length < 2) throw new Error("Die CSV enthält keine Projektdaten.");
    const headers = matrix[0];
    HEADERS.forEach((header) => { if (!headers.includes(header)) throw new Error(`Pflichtspalte „${header}“ fehlt.`); });
    const output = {};
    matrix.slice(1).forEach((values, index) => {
      if (values.length !== headers.length) throw new Error(`Zeile ${index + 2}: Spaltenanzahl stimmt nicht mit dem Header überein.`);
      const row = Object.fromEntries(headers.map((header, column) => [header, values[column]]));
      if (row.schema_version !== SCHEMA_VERSION) throw new Error(`Zeile ${index + 2}: Schema-Version „${row.schema_version}“ wird nicht unterstützt.`);
      if (!row.project_id || !row.project_name) throw new Error(`Zeile ${index + 2}: project_id oder project_name fehlt.`);
      if (!ENTITY_TYPES.has(row.entity_type)) throw new Error(`Zeile ${index + 2}: Unbekannter entity_type „${row.entity_type}“.`);
      const path = parsePointer(row.field_key); if (!path.length || path.some((part) => part === "__proto__" || part === "constructor" || part === "prototype")) throw new Error(`Zeile ${index + 2}: Ungültiger Feldpfad.`);
      let target = output;
      path.forEach((part, pathIndex) => {
        const last = pathIndex === path.length - 1;
        if (last) { target[part] = decodeValue(row, index + 2); return; }
        const nextIsArray = /^\d+$/.test(path[pathIndex + 1]);
        if (!own(target, part)) target[part] = nextIsArray ? [] : {};
        target = target[part];
      });
    });
    if (!output.projectId || !output.name || !output.state || typeof output.state !== "object") throw new Error("Projekt-ID, Projektname oder vollständiger Projekt-State fehlt.");
    return migrateProjectCsv(output, SCHEMA_VERSION);
  }

  function migrateProjectCsv(payload, version) {
    if (`${version}` !== SCHEMA_VERSION) throw new Error(`Schema-Version „${version}“ wird nicht unterstützt.`);
    return payload;
  }

  return { SCHEMA_VERSION, HEADERS, parseProjectCsv: parseCsv, migrateProjectCsv, serializeProjectToCsv, deserializeProjectFromCsv };
});
