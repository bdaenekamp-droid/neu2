const test = require("node:test");
const assert = require("node:assert/strict");
const {
  HEADERS,
  serializeProjectToCsv,
  deserializeProjectFromCsv,
} = require("../public/project-csv.js");

const project = {
  projectId: "proj_ä-01",
  name: "CryoVision; KI, Test",
  createdAt: "2026-07-01T08:30:00.000Z",
  lastModified: "2026-08-20T12:00:00.000Z",
  state: {
    schemaVersion: 1,
    startDate: "01.07.2026",
    endDate: null,
    durationMonths: 36,
    enabled: false,
    note: "",
    companyCount: 2,
    companyNames: ["Solidtec", "Müller & Söhne"],
    workPackages: [
      { id: "wp-1", title: "Analyse; Planung, Prüfung", pm: [0, ""], sub: [
        { id: "wp-1-1", title: "Mehrzeilig\nmit \"Anführungszeichen\" und Umlauten: äöü", pm: [4.25, 1] },
      ] },
    ],
    employeesByCompany: [
      [{ id: "emp-1", name: "Max Mustermann", annualSalary: 85000, projectPercent: 50, weeklyHours: 40, projectEntryDate: "01.08.2026", optional: null }],
      [{ id: "emp-2", name: "Zoë Müller", annualSalary: 0, projectPercent: 0, weeklyHours: 20, projectEntryDate: "" }],
    ],
    companyData: [
      { color: "#123456", fundingRate: 50, surchargeFactor: 1.2, maximumFunding: 450000, fundingByYear: { 2026: 1000 }, tabellenEintraege: { "wp-1-1": { start: "01.07.2026", ende: "31.12.2026", mitarbeiterNummer: 1, pm: 4.25 } }, zimMantelboegen: { unternehmen1: { legalName: "Solidtec GmbH" } } },
      { color: "#abcdef", tabellenEintraege: {}, zimMantelboegen: {} },
    ],
    companyHistoryByCompany: [{ history: [], historyIndex: 0 }, { history: [{ tabellenEintraege: {}, pmSnapshot: null }], historyIndex: 0 }],
    futureComplexField: { nested: [{ zero: 0, no: false, empty: "", nothing: null }], emptyObject: {}, emptyArray: [] },
  },
};

test("project CSV roundtrip is lossless for the complete and future project state", () => {
  const csv = serializeProjectToCsv(project);
  assert.equal(csv.charCodeAt(0), 0xfeff);
  assert.match(csv, /schema_version;project_id;project_name/);
  assert.match(csv, /WorkPackages;work_package;wp-1/);
  assert.match(csv, /Employees;employee;emp-1/);
  assert.match(csv, /Solidtec/);
  assert.match(csv, /Jahresgehalt/);
  assert.match(csv, /"Mehrzeilig\nmit ""Anführungszeichen"" und Umlauten: äöü"/);
  assert.deepEqual(deserializeProjectFromCsv(csv), project);
});

test("project CSV validates required headers and schema versions with line details", () => {
  const csv = serializeProjectToCsv(project);
  assert.throws(() => deserializeProjectFromCsv(csv.replace("field_label;", "wrong_label;")), /Pflichtspalte „field_label“ fehlt/);
  assert.throws(() => deserializeProjectFromCsv(csv.replace(/\n1;/, "\n99;")), /Zeile 2: Schema-Version/);
  assert.deepEqual(HEADERS.includes("value_origin"), true);
});
