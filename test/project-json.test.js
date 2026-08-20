const test = require("node:test");
const assert = require("node:assert/strict");
const { serializeProjectToJson, deserializeProjectFromJson, validateProjectJson } = require("../public/project-json.js");

const state = {
  schemaVersion: 1, startDate: "2026-07-01", durationMonths: 36, companyCount: 2,
  companyNames: ["Müller & Söhne", "KI GmbH"],
  workPackages: [{ id: "wp-3", title: "Forschung", pm: [0, ""], sub: [{ id: "wp-3-2", title: "Analyse – vollständig", pm: [4.25, null] }] }],
  employeesByCompany: [[{ name: "Max Mustermann", annualSalary: 85000, projectPercent: 50, weeklyHours: 40, actualHours: 20, partTimeFactor: 1, entryDate: null }], []],
  companyData: [{ foerderquotePercent: 45, zuschlagsfaktorPercent: 100, maximalbetrag: 560000, autoPlanningLocks: { employeeAssignment: false }, ergebnisse: { foerdersumme: 0 }, tabellenEintraege: { "wp-3-2": { start: "07.2026", ende: "10.2026", mitarbeiterNummer: "1" } } }, {}],
  companyHistoryByCompany: [[], []], extraFutureField: { zero: 0, disabled: false, nothing: null, empty: "", list: [] },
};
const project = { projectId: "cryo-1", name: "CryoVision Ä", createdAt: "2026-01-01T00:00:00.000Z", lastModified: "2026-08-20T00:00:00.000Z", state };

test("self describing project JSON is exactly lossless", () => {
  const exported = serializeProjectToJson(project, "2026-08-20T13:20:00.000Z");
  assert.equal(exported.exportInfo.format, "campusalliance-zim-project");
  assert.equal(exported.projectOverview.projectName, "CryoVision Ä");
  assert.equal(exported.projectData.employees[0].annualSalaryEUR, 85000);
  assert.equal(exported.projectData.assignments[0].assignedPersonMonths, 4.25);
  assert.equal(exported.projectData.assignments[0].employeeName, "Max Mustermann");
  assert.equal(exported.dataDictionary.personMonths.unit, "PM");
  assert.deepEqual(deserializeProjectFromJson(JSON.stringify(exported)).state, state);
});

test("validation produces actionable relationship and range errors", () => {
  const exported = serializeProjectToJson(project);
  exported.projectData.employees[0].projectAllocationPercent = 101;
  assert.throws(() => validateProjectJson(exported), /zwischen 0 und 100/);
  exported.projectData.employees[0].projectAllocationPercent = 50;
  exported.projectData.assignments[0].employeeId = "employee-99";
  assert.throws(() => validateProjectJson(exported), /employee-99.*nicht definiert/);
});
