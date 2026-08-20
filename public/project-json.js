(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  root.ProjectJson = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  const FORMAT = "campusalliance-zim-project";
  const FORMAT_VERSION = 1;
  const clone = (value) => JSON.parse(JSON.stringify(value));
  const asNumber = (value) => value === "" || value == null ? null : Number(value);

  const projectEnd = (start, months) => {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(start || "") || !Number.isFinite(Number(months))) return null;
    const date = new Date(`${start}T00:00:00Z`);
    date.setUTCMonth(date.getUTCMonth() + Number(months));
    date.setUTCDate(date.getUTCDate() - 1);
    return date.toISOString().slice(0, 10);
  };

  function readableData(state) {
    const companyNames = Array.isArray(state.companyNames) ? state.companyNames : [];
    const employees = [];
    (state.employeesByCompany || []).forEach((group, companyIndex) => (group || []).forEach((employee, employeeIndex) => {
      employees.push({
        employeeId: `company-${companyIndex + 1}-employee-${employeeIndex + 1}`,
        employeeName: employee.name || "",
        companyId: `company-${companyIndex + 1}`,
        companyName: companyNames[companyIndex] || `Unternehmen ${companyIndex + 1}`,
        annualSalaryEUR: employee.annualSalary,
        projectAllocationPercent: employee.projectPercent,
        weeklyHours: employee.weeklyHours,
        actualWeeklyHours: employee.actualHours,
        partTimeFactor: employee.partTimeFactor,
        ...clone(employee),
      });
    }));
    const workPackages = [];
    (state.workPackages || []).forEach((parent, parentIndex) => {
      workPackages.push({ workPackageId: parent.id, workPackageNumber: `${parentIndex + 1}`, description: parent.title, type: "main", personMonthsByCompany: clone(parent.pm || []) });
      (parent.sub || []).forEach((child, childIndex) => {
        const number = `${parentIndex + 1}.${childIndex + 1}`;
        const assignments = [];
        (child.pm || []).forEach((pm, companyIndex) => {
          const amount = asNumber(pm);
          if (amount == null) return;
          const planning = state.companyData?.[companyIndex]?.tabellenEintraege?.[child.id] || {};
          const employeeIndex = Math.max(0, Number(planning.mitarbeiterNummer || 1) - 1);
          const employee = state.employeesByCompany?.[companyIndex]?.[employeeIndex] || {};
          assignments.push({
            companyId: `company-${companyIndex + 1}`, companyName: companyNames[companyIndex] || `Unternehmen ${companyIndex + 1}`,
            employeeId: `company-${companyIndex + 1}-employee-${employeeIndex + 1}`, employeeName: employee.name || "",
            assignedPersonMonths: amount, startDate: planning.start || null, endDate: planning.ende || null,
          });
        });
        workPackages.push({ workPackageId: child.id, parentWorkPackageId: parent.id, workPackageNumber: number, description: child.title, type: "sub", personMonthsByCompany: clone(child.pm || []), assignments });
      });
    });
    return {
      project: { projectStart: state.startDate, durationMonths: state.durationMonths, projectEnd: projectEnd(state.startDate, state.durationMonths) },
      companies: companyNames.slice(0, state.companyCount).map((name, index) => ({ companyId: `company-${index + 1}`, companyName: name, order: index + 1 })),
      workPackages,
      employees,
      assignments: workPackages.flatMap((wp) => (wp.assignments || []).map((assignment) => ({ workPackageId: wp.workPackageId, workPackageNumber: wp.workPackageNumber, workPackageDescription: wp.description, ...assignment }))),
      funding: { inputsByCompany: (state.companyData || []).slice(0, state.companyCount).map((data, index) => ({ companyId: `company-${index + 1}`, companyName: companyNames[index], fundingRatePercent: data.foerderquotePercent, surchargeFactorPercent: data.zuschlagsfaktorPercent, maximumGrantEUR: data.maximalbetrag })), calculatedByCompany: (state.companyData || []).slice(0, state.companyCount).map((data, index) => ({ companyId: `company-${index + 1}`, companyName: companyNames[index], ...(clone(data.ergebnisse || {})) })) },
      settings: { autoPlanningLocksByCompany: (state.companyData || []).map((data) => clone(data.autoPlanningLocks || {})) },
      _internal: { description: "Vollständiger, verlustfreier Anwendungszustand. Dieser strukturierte Bereich ist beim Re-Import maßgeblich.", completeProjectState: clone(state) },
    };
  }

  function serializeProjectToJson(project, exportedAt = new Date().toISOString()) {
    if (!project || !project.state || typeof project.state !== "object") throw new Error("Kein vollständiges Projekt zum Exportieren vorhanden.");
    const data = readableData(project.state);
    return {
      exportInfo: { format: FORMAT, formatVersion: FORMAT_VERSION, description: "Vollständiger Export eines Projekts aus der CampusAlliance ZIM-Projektplanung.", exportedAt, language: "de-DE", purpose: "Vollständige Sicherung, externer Datenaustausch und erneuter Import eines einzelnen ZIM-Projekts." },
      projectOverview: { projectId: project.projectId, projectName: project.name, projectStart: project.state.startDate, projectEnd: data.project.projectEnd, durationMonths: project.state.durationMonths, numberOfCompanies: data.companies.length, numberOfWorkPackages: data.workPackages.length, numberOfEmployees: data.employees.length },
      dataDictionary: {
        personMonths: { label: "Personenmonate", description: "Geplanter Arbeitsumfang für ein Arbeitspaket.", type: "number", unit: "PM" },
        annualSalaryEUR: { label: "Jahresgehalt", description: "Brutto-Jahresgehalt des Mitarbeiters.", type: "number", unit: "EUR" },
        projectAllocationPercent: { label: "Projekteinsatz", description: "Maximal zulässiger Projekteinsatz des Mitarbeiters.", type: "number", unit: "percent" },
        weeklyHours: { label: "Wochenstunden", description: "Vertragliche Arbeitszeit je Woche.", type: "number", unit: "hours/week" },
        fundingRatePercent: { label: "Förderquote", description: "Förderfähiger Anteil der Kosten.", type: "number", unit: "percent", valueKind: "input" },
        maximumGrantEUR: { label: "Maximale Förderung", description: "Förderobergrenze des Unternehmens.", type: "number", unit: "EUR", valueKind: "input" },
      },
      projectData: data,
      projectRecord: { createdAt: project.createdAt || null, lastModified: project.lastModified || null, schemaVersion: project.schemaVersion || project.state.schemaVersion || 1 },
    };
  }

  function validateProjectJson(value) {
    if (!value || typeof value !== "object") throw new Error("Die Datei enthält kein JSON-Objekt.");
    if (value.exportInfo?.format !== FORMAT) throw new Error(`exportInfo.format muss „${FORMAT}“ sein.`);
    if (value.exportInfo?.formatVersion !== FORMAT_VERSION) throw new Error(`formatVersion ${value.exportInfo?.formatVersion} wird nicht unterstützt.`);
    if (!value.projectOverview?.projectId || !value.projectOverview?.projectName) throw new Error("projectOverview benötigt projectId und projectName.");
    const state = value.projectData?._internal?.completeProjectState;
    if (!state || typeof state !== "object" || Array.isArray(state)) throw new Error("projectData._internal.completeProjectState fehlt; ein verlustfreier Import ist nicht möglich.");
    (value.projectData?.employees || []).forEach((employee) => {
      if (typeof employee.projectAllocationPercent !== "number" || employee.projectAllocationPercent < 0 || employee.projectAllocationPercent > 100) throw new Error(`projectAllocationPercent von „${employee.employeeName || employee.employeeId}“ muss eine Zahl zwischen 0 und 100 sein.`);
    });
    const ids = new Set((value.projectData?.employees || []).map((employee) => employee.employeeId));
    (value.projectData?.assignments || []).forEach((assignment) => {
      if (assignment.employeeId && !ids.has(assignment.employeeId)) throw new Error(`Das Arbeitspaket ${assignment.workPackageNumber || assignment.workPackageId} verweist auf die Mitarbeiter-ID „${assignment.employeeId}“. Dieser Mitarbeiter ist in der Datei nicht definiert.`);
    });
    return true;
  }

  function migrateProjectJson(value) { validateProjectJson(value); return value; }
  function deserializeProjectFromJson(value) {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    const migrated = migrateProjectJson(parsed);
    return { projectId: migrated.projectOverview.projectId, name: migrated.projectOverview.projectName, createdAt: migrated.projectRecord?.createdAt, lastModified: migrated.projectRecord?.lastModified, schemaVersion: migrated.projectRecord?.schemaVersion || 1, state: clone(migrated.projectData._internal.completeProjectState) };
  }
  return { FORMAT, FORMAT_VERSION, serializeProjectToJson, deserializeProjectFromJson, validateProjectJson, migrateProjectJson };
});
