(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.EmployeeCapacity = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  const FULL_YEAR_PM = 10.5;
  const MONTHS_PER_YEAR = 12;
  const EPSILON = 1e-9;

  const availableMonthsInYear = (projectStart, projectEnd, employeeEntry, year) => {
    if (![projectStart, projectEnd, year].every(Number.isFinite) || projectEnd < projectStart) return 0;
    const yearStart = year * MONTHS_PER_YEAR;
    const yearEnd = yearStart + MONTHS_PER_YEAR - 1;
    const effectiveEntry = Number.isFinite(employeeEntry) ? employeeEntry : projectStart;
    const availableFrom = Math.max(projectStart, yearStart, effectiveEntry);
    const availableUntil = Math.min(projectEnd, yearEnd);
    return availableUntil < availableFrom ? 0 : availableUntil - availableFrom + 1;
  };

  const maxPmForYear = ({ projectStart, projectEnd, employeeEntry, projectPercent, year }) => {
    const percent = Math.max(0, Number.isFinite(projectPercent) ? projectPercent : 100);
    const months = availableMonthsInYear(projectStart, projectEnd, employeeEntry, year);
    return FULL_YEAR_PM * (percent / 100) * (months / MONTHS_PER_YEAR);
  };

  const splitPmByYear = (pm, start, end) => {
    if (![pm, start, end].every(Number.isFinite) || pm <= 0 || end < start) return {};
    const totalMonths = end - start + 1;
    const result = {};
    for (let month = start; month <= end; month += 1) {
      const year = Math.floor(month / MONTHS_PER_YEAR);
      result[year] = (result[year] || 0) + pm / totalMonths;
    }
    return result;
  };

  const aggregatePmByEmployeeAndYear = (assignments) =>
    (assignments || []).reduce((totals, assignment) => {
      if (!assignment || !Number.isInteger(assignment.employeeIndex)) return totals;
      const byYear = splitPmByYear(assignment.pm, assignment.start, assignment.end);
      Object.entries(byYear).forEach(([year, pm]) => {
        const key = `${assignment.employeeIndex}:${year}`;
        totals[key] = (totals[key] || 0) + pm;
      });
      return totals;
    }, {});

  const findViolations = ({ assignments, employees, projectStart, projectEnd }) => {
    const totals = aggregatePmByEmployeeAndYear(assignments);
    return Object.entries(totals).flatMap(([key, assignedPm]) => {
      const [employeeIndexText, yearText] = key.split(":");
      const employeeIndex = Number.parseInt(employeeIndexText, 10);
      const year = Number.parseInt(yearText, 10);
      const employee = employees?.[employeeIndex];
      if (!employee) return [];
      const allowedPm = maxPmForYear({
        projectStart,
        projectEnd,
        employeeEntry: employee.entryMonth,
        projectPercent: employee.projectPercent,
        year,
      });
      return assignedPm > allowedPm + EPSILON
        ? [{ employeeIndex, year, assignedPm, allowedPm }]
        : [];
    });
  };

  return {
    FULL_YEAR_PM,
    EPSILON,
    availableMonthsInYear,
    maxPmForYear,
    splitPmByYear,
    aggregatePmByEmployeeAndYear,
    findViolations,
  };
});
