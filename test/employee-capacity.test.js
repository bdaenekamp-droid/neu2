const test = require("node:test");
const assert = require("node:assert/strict");
const capacity = require("../public/employee-capacity.js");

const month = (year, oneBasedMonth) => year * 12 + oneBasedMonth - 1;

test("uses 10.5 PM as the full-year basis for every allocation percentage", () => {
  for (const [projectPercent, expected] of [[100, 10.5], [80, 8.4], [75, 7.875], [50, 5.25], [25, 2.625]]) {
    assert.equal(capacity.maxPmForYear({
      projectStart: month(2027, 1), projectEnd: month(2027, 12), projectPercent, year: 2027,
    }), expected);
  }
});

test("prorates the first and last project years independently", () => {
  const common = { projectStart: month(2026, 7), projectEnd: month(2028, 6), projectPercent: 50 };
  assert.equal(capacity.maxPmForYear({ ...common, year: 2026 }), 2.625);
  assert.equal(capacity.maxPmForYear({ ...common, year: 2027 }), 5.25);
  assert.equal(capacity.maxPmForYear({ ...common, year: 2028 }), 2.625);
});

test("a later employee project entry further reduces the affected year", () => {
  assert.equal(capacity.maxPmForYear({
    projectStart: month(2026, 7),
    projectEnd: month(2027, 12),
    employeeEntry: month(2027, 4),
    projectPercent: 50,
    year: 2027,
  }), 3.9375);
});

test("adds all work packages per employee and calendar year", () => {
  const assignments = [2, 1.5, 1.75].map((pm) => ({
    employeeIndex: 0, pm, start: month(2027, 1), end: month(2027, 12),
  }));
  assert.equal(capacity.aggregatePmByEmployeeAndYear(assignments)["0:2027"], 5.25);
  assert.deepEqual(capacity.findViolations({
    assignments,
    employees: [{ projectPercent: 50 }],
    projectStart: month(2027, 1),
    projectEnd: month(2027, 12),
  }), []);

  assignments.push({ employeeIndex: 0, pm: 0.25, start: month(2027, 1), end: month(2027, 1) });
  const [violation] = capacity.findViolations({
    assignments,
    employees: [{ projectPercent: 50 }],
    projectStart: month(2027, 1),
    projectEnd: month(2027, 12),
  });
  assert.equal(violation.year, 2027);
  assert.equal(violation.allowedPm, 5.25);
  assert.equal(violation.assignedPm, 5.5);
});

test("does not offset an overloaded year with free capacity in another year", () => {
  const violations = capacity.findViolations({
    assignments: [
      { employeeIndex: 0, pm: 6, start: month(2027, 1), end: month(2027, 12) },
      { employeeIndex: 0, pm: 1, start: month(2028, 1), end: month(2028, 12) },
    ],
    employees: [{ projectPercent: 50 }],
    projectStart: month(2027, 1),
    projectEnd: month(2028, 12),
  });
  assert.deepEqual(violations.map(({ year }) => year), [2027]);
});
