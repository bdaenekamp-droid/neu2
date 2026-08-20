const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const source = fs.readFileSync(path.join(__dirname, "..", "public", "index.html"), "utf8");

test("PDF page dimensions include the complete timeline, header, rows, and PM reserve", () => {
  assert.match(source, /pageWidth = margin \* 2 \+ apWidth \+ renderedMonthCount \* monthWidth \+ pmLabelReserve/);
  assert.match(source, /pageHeight = margin \* 2 \+ titleHeight \+ timelineHeaderHeight \+ Math\.max\(1, renderedRowCount\) \* rowHeight/);
  assert.match(source, /pdfRows\[0\] === ganttExportRows\[0\].*pdfRows\.at\(-1\) === ganttExportRows\.at\(-1\)/s);
  assert.match(source, /pdfTimeline\[0\] === timeline\[0\].*pdfTimeline\.at\(-1\) === timeline\.at\(-1\)/s);
});

test("Word pagination derives the row count from usable A4 landscape height", () => {
  assert.match(source, /availableRowHeight = pageHeight - pageMargin \* 2 - titleAndLegendHeight - timelineHeaderHeight/);
  assert.match(source, /rowsPerPage = Math\.max\(1, Math\.floor\(availableRowHeight \/ ganttRowHeight\)\)/);
  assert.doesNotMatch(source, /const rowsPerPage = 18/);
  assert.match(source, /orientation: PageOrientation\.LANDSCAPE/);
});

test("PDF and Word row labels render only AP numbers", () => {
  assert.match(source, /pdf\.text\(row\.number, gridX/);
  assert.match(source, /children: \[cell\(row\.number,/);
  assert.doesNotMatch(source, /pdf\.text\(`\$\{row\.number\}\$\{isHeading/);
  assert.doesNotMatch(source, /children: \[cell\(`\$\{row\.number\}\$\{isHeading/);
});

