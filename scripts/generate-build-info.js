#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const outputPath = path.join(__dirname, '..', 'public', 'version.js');

const getLastUpdateDate = () => {
  try {
    const commitIso = execSync('git log -1 --format=%cI', {
      cwd: path.join(__dirname, '..'),
      stdio: ['ignore', 'pipe', 'ignore'],
    })
      .toString()
      .trim();

    if (commitIso) {
      const commitDate = new Date(commitIso);
      if (!Number.isNaN(commitDate.getTime())) {
        return commitDate;
      }
    }
  } catch (_) {
    // Fallback below.
  }

  return new Date();
};

const formatGermanDate = (date) =>
  new Intl.DateTimeFormat('de-DE', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'Europe/Berlin',
  }).format(date);

const lastSoftwareUpdate = formatGermanDate(getLastUpdateDate());

const fileContent = `window.APP_VERSION_META = Object.freeze({\n  LAST_SOFTWARE_UPDATE: "${lastSoftwareUpdate}",\n});\n`;

fs.mkdirSync(path.dirname(outputPath), { recursive: true });
fs.writeFileSync(outputPath, fileContent, 'utf8');
console.log(`Generated ${path.relative(path.join(__dirname, '..'), outputPath)} with ${lastSoftwareUpdate}`);
