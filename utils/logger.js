const fs = require('fs');
const path = require('path');

const currentFile = __filename;
const projectRoot = path.resolve(__dirname, '../');
const LOG_DIRECTORY = path.join(projectRoot, 'logs');
const LOG_FILE = path.join(LOG_DIRECTORY, 'RSBSAetl.log');

try {
  if (!fs.existsSync(LOG_DIRECTORY)) {
    fs.mkdirSync(LOG_DIRECTORY, { recursive: true });
  }

  if (!fs.existsSync(LOG_FILE)) {
    fs.writeFileSync(LOG_FILE, '', { flag: 'w' });
  }
} catch (error) {
  console.error(`Failed to set up log directory or file: ${error.message}`);
  console.error(`Error stack: ${error.stack}`);
}

function getPHTTimestamp() {
  const now = new Date();
  const options = {
    timeZone: 'Asia/Manila',
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  };
  const parts = new Intl.DateTimeFormat('en-CA', options).formatToParts(now);
  const dateParts = {};
  parts.forEach(({ type, value }) => {
    dateParts[type] = value;
  });
  const { year, month, day, hour, minute, second } = dateParts;
  const ms = now.getMilliseconds().toString().padStart(3, '0');
  return `${year}-${month}-${day}T${hour}:${minute}:${second}.${ms}+08:00`;
}

const logger = {
  log: (message) => {
    const timestamp = getPHTTimestamp();
    const logMessage = `[${timestamp}] ${message}\n`;
    console.log(logMessage);
    try {
      fs.appendFileSync(LOG_FILE, logMessage, { flag: 'a' });
    } catch (error) {
      console.error(`Failed to write to log file: ${error.message}`);
      console.error(`Error stack: ${error.stack}`);
    }
  },
  error: (message) => {
    const timestamp = getPHTTimestamp();
    const errorMessage = `[${timestamp}] ERROR: ${message}\n`;
    console.error(errorMessage);
    try {
      fs.appendFileSync(LOG_FILE, errorMessage, { flag: 'a' });
    } catch (error) {
      console.error(`Failed to write error to log file: ${error.message}`);
      console.error(`Error stack: ${error.stack}`);
    }
  },
  debug: (message) => {
    const timestamp = getPHTTimestamp();
    const debugMessage = `[${timestamp}] DEBUG: ${message}\n`;
    try {
      fs.appendFileSync(LOG_FILE, debugMessage, { flag: 'a' });
    } catch (error) {
      console.error(`Failed to write debug to log file: ${error.message}`);
      console.error(`Error stack: ${error.stack}`);
    }
  },
  getLogFilePath: () => {
    return LOG_FILE;
  }
};

module.exports = logger;
