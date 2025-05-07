const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const fs = require('fs');
const path = require('path');
const { sourcePool, targetPool } = require('./config/db');
const EtlService = require('./services/etlService');
const logger = require('./utils/logger');
const { Tail } = require('tail')
const clients = new Set();
const app = express();
const port = process.env.PORT || 5006;

app.use(cors({
    origin: 'http://localhost:8085',
    methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
    credentials: true,
    allowedHeaders: ['Content-Type', 'Authorization'],
  }));

app.use(express.json());

const etlService = new EtlService(sourcePool, targetPool);
let etlTask = null;
let isEtlRunning = false;
let currentSchedule = null;
let etlStartTime = null;
let lastRunTime = null;

function isValidCronPattern(pattern) {
  try {
    return cron.validate(pattern);
  } catch (e) {
    logger.error(`Cron validation error: ${e.message}`);
    return false;
  }
}

function formatUptime(ms) {
  const seconds = Math.floor(ms / 1000) % 60;
  const minutes = Math.floor(ms / (1000 * 60)) % 60;
  const hours = Math.floor(ms / (1000 * 60 * 60));
  return `${hours}h ${minutes}m ${seconds}s`;
}

app.post('/api/start-etl', async (req, res) => {
  if (isEtlRunning) {
    return res.status(400).json({
      message: 'ETL scheduler is already running',
      currentSchedule: currentSchedule,
      startTime: etlStartTime ? etlStartTime.toISOString() : null
    });
  }

  try {
    if (!req.body) {
      return res.status(400).json({
        message: 'Request body is missing or invalid',
        example: {
          schedule: '* * * * *',
          description: 'Run every minute'
        }
      });
    }

    const cronSchedule = req.body.schedule || '* * * * *';

    if (!isValidCronPattern(cronSchedule)) {
      return res.status(400).json({
        message: 'Invalid cron schedule pattern',
        validExample: '* * * * * (every minute)'
      });
    }

    etlTask = cron.schedule(cronSchedule, async () => {
      try {
        lastRunTime = new Date();
        await etlService.runEtlProcess();
      } catch (error) {
        logger.error(`Scheduled ETL run failed: ${error.message}`);
      }
    }, {
      scheduled: true,
      timezone: 'Asia/Manila',
      runOnInit: false
    });

    isEtlRunning = true;
    currentSchedule = cronSchedule;
    etlStartTime = new Date();

    res.json({
      message: 'ETL scheduler started successfully',
      schedule: cronSchedule,
      timezone: 'Asia/Manila',
      startTime: etlStartTime.toISOString()
    });
  } catch (error) {
    if (etlTask) {
      etlTask.stop();
      etlTask = null;
    }
    isEtlRunning = false;
    currentSchedule = null;
    etlStartTime = null;

    res.status(500).json({
      message: 'Failed to start ETL scheduler',
      error: error.message
    });
  }
});

app.post('/api/stop-etl', async (req, res) => {
  if (!isEtlRunning) {
    logger.log('Stop ETL request received but scheduler was not running');
    return res.status(400).json({
      message: 'ETL scheduler is not running',
      timestamp: new Date().toISOString()
    });
  }

  try {
    if (etlTask) {
      etlTask.stop();
      etlTask = null;
    }

    const stopInfo = await etlService.stopEtlProcess();

    isEtlRunning = false;
    const stoppedSchedule = currentSchedule;
    currentSchedule = null;
    const stopTime = new Date(stopInfo.stopTime);

    const uptimeMs = etlStartTime ? stopTime - etlStartTime : 0;
    etlStartTime = null;
    // logger.log(`ETL scheduler stopped successfully. Total uptime: ${formatUptime(uptimeMs)}`);
    res.json({
      message: stopInfo.message,
      stoppedSchedule: stoppedSchedule,
      stoppedAt: stopTime.toISOString(),
      totalUptime: formatUptime(uptimeMs),
      lastRun: stopInfo.lastRun ? new Date(stopInfo.lastRun).toISOString() : null
    });
  } catch (error) {
    logger.error(`Failed to stop ETL scheduler: ${error.message}`);
    res.status(500).json({
      message: 'Failed to stop ETL scheduler',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

app.get('/api/logs', (req, res) => {
    try {
        const logFilePath = logger.getLogFilePath();
        const logDir = path.dirname(logFilePath);

        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir, { recursive: true });
        }

        if (!fs.existsSync(logFilePath)) {
            fs.writeFileSync(logFilePath, '');
            return res.json({
                message: 'Created new log file',
                logs: [],
                count: 0
            });
        }

        const data = fs.readFileSync(logFilePath, 'utf8');
        const lines = data.split('\n')
            .filter(line => line.trim())
            .map(line => {
                const timestampMatch = line.match(/^\[(.*?)\]/);
                return {
                    timestamp: timestampMatch ? timestampMatch[1] : new Date().toISOString(),
                    message: timestampMatch ? line.replace(timestampMatch[0], '').trim() : line.trim()
                };
            });

        const lastLines = lines.slice(-100).reverse();

        res.json({
            message: 'Logs retrieved successfully',
            logs: lastLines,
            count: lastLines.length,
            path: logFilePath
        });

    } catch (error) {
        console.error(`Error in /api/logs: ${error.message}`);
        res.status(500).json({
            message: 'Error processing logs request',
            error: error.message
        });
    }
});

app.delete('/api/logs', (req, res) => {
    try {
        const logFilePath = logger.getLogFilePath();
        fs.writeFileSync(logFilePath, '');

        clients.forEach(client => {
            client.write(`event: clear\ndata: {}\n\n`);
        });

        res.json({
            message: 'Logs cleared successfully',
            timestamp: new Date().toISOString()
        });
    } catch (err) {
        console.error(`Error clearing logs: ${err.message}`);
        res.status(500).json({
            message: 'Failed to clear logs',
            error: err.message,
            timestamp: new Date().toISOString()
        });
    }
});

app.get('/api/logs/stream', (req, res) => {
    const logFilePath = logger.getLogFilePath();

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    clients.add(res);

    res.write(`event: connected\ndata: ${JSON.stringify({
        message: 'Connected to log stream',
        timestamp: new Date().toISOString()
    })}\n\n`);

    const tail = new Tail(logFilePath);

    tail.on('line', (line) => {
        const timestampMatch = line.match(/^\[(.*?)\]/);
        const logEntry = {
            timestamp: timestampMatch ? timestampMatch[1] : new Date().toISOString(),
            message: timestampMatch ? line.replace(timestampMatch[0], '').trim() : line.trim()
        };

        res.write(`event: log\ndata: ${JSON.stringify(logEntry)}\n\n`);
    });

    tail.on('error', (error) => {
        console.error('Tail error:', error);
    });

    req.on('close', () => {
        clients.delete(res);
        tail.unwatch();
    });
});

function initializeLogWatcher() {
    const logFilePath = logger.getLogFilePath();

    const logDir = path.dirname(logFilePath);
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir, { recursive: true });
    }

    if (!fs.existsSync(logFilePath)) {
        fs.writeFileSync(logFilePath, '');
    }
}

initializeLogWatcher();

app.get('/api/etl-status', (req, res) => {
  const uptime = isEtlRunning && etlStartTime
    ? formatUptime(new Date() - etlStartTime)
    : '0h 0m 0s';

  const status = {
    isRunning: isEtlRunning,
    currentSchedule: currentSchedule,
    lastRun: lastRunTime ? lastRunTime.toISOString() : null,
    startTime: etlStartTime ? etlStartTime.toISOString() : null,
    uptime: uptime
  };
  res.json(status);
});

app.get('/health', (req, res) => {
  const uptime = isEtlRunning && etlStartTime
    ? formatUptime(new Date() - etlStartTime)
    : '0h 0m 0s';

  res.status(200).json({
    status: 'healthy',
    etlScheduler: isEtlRunning ? 'running' : 'stopped',
    startTime: etlStartTime ? etlStartTime.toISOString() : null,
    uptime: uptime
  });
});

app.use((err, req, res, next) => {
  logger.error(`Unhandled error: ${err.message}`);
  res.status(500).json({
    message: 'Internal server error',
    error: process.env.NODE_ENV === 'production' ? 'An unexpected error occurred' : err.message
  });
});

const server = app.listen(port, () => {
  logger.log(`IMP ETL Control API running on port ${port}`);
});

server.on('error', (error) => {
  logger.error(`Server failed to start: ${error.message}`);
  process.exit(1);
});

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

function gracefulShutdown() {
  logger.log('Shutting down gracefully...');
  if (etlTask) {
    etlTask.stop();
  }
  server.close(() => {
    logger.log('Server closed successfully');
    process.exit(0);
  });

  setTimeout(() => {
    logger.log('Forcing shutdown after timeout');
    process.exit(1);
  }, 10000);
}