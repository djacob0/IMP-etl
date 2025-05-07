require('dotenv').config();

const mysql = require('mysql2/promise');

const sourcePool = mysql.createPool({
  host: process.env.SOURCE_DB_HOST,
  user: process.env.SOURCE_DB_USER,
  password: process.env.SOURCE_DB_PASSWORD,
  database: process.env.SOURCE_DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  // ssl: {
  //   rejectUnauthorized: true,
  //   ca: fs.readFileSync(process.env.DB_SSL_CA_PATH)
  // },
  connectTimeout: 10000,
  charset: 'utf8mb4'
});

const targetPool = mysql.createPool({
  host: process.env.TARGET_DB_HOST,
  user: process.env.TARGET_DB_USER,
  password: process.env.TARGET_DB_PASSWORD,
  database: process.env.TARGET_DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  // ssl: {
  //   rejectUnauthorized: true,
  //   ca: fs.readFileSync(process.env.DB_SSL_CA_PATH)
  // },
  connectTimeout: 10000,
  charset: 'utf8mb4'
});

module.exports = { sourcePool, targetPool };