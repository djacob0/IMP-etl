class EtlLogger {
  constructor(pool) {
    this.pool = pool;
  }

  async getRecordsBatch(offset = 0, limit = 10000) {
    const [rows] = await this.pool.query(
      `SELECT log_id, reference_no, \`table\`
        FROM etl_logger_voucher
        WHERE reference_no IS NOT NULL
          AND \`table\` IS NOT NULL
        ORDER BY log_id ASC
        LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    return rows;
  }

  async getTotalRecords() {
    const [result] = await this.pool.query(
      `SELECT COUNT(*) as total
        FROM etl_logger_voucher
        WHERE reference_no IS NOT NULL
          AND \`table\` IS NOT NULL`
    );
    return result[0].total;
  }
}

  module.exports = EtlLogger;