class SourceTable {
  constructor(sourcePool, targetPool) {
      this.sourcePool = sourcePool;
      this.targetPool = targetPool;
  }

  async getDataByReferenceNo(tableName, referenceNo) {
    const [rows] = await this.sourcePool.query(
      `SELECT * FROM ${tableName} WHERE reference_no = ?`,
      [referenceNo]
    );
    return rows[0];
  }

  async transferData(sourceTable, targetTable, data) {
    const idField = targetTable === 'voucher' ? 'voucher_id' : 'reference_no';
    const idValue = targetTable === 'voucher' ? data.voucher_id : data.reference_no;

    const [existing] = await this.targetPool.query(
      `SELECT * FROM ${targetTable} WHERE ${idField} = ?`,
      [idValue]
    );

    if (existing.length > 0) {
      const setClause = Object.keys(data)
        .filter(key => key !== idField)
        .map(key => `${key} = ?`)
        .join(', ');

      const values = Object.keys(data)
        .filter(key => key !== idField)
        .map(key => data[key]);

      values.push(idValue);

      await this.targetPool.query(
        `UPDATE ${targetTable} SET ${setClause} WHERE ${idField} = ?`,
        values
      );
    } else {
      const columns = Object.keys(data).join(', ');
      const placeholders = Object.keys(data).map(() => '?').join(', ');
      const values = Object.values(data);

      await this.targetPool.query(
        `INSERT INTO ${targetTable} (${columns}) VALUES (${placeholders})`,
        values
      );
    }
  }
}

module.exports = SourceTable;