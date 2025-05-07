const EtlLogger = require('../models/EtlLogger');
const logger = require('../utils/logger');
const { sourcePool, targetPool } = require('../config/db');

class EtlService {
    constructor(sourcePool, targetPool) {
        this.sourcePool = sourcePool;
        this.targetPool = targetPool;
        this.etlLogger = new EtlLogger(sourcePool);
        this.lastRun = null;
        this.batchSize = 10000;
    }

    async getSourceData(referenceNo) {
        const [rows] = await this.sourcePool.query(
            `SELECT * FROM voucher WHERE reference_no = ?`,
            [referenceNo]
        );
        return rows;
    }

    async validateVoucherUniqueness(data, connection = this.targetPool) {
        const [mismatches] = await connection.query(
            `SELECT reference_no FROM voucher
             WHERE voucher_id = ? AND reference_no != ?
             LIMIT 1`,
            [data.voucher_id, data.reference_no]
        );

        if (mismatches.length > 0) {
            throw new Error(
                `Voucher ID ${data.voucher_id} already exists with different reference_no ` +
                `(${mismatches[0].reference_no} vs ${data.reference_no})`
            );
        }
    }

    async updateExistingVoucher(data, connection = this.targetPool) {
        const columns = Object.keys(data).filter(col => col !== 'voucher_id');
        const setClause = columns.map(col => `${col} = ?`).join(', ');
        const values = columns.map(col => data[col]);
        values.push(data.voucher_id);

        await connection.query(
            `UPDATE voucher SET ${setClause} WHERE voucher_id = ?`,
            values
        );
    }

    async insertNewVoucher(data, connection = this.targetPool) {
        const columns = Object.keys(data);
        const placeholders = columns.map(() => '?').join(', ');
        const values = columns.map(col => data[col]);

        await connection.query(
            `INSERT INTO voucher (${columns.join(', ')}) VALUES (${placeholders})`,
            values
        );
    }

    async handleTransfer(data, connection) {
        const [existing] = await connection.query(
            `SELECT 1 FROM voucher WHERE voucher_id = ? LIMIT 1`,
            [data.voucher_id]
        );

        if (existing.length > 0) {
            await this.updateExistingVoucher(data, connection);
        } else {
            await this.insertNewVoucher(data, connection);
        }
    }

    async transferData(data) {
        await this.ensureTableExists();

        if (data.length > 0) {
            const connection = await this.targetPool.getConnection();
            try {
                await connection.beginTransaction();

                for (const record of data) {
                    const processed = this.processData(record);
                    await this.validateVoucherUniqueness(processed, connection);
                    await this.handleTransfer(processed, connection);
                }
                await connection.commit();
            } catch (error) {
                await connection.rollback();
                logger.error('Voucher transfer failed', {
                    reference_no: data[0]?.reference_no,
                    error: error.message,
                    sampleData: data.slice(0, 2),
                    stack: error.stack
                });
                throw error;
            } finally {
                connection.release();
            }
        }
    }

    processData(data) {
        const processed = {...data};

        // Handle sex field validation
        if (processed.sex) {
            processed.sex = String(processed.sex).toUpperCase().trim();
            if (!['MALE', 'FEMALE'].includes(processed.sex)) {
                processed.sex = null;
            }
        } else {
            processed.sex = null;
        }

        // Uppercase transformation for other fields
        const fieldsToUpper = [
            'first_name', 'middle_name', 'last_name', 'ext_name',
            'mother_maiden', 'birth_place', 'reg_desc', 'prv_desc',
            'mun_desc', 'brgy_desc', 'seed_class', 'rrp_fertilizer_kind',
            'voucher_status', 'encode_agency', 'encoded_by_fullname',
            'cancelled_by_fullname', 'voucher_remarks', 'batch_code',
            'if_4ps', 'if_ip', 'if_pwd', 'voucher_season', 'reg_desc_farm',
            'prv_desc_farm', 'mun_desc_farm', 'brgy_desc_farm', 'cropname',
            'agri_input', 'variety', 'unit', 'cluster_org_assc'
        ];

        fieldsToUpper.forEach(field => {
            if (processed[field] && typeof processed[field] === 'string') {
                processed[field] = processed[field].toUpperCase();
            }
        });

        return processed;
    }

    async ensureTableExists() {
        const sql = `
        CREATE TABLE IF NOT EXISTS voucher (
          voucher_id VARCHAR(36) PRIMARY KEY,
          rsbsa_no VARCHAR(50),
          control_no VARCHAR(50),
          reference_no VARCHAR(15),
          program_id VARCHAR(36),
          fund_id VARCHAR(36),
          fund_desc VARCHAR(120),
          type VARCHAR(36),
          first_name VARCHAR(150),
          middle_name VARCHAR(150),
          last_name VARCHAR(150),
          ext_name VARCHAR(10),
          sex ENUM('MALE', 'FEMALE'),
          birthday VARCHAR(12),
          birth_place VARCHAR(255),
          mother_maiden VARCHAR(255),
          contact_no VARCHAR(20),
          civil_status TINYINT(1),
          geo_code VARCHAR(10),
          reg TINYINT(2) UNSIGNED ZEROFILL,
          reg_desc VARCHAR(150),
          prv TINYINT(2) UNSIGNED ZEROFILL,
          prv_desc VARCHAR(150),
          mun TINYINT(2) UNSIGNED ZEROFILL,
          mun_desc VARCHAR(150),
          brgy SMALLINT(3) UNSIGNED ZEROFILL,
          brgy_desc VARCHAR(150),
          farm_area DECIMAL(10,4),
          seed_class VARCHAR(120),
          sub_project TINYINT(1),
          rrp_fertilizer_kind VARCHAR(7),
          amount DECIMAL(11,2),
          amount_val DECIMAL(11,2),
          voucher_status VARCHAR(36),
          encode_agency VARCHAR(50),
          encoded_by_id VARCHAR(36),
          cancelled_by_id VARCHAR(36),
          encoded_by_fullname VARCHAR(255),
          cancelled_by_fullname VARCHAR(255),
          is_scanned ENUM('1', '0'),
          scanned_date DATETIME,
          date_cancelled DATETIME,
          last_scanned_by_id VARCHAR(36),
          date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          voucher_remarks VARCHAR(450),
          batch_code VARCHAR(45),
          if_4ps VARCHAR(5),
          if_ip VARCHAR(5),
          if_pwd VARCHAR(5),
          voucher_season VARCHAR(25),
          reg_farm TINYINT(2) UNSIGNED ZEROFILL,
          reg_desc_farm VARCHAR(150),
          prv_farm TINYINT(2) UNSIGNED ZEROFILL,
          prv_desc_farm VARCHAR(150),
          mun_farm TINYINT(2) UNSIGNED ZEROFILL,
          mun_desc_farm VARCHAR(150),
          brgy_farm SMALLINT(3) UNSIGNED ZEROFILL,
          brgy_desc_farm VARCHAR(150),
          cropname VARCHAR(120),
          agri_input VARCHAR(120),
          variety VARCHAR(120),
          unit VARCHAR(45),
          cluster_org_assc VARCHAR(120),
          year_funded VARCHAR(45),
          restored_by_id VARCHAR(36),
          restored_by_fullname VARCHAR(255),
          date_restored DATETIME,
          month_planting VARCHAR(3),
          crop_area DECIMAL(10,4),
          INDEX idx_reference_no (reference_no),
          INDEX idx_voucher_id (voucher_id)
        )`;
        await this.targetPool.query(sql);
    }

    async processBatch(batch) {
      let processedCount = 0;
      let skippedCount = 0;
      const batchErrors = [];
      const batchWarnings = [];

      for (const record of batch) {
          try {
              if (!record.reference_no) {
                  const warning = 'Skipped due to missing reference number';
                  batchWarnings.push({ log_id: record.log_id, message: warning });
                  skippedCount++;
                  continue;
              }

              const sourceData = await this.getSourceData(record.reference_no);
              if (!sourceData || sourceData.length === 0) {
                  const warning = `No source data for reference ${record.reference_no}`;
                  batchWarnings.push({ log_id: record.log_id, message: warning });
                  skippedCount++;
                  continue;
              }

              await this.transferData(sourceData);
              processedCount += sourceData.length;

          } catch (error) {
              const errorDetails = {
                  log_id: record.log_id,
                  reference_no: record.reference_no,
                  error: error.message,
                  stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
                  timestamp: new Date().toISOString()
              };
              batchErrors.push(errorDetails);
              skippedCount++;
              logger.error(`Failed to process record ${record.log_id}`, {
                  reference_no: record.reference_no,
                  error: error.message,
                  stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
                  details: error
              });
          }
      }

      if (batchErrors.length > 0) {
          logger.error(`Batch completed with ${batchErrors.length} errors`, {
              sampleErrors: batchErrors.slice(0, 5),
              errorRate: `${((batchErrors.length / batch.length) * 100).toFixed(2)}%`
          });
      }

      if (batchWarnings.length > 0) {
          logger.log(`Batch had ${batchWarnings.length} warnings`, {
              sampleWarnings: batchWarnings.slice(0, 5)
          });
      }

      return {
          processedCount,
          skippedCount,
          errors: batchErrors,
          warnings: batchWarnings
      };
  }

  async runEtlProcess() {
      try {
          this.lastRun = new Date();
          const batchSize = 10000;
          let offset = 0;
          let totalProcessed = 0;
          let totalSkipped = 0;
          let lastLoggedProgress = -1;

          const totalRecords = await this.etlLogger.getTotalRecords();
          logger.log(`Starting IMP ETL. Total records: ${totalRecords}`);

          if (totalRecords === 0) {
              logger.log('No records to process for IMP ETL');
              return {
                  processed: 0,
                  skipped: 0,
                  startTime: this.lastRun,
                  endTime: new Date()
              };
          }

          while (offset < totalRecords) {
              const currentOffset = offset;
              logger.log(`Processing batch: ${currentOffset} to ${Math.min(currentOffset + batchSize - 1, totalRecords - 1)}`);

              const batch = await this.etlLogger.getRecordsBatch(currentOffset, batchSize);
              if (batch.length === 0) break;

              const result = await this.processBatch(batch);
              totalProcessed += result.processedCount;
              totalSkipped += result.skippedCount;
              offset += batchSize;

              const currentProgress = Math.min(Math.round((offset / totalRecords) * 100), 100);
              if (currentProgress > lastLoggedProgress) {
                  logger.log(`Progress: ${currentProgress}% (${Math.min(offset, totalRecords)}/${totalRecords})`);
                  lastLoggedProgress = currentProgress;
              }

              await new Promise(resolve => setTimeout(resolve, 1000));
          }

          logger.log(`IMP ETL completed. Total Processed: ${totalProcessed}, Total Skipped: ${totalSkipped}`);
          return {
              processed: totalProcessed,
              skipped: totalSkipped,
              startTime: this.lastRun,
              endTime: new Date()
          };
      } catch (error) {
          logger.error('IMP ETL process failed', {
              error: error.message,
              stack: error.stack
          });
          throw error;
      }
  }

    getPHTTimestamp() {
        const now = new Date();
        return now.toLocaleString('en-CA', {
            timeZone: 'Asia/Manila',
            hour12: false
        });
    }

    async stopEtlProcess() {
        const stopTime = this.getPHTTimestamp();
        logger.log(`IMP ETL stopped at ${stopTime}`);
        return {
            message: 'ETL stopped successfully',
            lastRun: this.lastRun,
            stopTime: stopTime
        };
    }
}

module.exports = EtlService;