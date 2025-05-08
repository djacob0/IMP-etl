const EtlLogger = require('../models/EtlLogger');
const logger = require('../utils/logger');
const { sourcePool, targetPool } = require('../config/db');

class EtlService {
    constructor(sourcePool, targetPool) {
        this.sourcePool = sourcePool;
        this.targetPool = targetPool;
        this.etlLogger = new EtlLogger(sourcePool);
        this.lastRun = null;
        this.batchSize = 100000;
        this.concurrencyLimit = 4;
        this.retryLimit = 3;
        this.retryDelay = 1000;
    }

    async getVoucherData(referenceNo) {
        const [rows] = await this.sourcePool.query(
            `SELECT * FROM voucher WHERE reference_no = ?`,
            [referenceNo]
        );
        return rows;
    }

    async validateVoucherUniqueness(dataBatch, connection) {
        const values = dataBatch.flatMap(data => [data.voucher_id, data.reference_no]);
        const placeholders = dataBatch.map(() => '(?, ?)').join(',');
        const [existing] = await connection.query(
            `SELECT voucher_id, reference_no FROM voucher
                WHERE (voucher_id, reference_no) IN (${placeholders})`,
            values
        );
        return new Map(existing.map(e => [`${e.voucher_id}:${e.reference_no}`, e]));
    }

    async bulkUpdateVouchers(dataBatch, connection) {
        if (dataBatch.length === 0) return;
        const columns = Object.keys(dataBatch[0]).filter(col => col !== 'voucher_id' && col !== 'reference_no');
        const setClause = columns.map(col => `${col} = VALUES(${col})`).join(', ');
        const placeholders = dataBatch.map(() => `(${columns.map(() => '?').join(', ')}, ?, ?)`).join(', ');
        const values = dataBatch.flatMap(data => [
            ...columns.map(col => data[col] === undefined ? null : data[col]),
            data.voucher_id,
            data.reference_no
        ]);

        await connection.query(
            `INSERT INTO voucher (${columns.join(', ')}, voucher_id, reference_no)
                VALUES ${placeholders}
                ON DUPLICATE KEY UPDATE ${setClause}`,
            values
        );
    }

    async transferVoucherData(data, referenceNo) {
        if (data.length === 0) return { processed: 0, skipped: 0, warnings: [] };
        const connection = await this.targetPool.getConnection();
        const warnings = [];
        let processedCount = 0;
        let skippedCount = 0;

        try {
            await connection.beginTransaction();
            const processed = data.map(record => this.processVoucherData(record));
            const validRecords = processed.filter(record => {
                if (!record.voucher_id || !record.reference_no) {
                    warnings.push({
                        message: `Skipped record due to missing voucher_id or reference_no`,
                        reference_no: referenceNo
                    });
                    skippedCount++;
                    return false;
                }
                return true;
            });

            if (validRecords.length > 0) {
                let retries = 0;
                while (retries < this.retryLimit) {
                    try {
                        await this.bulkUpdateVouchers(validRecords, connection);
                        processedCount += validRecords.length;
                        break;
                    } catch (error) {
                        retries++;
                        if (retries === this.retryLimit) {
                            warnings.push({
                                message: `Failed to process voucher records for reference_no ${referenceNo} after ${this.retryLimit} retries: ${error.message}`,
                                reference_no: referenceNo
                            });
                            skippedCount += validRecords.length;
                            break;
                        }
                        logger.warn(`Retry ${retries}/${this.retryLimit} for voucher reference_no ${referenceNo}`, {
                            error: error.message
                        });
                        await new Promise(resolve => setTimeout(resolve, this.retryDelay));
                    }
                }
            }

            await connection.commit();
            return { processed: processedCount, skipped: skippedCount, warnings };
        } catch (error) {
            await connection.rollback();
            logger.error('Voucher transfer failed', {
                reference_no: referenceNo,
                error: error.message,
                sampleData: data.slice(0, 2),
                stack: error.stack
            });
            throw error;
        } finally {
            connection.release();
        }
    }

    processVoucherData(data) {
        const processed = { ...data };
        if (processed.sex) {
            processed.sex = String(processed.sex).toUpperCase().trim();
            if (!['MALE', 'FEMALE'].includes(processed.sex)) {
                processed.sex = null;
            }
        } else {
            processed.sex = null;
        }
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
                processed[field] = processed[field].toUpperCase().trim();
            }
        });
        ['farm_area', 'amount', 'amount_val', 'crop_area'].forEach(field => {
            if (processed[field] !== null && processed[field] !== undefined) {
                processed[field] = parseFloat(processed[field]) || null;
            }
        });
        ['birthday', 'scanned_date', 'date_cancelled', 'date_restored'].forEach(field => {
            if (processed[field] && !isNaN(Date.parse(processed[field]))) {
                processed[field] = new Date(processed[field]).toISOString().split('T')[0];
            } else {
                processed[field] = null;
            }
        });
        return processed;
    }

    async getTransactionData(referenceNo) {
        const [rows] = await this.sourcePool.query(
            `SELECT * FROM voucher_transaction WHERE reference_no = ?`,
            [referenceNo]
        );
        return rows;
    }

    async validateTransactionUniqueness(dataBatch, connection) {
        const values = dataBatch.flatMap(data => [data.voucher_details_id, data.reference_no]);
        const placeholders = dataBatch.map(() => '(?, ?)').join(',');
        const [existing] = await connection.query(
            `SELECT voucher_details_id, reference_no FROM voucher_transaction
                WHERE (voucher_details_id, reference_no) IN (${placeholders})`,
            values
        );
        return new Map(existing.map(e => [`${e.voucher_details_id}:${e.reference_no}`, e]));
    }

    async bulkUpdateTransactions(dataBatch, connection) {
        if (dataBatch.length === 0) return;
        const columns = Object.keys(dataBatch[0]).filter(col => col !== 'voucher_details_id' && col !== 'reference_no');
        const setClause = columns.map(col => `${col} = VALUES(${col})`).join(', ');
        const placeholders = dataBatch.map(() => `(${columns.map(() => '?').join(', ')}, ?, ?)`).join(', ');
        const values = dataBatch.flatMap(data => [
            ...columns.map(col => data[col] === undefined ? null : data[col]),
            data.voucher_details_id,
            data.reference_no
        ]);

        await connection.query(
            `INSERT INTO voucher_transaction (${columns.join(', ')}, voucher_details_id, reference_no)
                VALUES ${placeholders}
                ON DUPLICATE KEY UPDATE ${setClause}`,
            values
        );
    }

    async transferTransactionData(data, referenceNo) {
        if (data.length === 0) return { processed: 0, skipped: 0, warnings: [] };
        const connection = await this.targetPool.getConnection();
        const warnings = [];
        let processedCount = 0;
        let skippedCount = 0;

        try {
            await connection.beginTransaction();
            const processed = data.map(record => this.processTransactionData(record));
            const validRecords = processed.filter(record => {
                if (!record.voucher_details_id || !record.reference_no) {
                    warnings.push({
                        message: `Skipped transaction record due to missing voucher_details_id or reference_no`,
                        reference_no: referenceNo
                    });
                    skippedCount++;
                    return false;
                }
                return true;
            });

            if (validRecords.length > 0) {
                let retries = 0;
                while (retries < this.retryLimit) {
                    try {
                        await this.bulkUpdateTransactions(validRecords, connection);
                        processedCount += validRecords.length;
                        break;
                    } catch (error) {
                        retries++;
                        if (retries === this.retryLimit) {
                            warnings.push({
                                message: `Failed to process transaction records for reference_no ${referenceNo} after ${this.retryLimit} retries: ${error.message}`,
                                reference_no: referenceNo
                            });
                            skippedCount += validRecords.length;
                            break;
                        }
                        logger.warn(`Retry ${retries}/${this.retryLimit} for transaction reference_no ${referenceNo}`, {
                            error: error.message
                        });
                        await new Promise(resolve => setTimeout(resolve, this.retryDelay));
                    }
                }
            }

            await connection.commit();
            return { processed: processedCount, skipped: skippedCount, warnings };
        } catch (error) {
            await connection.rollback();
            logger.error('Voucher transaction transfer failed', {
                reference_no: referenceNo,
                error: error.message,
                sampleData: data.slice(0, 2),
                stack: error.stack
            });
            throw error;
        } finally {
            connection.release();
        }
    }

    processTransactionData(data) {
        const processed = { ...data };
        ['quantity', 'amount', 'total_amount', 'cash_added', 'latitude', 'longitude'].forEach(field => {
            if (processed[field] !== null && processed[field] !== undefined) {
                processed[field] = parseFloat(processed[field]) || null;
            }
        });

        if (processed.payout !== undefined) {
            processed.payout = processed.payout === '1' ? '1' : '0';
        }
        ['ishold', 'isremove', 'isretransact'].forEach(field => {
            if (processed[field] !== undefined) {
                processed[field] = processed[field] ? 1 : 0;
            }
        });

        ['transac_date', 'payout_date', 'date_hold', 'date_removed'].forEach(field => {
            if (processed[field] && !isNaN(Date.parse(processed[field]))) {
                processed[field] = new Date(processed[field]).toISOString().split('T')[0];
            } else {
                processed[field] = null;
            }
        });

        const fieldsToUpper = [
            'unit_type', 'return_status', 'item_category', 'item_sub_category',
            'item_category_remarks', 'transac_by_fullname', 'removed_by_name',
            'additional_info', 'remarks', 'return_status', 'removed_by_name',
            'item_category', 'item_sub_category', 'item_category_remarks'
        ];
        fieldsToUpper.forEach(field => {
            if (processed[field] && typeof processed[field] === 'string') {
                processed[field] = processed[field].toUpperCase().trim();
            }
        });

        return processed;
    }

    async ensureTablesExist() {
        await this.targetPool.query(`
            CREATE TABLE IF NOT EXISTS voucher (
                voucher_id VARCHAR(36),
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
                PRIMARY KEY (voucher_id, reference_no),
                INDEX idx_reference_no (reference_no)
            )
        `);

        await this.targetPool.query(`
            CREATE TABLE IF NOT EXISTS voucher_transaction (
                voucher_details_id VARCHAR(36),
                transaction_id VARCHAR(36),
                reference_no VARCHAR(15),
                supplier_id VARCHAR(36),
                sub_program_id VARCHAR(36),
                fund_id VARCHAR(36),
                quantity DECIMAL(11,2),
                amount DECIMAL(11,2),
                total_amount DECIMAL(11,2),
                cash_added DECIMAL(11,2),
                unit_type VARCHAR(45),
                additional_info MEDIUMTEXT,
                latitude DECIMAL(10,8),
                longitude DECIMAL(11,8),
                transac_date TIMESTAMP,
                transac_by_id VARCHAR(36),
                transac_by_fullname VARCHAR(255),
                payout ENUM('1','0'),
                payout_date DATETIME,
                date_hold DATE,
                remarks TEXT,
                batch_id VARCHAR(36),
                ishold TINYINT(1),
                return_status VARCHAR(50),
                isremove TINYINT(1),
                date_removed DATETIME,
                removed_by_id VARCHAR(38),
                removed_by_name VARCHAR(38),
                item_category VARCHAR(50),
                item_sub_category MEDIUMTEXT,
                item_category_remarks MEDIUMTEXT,
                isretransact TINYINT(1),
                PRIMARY KEY (voucher_details_id, reference_no),
                INDEX idx_reference_no (reference_no)
            )
        `);
    }

    async processBatch(batch) {
        let processedVoucherCount = 0;
        let processedTransactionCount = 0;
        let skippedVoucherCount = 0;
        let skippedTransactionCount = 0;
        const batchErrors = [];
        const batchWarnings = [];

        const runConcurrently = async (tasks) => {
            const results = [];
            while (tasks.length > 0) {
                const currentBatch = tasks.splice(0, this.concurrencyLimit);
                const batchResults = await Promise.all(currentBatch.map(task => task().catch(err => ({ error: err }))));
                results.push(...batchResults);
            }
            return results;
        };

        const tasks = batch.map(record => async () => {
            try {
                if (!record.reference_no) {
                    batchWarnings.push({
                        log_id: record.log_id,
                        message: 'Skipped due to missing reference number'
                    });
                    skippedVoucherCount++;
                    skippedTransactionCount++;
                    return;
                }

                const [voucherData, transactionData] = await Promise.all([
                    this.getVoucherData(record.reference_no),
                    this.getTransactionData(record.reference_no)
                ]);

                let voucherResult = { processed: 0, skipped: 0, warnings: [] };
                if (voucherData && voucherData.length > 0) {
                    voucherResult = await this.transferVoucherData(voucherData, record.reference_no);
                } else {
                    batchWarnings.push({
                        log_id: record.log_id,
                        message: `No voucher data for reference ${record.reference_no}`
                    });
                    skippedVoucherCount++;
                }

                let transactionResult = { processed: 0, skipped: 0, warnings: [] };
                if (transactionData && transactionData.length > 0) {
                    transactionResult = await this.transferTransactionData(transactionData, record.reference_no);
                } else {
                    batchWarnings.push({
                        log_id: record.log_id,
                        message: `No transaction data for reference ${record.reference_no}`
                    });
                    skippedTransactionCount++;
                }

                processedVoucherCount += voucherResult.processed;
                processedTransactionCount += transactionResult.processed;
                skippedVoucherCount += voucherResult.skipped;
                skippedTransactionCount += transactionResult.skipped;
                batchWarnings.push(...voucherResult.warnings, ...transactionResult.warnings);
            } catch (error) {
                const errorDetails = {
                    log_id: record.log_id,
                    reference_no: record.reference_no,
                    error: error.message,
                    stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
                    timestamp: new Date().toISOString()
                };
                batchErrors.push(errorDetails);
                skippedVoucherCount++;
                skippedTransactionCount++;
                logger.error(`Failed to process record ${record.log_id}`, {
                    reference_no: record.reference_no,
                    error: error.message,
                    stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
                });
            }
        });

        await runConcurrently(tasks);

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
            processedVoucherCount,
            processedTransactionCount,
            skippedVoucherCount,
            skippedTransactionCount,
            errors: batchErrors,
            warnings: batchWarnings
        };
    }

    async runEtlProcess() {
        try {
            this.lastRun = new Date();
            let offset = 0;
            let totalVoucherProcessed = 0;
            let totalTransactionProcessed = 0;
            let totalVoucherSkipped = 0;
            let totalTransactionSkipped = 0;
            let lastLoggedProgress = -1;

            await this.ensureTablesExist();

            const totalRecords = await this.etlLogger.getTotalRecords();
            logger.log(`Starting ETL process. Total records: ${totalRecords}`);

            if (totalRecords === 0) {
                logger.log('No records to process for ETL');
                return {
                    vouchersProcessed: 0,
                    transactionsProcessed: 0,
                    vouchersSkipped: 0,
                    transactionsSkipped: 0,
                    startTime: this.lastRun,
                    endTime: new Date()
                };
            }

            while (offset < totalRecords) {
                const currentOffset = offset;
                logger.log(`Processing batch: ${currentOffset} to ${Math.min(currentOffset + this.batchSize - 1, totalRecords - 1)}`);

                const batch = await this.etlLogger.getRecordsBatch(currentOffset, this.batchSize);
                if (batch.length === 0) break;

                const result = await this.processBatch(batch);
                totalVoucherProcessed += result.processedVoucherCount;
                totalTransactionProcessed += result.processedTransactionCount;
                totalVoucherSkipped += result.skippedVoucherCount;
                totalTransactionSkipped += result.skippedTransactionCount;
                offset += this.batchSize;

                const currentProgress = Math.min(Math.round((offset / totalRecords) * 100), 100);
                if (currentProgress > lastLoggedProgress) {
                    logger.log(`Progress: ${currentProgress}% (${Math.min(offset, totalRecords)}/${totalRecords})`);
                    lastLoggedProgress = currentProgress;
                }

                await new Promise(resolve => setTimeout(resolve, 50));
            }

            logger.log(`ETL completed.
                Vouchers - Processed: ${totalVoucherProcessed}, Skipped: ${totalVoucherSkipped}
                Transactions - Processed: ${totalTransactionProcessed}, Skipped: ${totalTransactionSkipped}`);

            return {
                vouchersProcessed: totalVoucherProcessed,
                transactionsProcessed: totalTransactionProcessed,
                vouchersSkipped: totalVoucherSkipped,
                transactionsSkipped: totalTransactionSkipped,
                startTime: this.lastRun,
                endTime: new Date()
            };
        } catch (error) {
            logger.error('ETL process failed', {
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
            hour12: false,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }

    async stopEtlProcess() {
        const stopTime = this.getPHTTimestamp();
        logger.log(`ETL stopped at ${stopTime}`);
        return {
            message: 'ETL stopped successfully',
            lastRun: this.lastRun,
            stopTime: stopTime
        };
    }
}

module.exports = EtlService;