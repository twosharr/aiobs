export class DataContractsValidator {
  constructor() {
    this.contracts = {
      payment_gateway: [
        {
          name: 'expect_transaction_id_not_null',
          type: 'not_null',
          column: 'transaction_id',
          criticality: 'CRITICAL'
        },
        {
          name: 'expect_transaction_id_unique',
          type: 'unique',
          column: 'transaction_id',
          criticality: 'HIGH'
        },
        {
          name: 'expect_currency_in_set',
          type: 'in_set',
          column: 'currency',
          allowed: ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD'],
          criticality: 'HIGH'
        },
        {
          name: 'expect_amount_positive',
          type: 'numeric_range',
          column: 'amount',
          min: 0.01,
          max: 1000000,
          criticality: 'CRITICAL'
        }
      ],
      crm_customers: [
        {
          name: 'expect_email_not_null',
          type: 'not_null',
          column: 'email',
          criticality: 'CRITICAL'
        },
        {
          name: 'expect_customer_id_unique',
          type: 'unique',
          column: 'customer_id',
          criticality: 'CRITICAL'
        }
      ]
    };
  }

  validate(sourceName, records) {
    const rules = this.contracts[sourceName] || [];
    if (rules.length === 0) {
      return { score: 100, passedCount: records.length, failedCount: 0, badRecords: [], ruleResults: [] };
    }

    const seenUnique = {};
    const badRecordsMap = new Map();
    const ruleResults = [];

    for (const rule of rules) {
      let passed = 0;
      let failed = 0;
      const failures = [];

      records.forEach((row, idx) => {
        const val = row[rule.column];
        let rowValid = true;
        let failReason = '';

        if (rule.type === 'not_null') {
          if (val === null || val === undefined || val === '') {
            rowValid = false;
            failReason = `Column '${rule.column}' is null or empty`;
          }
        } else if (rule.type === 'unique') {
          const key = `${rule.column}:${val}`;
          if (seenUnique[key]) {
            rowValid = false;
            failReason = `Duplicate key '${val}' in column '${rule.column}'`;
          } else {
            seenUnique[key] = true;
          }
        } else if (rule.type === 'in_set') {
          if (val && !rule.allowed.includes(val)) {
            rowValid = false;
            failReason = `Value '${val}' not in allowed set [${rule.allowed.join(', ')}]`;
          }
        } else if (rule.type === 'numeric_range') {
          const num = typeof val === 'number' ? val : parseFloat(val);
          if (isNaN(num) || num < rule.min || num > rule.max) {
            rowValid = false;
            failReason = `Value '${val}' out of valid range (${rule.min}-${rule.max})`;
          }
        }

        if (rowValid) {
          passed++;
        } else {
          failed++;
          failures.push({ idx, row, failReason });
          if (!badRecordsMap.has(idx)) {
            badRecordsMap.set(idx, { row, violations: [failReason], criticality: rule.criticality });
          } else {
            badRecordsMap.get(idx).violations.push(failReason);
          }
        }
      });

      ruleResults.push({
        ruleName: rule.name,
        column: rule.column,
        criticality: rule.criticality,
        passed,
        failed,
        successRate: records.length > 0 ? parseFloat(((passed / records.length) * 100).toFixed(1)) : 100
      });
    }

    const badRecords = Array.from(badRecordsMap.values());
    const validRecords = records.filter((_, idx) => !badRecordsMap.has(idx));
    const totalChecks = rules.length * records.length;
    const totalPassedChecks = ruleResults.reduce((acc, r) => acc + r.passed, 0);
    const qualityScore = totalChecks > 0 ? parseFloat(((totalPassedChecks / totalChecks) * 100).toFixed(1)) : 100;

    return {
      score: qualityScore,
      totalRecords: records.length,
      validCount: validRecords.length,
      badCount: badRecords.length,
      validRecords,
      badRecords,
      ruleResults
    };
  }
}

export const dataContractsValidator = new DataContractsValidator();
