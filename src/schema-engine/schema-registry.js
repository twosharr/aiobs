import crypto from 'crypto';

export class SchemaRegistry {
  constructor() {
    // Registered Baseline Schemas for enterprise entities
    this.baselines = {
      payment_gateway: {
        entity: 'transactions',
        version: '1.0.0',
        columns: {
          transaction_id: { type: 'string', required: true },
          account_id: { type: 'string', required: true },
          amount: { type: 'number', required: true },
          currency: { type: 'string', required: true },
          status: { type: 'string', required: true },
          created_at: { type: 'string', required: true }
        }
      },
      crm_customers: {
        entity: 'customers',
        version: '1.0.0',
        columns: {
          customer_id: { type: 'string', required: true },
          full_name: { type: 'string', required: true },
          email: { type: 'string', required: true },
          tier: { type: 'string', required: false },
          country: { type: 'string', required: true },
          signup_date: { type: 'string', required: true }
        }
      }
    };

    // History of detected schema drift events
    this.driftEvents = [];
  }

  /**
   * Infer schema fingerprint from a batch of records
   */
  fingerprint(records) {
    if (!records || records.length === 0) return { columns: {}, hash: 'empty' };

    const sample = records.slice(0, 50);
    const inferred = {};

    for (const row of sample) {
      for (const [key, val] of Object.entries(row)) {
        let detectedType = typeof val;
        if (val === null || val === undefined) {
          detectedType = 'null';
        } else if (Array.isArray(val)) {
          detectedType = 'array';
        } else if (typeof val === 'number') {
          detectedType = Number.isInteger(val) ? 'integer' : 'float';
        }

        if (!inferred[key]) {
          inferred[key] = {
            types: new Set([detectedType]),
            nullCount: val === null ? 1 : 0,
            samples: [val]
          };
        } else {
          inferred[key].types.add(detectedType);
          if (val === null) inferred[key].nullCount++;
          if (inferred[key].samples.length < 3 && val !== null) inferred[key].samples.push(val);
        }
      }
    }

    const columns = {};
    for (const [col, meta] of Object.entries(inferred)) {
      // primary type
      const typesArr = Array.from(meta.types).filter(t => t !== 'null');
      columns[col] = {
        primaryType: typesArr[0] || 'string',
        typeVariations: Array.from(meta.types),
        nullable: meta.nullCount > 0,
        nullRate: parseFloat((meta.nullCount / sample.length).toFixed(3))
      };
    }

    const hash = crypto.createHash('md5').update(JSON.stringify(Object.keys(columns).sort())).digest('hex');

    return { columns, hash, sampleSize: sample.length };
  }

  /**
   * Compare incoming batch fingerprint against registered baseline
   */
  evaluateDrift(sourceName, batchFingerprint) {
    const baseline = this.baselines[sourceName];
    if (!baseline) {
      return { hasDrift: false, isNewSource: true, message: `New source '${sourceName}' auto-registered.` };
    }

    const addedColumns = [];
    const missingColumns = [];
    const typeMismatches = [];

    const baselineCols = baseline.columns;
    const incomingCols = batchFingerprint.columns;

    // Check for missing baseline columns
    for (const [colName, colDef] of Object.entries(baselineCols)) {
      if (!incomingCols[colName]) {
        if (colDef.required) {
          missingColumns.push({ column: colName, severity: 'CRITICAL', baselineDef: colDef });
        } else {
          missingColumns.push({ column: colName, severity: 'LOW', baselineDef: colDef });
        }
      } else {
        // Type comparison
        const incomingType = incomingCols[colName].primaryType;
        const expectedType = colDef.type;

        // Number/float compatibility
        const isNumeric = (incomingType === 'integer' || incomingType === 'float') && expectedType === 'number';
        if (incomingType !== expectedType && !isNumeric) {
          typeMismatches.push({
            column: colName,
            expected: expectedType,
            received: incomingType,
            severity: 'HIGH'
          });
        }
      }
    }

    // Check for newly added columns
    for (const colName of Object.keys(incomingCols)) {
      if (!baselineCols[colName]) {
        addedColumns.push({
          column: colName,
          detectedType: incomingCols[colName].primaryType,
          severity: 'INFO'
        });
      }
    }

    const hasDrift = addedColumns.length > 0 || missingColumns.length > 0 || typeMismatches.length > 0;
    const isBreaking = missingColumns.some(c => c.severity === 'CRITICAL') || typeMismatches.length > 0;

    const driftReport = {
      sourceName,
      timestamp: new Date().toISOString(),
      hasDrift,
      isBreaking,
      diff: {
        addedColumns,
        missingColumns,
        typeMismatches
      },
      baselineVersion: baseline.version
    };

    if (hasDrift) {
      this.driftEvents.unshift(driftReport);
      if (this.driftEvents.length > 50) this.driftEvents.pop();
    }

    return driftReport;
  }

  /**
   * Canonical Normalization for Silver layer
   */
  normalizeRecord(sourceName, record) {
    const normalized = { ...record };

    if (sourceName === 'payment_gateway') {
      // Cast amount if received as string like "$120.50"
      if (typeof normalized.amount === 'string') {
        const cleaned = normalized.amount.replace(/[^0-9.-]+/g, '');
        normalized.amount = parseFloat(cleaned) || 0.0;
        normalized._type_cast_applied = true;
      }
      // Canonical standard field names
      normalized.canonical_transaction_id = normalized.transaction_id;
      normalized.canonical_timestamp = normalized.created_at || new Date().toISOString();
    }

    if (sourceName === 'crm_customers') {
      normalized.canonical_customer_id = normalized.customer_id;
      normalized.canonical_email = (normalized.email || 'unknown@domain.com').toLowerCase();
    }

    return normalized;
  }
}

export const schemaRegistry = new SchemaRegistry();
