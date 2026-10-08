import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const STORAGE_ROOT = path.resolve(__dirname, '../../storage/lakehouse');

// Ensure lakehouse directories exist
const TIERS = ['bronze', 'silver', 'gold', 'quarantine'];

export class Lakehouse {
  constructor() {
    this.storagePath = STORAGE_ROOT;
    this.initDirectories();
  }

  initDirectories() {
    for (const tier of TIERS) {
      const tierPath = path.join(this.storagePath, tier);
      if (!fs.existsSync(tierPath)) {
        fs.mkdirSync(tierPath, { recursive: true });
      }
    }
  }

  /**
   * Save incoming raw batch into Bronze Landing
   */
  writeBronze(sourceName, batchId, records, metadata = {}) {
    const timestamp = new Date().toISOString();
    const envelope = {
      batchId,
      sourceName,
      tier: 'bronze',
      ingestedAt: timestamp,
      recordCount: records.length,
      metadata,
      records
    };

    const filePath = path.join(this.storagePath, 'bronze', `${sourceName}_${batchId}.json`);
    fs.writeFileSync(filePath, JSON.stringify(envelope, null, 2));
    return envelope;
  }

  /**
   * Save validated, schema-conformed records to Silver
   */
  writeSilver(tableName, batchId, records, qualityMetrics = {}) {
    const timestamp = new Date().toISOString();
    const payload = {
      batchId,
      tableName,
      tier: 'silver',
      processedAt: timestamp,
      recordCount: records.length,
      qualityScore: qualityMetrics.score ?? 100,
      qualityMetrics,
      records
    };

    const filePath = path.join(this.storagePath, 'silver', `${tableName}_${batchId}.json`);
    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
    return payload;
  }

  /**
   * Save aggregated business marts into Gold
   */
  writeGold(martName, batchId, records, summaryMetrics = {}) {
    const timestamp = new Date().toISOString();
    const payload = {
      batchId,
      martName,
      tier: 'gold',
      aggregatedAt: timestamp,
      recordCount: records.length,
      summaryMetrics,
      records
    };

    const filePath = path.join(this.storagePath, 'gold', `${martName}_${batchId}.json`);
    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
    return payload;
  }

  /**
   * Move rejected records to Quarantine with diagnosis
   */
  writeQuarantine(sourceName, batchId, badRecords, violationReason, agentDiagnosis = null) {
    const timestamp = new Date().toISOString();
    const payload = {
      batchId,
      sourceName,
      tier: 'quarantine',
      quarantinedAt: timestamp,
      violatedCount: badRecords.length,
      violationReason,
      agentDiagnosis,
      records: badRecords
    };

    const filePath = path.join(this.storagePath, 'quarantine', `${sourceName}_${batchId}_quarantine.json`);
    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
    return payload;
  }

  /**
   * Read recent files summary across all tiers
   */
  getOverview() {
    const overview = {};
    for (const tier of TIERS) {
      const tierPath = path.join(this.storagePath, tier);
      const files = fs.existsSync(tierPath) ? fs.readdirSync(tierPath) : [];
      overview[tier] = {
        totalFiles: files.length,
        recentFiles: files.slice(-5)
      };
    }
    return overview;
  }
}

export const lakehouse = new Lakehouse();
