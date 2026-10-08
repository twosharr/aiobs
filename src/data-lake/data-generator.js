import { v4 as uuidv4 } from 'uuid';

export class DataGenerator {
  constructor() {
    this.chaosConfig = {
      activeScenario: null, // 'SCHEMA_DRIFT' | 'NULL_SURGE' | 'DUPLICATE_FLOOD' | 'STALENESS_LAG' | 'COST_SPIKE' | null
      severity: 0.5
    };
  }

  setChaos(scenario, severity = 0.5) {
    this.chaosConfig.activeScenario = scenario;
    this.chaosConfig.severity = severity;
    return { status: 'applied', scenario, severity };
  }

  clearChaos() {
    const prev = this.chaosConfig.activeScenario;
    this.chaosConfig.activeScenario = null;
    return { status: 'cleared', previousScenario: prev };
  }

  /**
   * Fetch live data from the public internet (demonstrates real world ingestion)
   */
  async fetchInternetSource(sourceType = 'users') {
    const endpoints = {
      users: 'https://jsonplaceholder.typicode.com/users',
      posts: 'https://jsonplaceholder.typicode.com/posts',
      orders: 'https://fakestoreapi.com/carts'
    };

    try {
      const url = endpoints[sourceType] || endpoints.users;
      const res = await fetch(url, { signal: AbortSignal.timeout(4000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      return {
        source: `internet_api_${sourceType}`,
        fetchedAt: new Date().toISOString(),
        count: Array.isArray(data) ? data.length : 1,
        records: Array.isArray(data) ? data : [data]
      };
    } catch (err) {
      // Fallback to local high-fidelity synthetic if internet is slow or offline
      return this.generateSyntheticBatch('crm_customers', 10);
    }
  }

  /**
   * Generate realistic enterprise data stream
   */
  generateSyntheticBatch(streamName, count = 25) {
    const batchId = `batch_${Date.now()}_${uuidv4().substring(0, 6)}`;
    const records = [];
    const activeChaos = this.chaosConfig.activeScenario;

    for (let i = 0; i < count; i++) {
      let record = {};

      if (streamName === 'payment_gateway') {
        const isNullSurge = activeChaos === 'NULL_SURGE' && Math.random() < 0.6;
        const isDrift = activeChaos === 'SCHEMA_DRIFT';

        record = {
          transaction_id: `TXN_${100000 + i}`,
          account_id: `ACC_${(i % 15) + 1}`,
          // In schema drift, field name changes or type changes from number to string with symbols
          amount: isDrift ? `$${(Math.random() * 500 + 10).toFixed(2)}` : parseFloat((Math.random() * 500 + 10).toFixed(2)),
          currency: isNullSurge ? null : ['USD', 'EUR', 'GBP', 'JPY'][Math.floor(Math.random() * 4)],
          status: ['SUCCESS', 'SUCCESS', 'SUCCESS', 'PENDING', 'FAILED'][Math.floor(Math.random() * 5)],
          created_at: new Date(Date.now() - (activeChaos === 'STALENESS_LAG' ? 14400000 : Math.random() * 60000)).toISOString()
        };

        if (isDrift) {
          // Rename or add unexpected structural fields
          record.payment_gateway_vendor_v2 = 'STRIPE_GLOBAL';
          record.exchange_fee_estimate = '0.025%';
        }
      } else if (streamName === 'crm_customers') {
        const isNullSurge = activeChaos === 'NULL_SURGE' && Math.random() < 0.5;
        const isDrift = activeChaos === 'SCHEMA_DRIFT';

        record = {
          customer_id: `CUST_${2000 + i}`,
          full_name: `Customer ${i}`,
          email: isNullSurge ? null : `client_${i}@enterprise-corp.com`,
          tier: ['GOLD', 'PLATINUM', 'SILVER', 'STANDARD'][i % 4],
          country: ['US', 'DE', 'GB', 'IN', 'FR'][i % 5],
          signup_date: new Date(Date.now() - i * 86400000).toISOString()
        };

        if (isDrift) {
          // Schema drift: cust_id renamed, phone number format drifted
          record.user_legacy_guid = uuidv4();
          record.tax_identifier_raw = 'VAT-EU-9921';
        }
      } else {
        // default e-commerce orders
        record = {
          order_id: `ORD_${50000 + i}`,
          customer_id: `CUST_${2000 + (i % 20)}`,
          items_count: Math.floor(Math.random() * 6) + 1,
          total_price: parseFloat((Math.random() * 300 + 15).toFixed(2)),
          fulfillment_status: 'PROCESSING',
          timestamp: new Date().toISOString()
        };
      }

      records.push(record);

      // Duplicate flood injection
      if (activeChaos === 'DUPLICATE_FLOOD' && Math.random() < 0.3) {
        records.push({ ...record }); // Duplicate record
      }
    }

    return {
      batchId,
      source: streamName,
      generatedAt: new Date().toISOString(),
      activeChaos,
      records
    };
  }
}

export const dataGenerator = new DataGenerator();
