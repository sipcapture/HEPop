import { DuckDBInstance } from '@duckdb/node-api';
import path from 'path';
import fs from 'fs';

class QueryClient {
  constructor(baseDir = './data', bufferManager = null) {
    this.baseDir = baseDir;
    this.db = null;
    this.defaultTimeRange = 10 * 60 * 1000000000; // 10 minutes in nanoseconds
    this.buffer = bufferManager; // Store reference to buffer manager
  }

  async initialize() {
    try {
      this.db = await DuckDBInstance.create(':memory:');
      console.log('Initialized DuckDB for querying');
    } catch (error) {
      console.error('Failed to initialize DuckDB:', error);
      throw error;
    }
  }

  async findRelevantFiles(type, timeRange) {
    const files = [];
    const writers = await fs.promises.readdir(this.baseDir);

    for (const writer of writers) {
      const typePath = path.join(
        this.baseDir,
        writer,
        'dbs',
        'hep-0',
        `hep_${type}-0`
      );

      try {
        // Always read fresh metadata
        const metadataPath = path.join(typePath, 'metadata.json');
        const metadata = JSON.parse(await fs.promises.readFile(metadataPath, 'utf8'));

        // Only use files that exist and match time range
        const relevantFiles = metadata.files.filter(file => {
          // Skip files that don't exist
          if (!fs.existsSync(file.path)) {
            return false;
          }

          const { start, end } = timeRange;
          const fileStart = file.min_time;
          const fileEnd = file.max_time;

          return (!start || fileEnd >= start) && (!end || fileStart <= end);
        });

        files.push(...relevantFiles);
      } catch (error) {
        if (error.code !== 'ENOENT') {
          console.error(`Error reading metadata for type ${type} in ${writer}:`, error);
        }
      }
    }

    return files.sort((a, b) => a.min_time - b.min_time);
  }

  parseQuery(sql) {
    // Extract SELECT columns
    const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/i);
    const columns = selectMatch ? selectMatch[1].trim() : '*';

    // Extract type/measurement name
    const fromMatch = sql.match(/FROM\s+([a-zA-Z0-9_]+)/i);
    let type = null;

    if (fromMatch) {
      const tableName = fromMatch[1];
      // Check if it's a HEP type (hep_NUMBER)
      const hepMatch = tableName.match(/^hep_(\d+)$/i);
      if (hepMatch) {
        type = parseInt(hepMatch[1]);
      } else {
        // It's a Line Protocol measurement name
        type = tableName;
      }
    }


    // Extract time range
    const timeMatch = sql.match(/time\s*(>=|>|<=|<|=)\s*'([^']+)'/i);
    let timeRange;

    if (timeMatch) {
      const operator = timeMatch[1];
      const timestamp = new Date(timeMatch[2]).getTime() * 1000000;
      const now = Date.now() * 1000000;

      switch (operator) {
        case '>=':
        case '>':
          timeRange = { start: timestamp, end: now };
          break;
        case '<=':
        case '<':
          timeRange = { start: null, end: timestamp };
          break;
        case '=':
          timeRange = { start: timestamp, end: timestamp };
          break;
      }
    } else {

      // Default to last 10 minutes
      const now = Date.now() * 1000000;
      timeRange = {
        start: now - this.defaultTimeRange,
        end: now
      };
    }

    // Extract additional WHERE conditions
    const whereClause = sql.match(/WHERE\s+(.*?)(?:\s+(?:ORDER|GROUP|LIMIT|$))/i);
    let conditions = '';
    if (whereClause) {
      conditions = whereClause[1].replace(/time\s*(>=|>|<=|<|=)\s*'[^']+'\s*(AND|OR)?/i, '').trim();
      if (conditions) conditions = `AND ${conditions}`;
    }

    // Extract ORDER BY, LIMIT, etc.
    const orderMatch = sql.match(/ORDER\s+BY\s+(.*?)(?:\s+(?:LIMIT|$))/i);
    const limitMatch = sql.match(/LIMIT\s+(\d+)/i);
    
    const orderBy = orderMatch ? `ORDER BY ${orderMatch[1]}` : '';
    const limit = limitMatch ? `LIMIT ${limitMatch[1]}` : '';

    return {
      columns,
      type,
      timeRange,
      conditions,
      orderBy,
      limit
    };
  }

  async query(sql, options = {}) {
    const parsed = this.parseQuery(sql);
    
    if (!this.buffer) {
      throw new Error('No buffer manager available');
    }

    try {
      const dbName = options.db || 'hep';
      const files = await this.findRelevantFiles(parsed.type, parsed.timeRange);

      // Build query with union_by_name=true
      let query;
      if (files.length > 0) {
        const isAggregateQuery = parsed.columns.toLowerCase().includes('count(') || 
                               parsed.columns.toLowerCase().includes('avg(');
        
        if (isAggregateQuery) {
          // For aggregate queries, apply WHERE conditions before aggregating
          query = `
            WITH filtered_data AS (
              SELECT ${this.buffer.isLineProtocol ? '*' : 'timestamp, rcinfo, payload'} 
              FROM read_parquet([${files.map(f => `'${f.path}'`).join(', ')}], union_by_name=true)
              WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
              AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'
              ${parsed.conditions}
              UNION ALL
              SELECT ${this.buffer.isLineProtocol ? '*' : 'timestamp, rcinfo, payload'}
              FROM buffer_data
              WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
              AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'
              ${parsed.conditions}
            )
            SELECT ${parsed.columns}
            FROM filtered_data
            ${parsed.orderBy}
            ${parsed.limit}
          `;
        } else {
          // Non-aggregate queries remain the same
          query = `
            SELECT ${parsed.columns}
            FROM (
              SELECT ${this.buffer.isLineProtocol ? '*' : 'timestamp, rcinfo, payload'}
              FROM read_parquet([${files.map(f => `'${f.path}'`).join(', ')}], union_by_name=true)
              WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
              AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'
              ${parsed.conditions}
              UNION ALL
              SELECT ${this.buffer.isLineProtocol ? '*' : 'timestamp, rcinfo, payload'}
              FROM buffer_data
              WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
              AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'
              ${parsed.conditions}
            ) combined_data
            ${parsed.orderBy}
            ${parsed.limit}
          `;
        }
      } else {
        // Only query buffer data
        query = `
          SELECT ${parsed.columns}
          FROM buffer_data
          WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
          AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'
          ${parsed.conditions}
          ${parsed.orderBy}
          ${parsed.limit}
        `;
      }

      // Execute query
      const connection = await this.db.connect();
      const result = await connection.runAndReadAll(query);
      return result.getRows().map(row => {
        const obj = {};
        result.columnNames().forEach((col, i) => {
          obj[col] = row[i];
        });
        return obj;
      });

    } catch (error) {
      console.error('Query error:', error);
      throw error;
    }
  }

  async close() {
    // Nothing to clean up
  }
}

// Example usage:
async function main() {
  const client = new QueryClient();
  await client.initialize();

  try {
    const result = await client.query(`
      SELECT * FROM hep_1 
      WHERE time >= '2025-02-08T19:00:00' 
      AND time < '2025-02-08T20:00:00' 
      LIMIT 10
    `);
    console.log('Query result:', result);
  } finally {
    await client.close();
  }
}

if (require.main === module) {
  main().catch(console.error);
}

export default QueryClient;


