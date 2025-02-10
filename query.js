import { DuckDBInstance } from '@duckdb/node-api';
import path from 'path';
import fs from 'fs';

class QueryClient {
  constructor(baseDir = './data') {
    this.baseDir = baseDir;
    this.db = null;
    this.defaultTimeRange = 10 * 60 * 1000000000; // 10 minutes in nanoseconds
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

  async query(sql) {
    if (!this.db) {
      throw new Error('QueryClient not initialized');
    }

    try {
      const parsed = this.parseQuery(sql);
      if (!parsed.type) {
        throw new Error('Could not determine type from query');
      }

      const files = await this.findRelevantFiles(parsed.type, parsed.timeRange);
      const connection = await this.db.connect();

      try {
        // Get buffered data for this type
        const buffer = this.buffer?.buffers.get(parsed.type);
        let query;

        if (buffer?.rows?.length) {
          // Create temp table from buffer
          await connection.query(`
            CREATE TEMP TABLE buffer_data AS 
            SELECT * FROM (
              VALUES ${buffer.rows.map(row => `(
                ${buffer.isLineProtocol ? 
                  `'${row.timestamp.toISOString()}', '${row.tags}', ${Object.entries(row).filter(([k]) => !['timestamp', 'tags'].includes(k)).map(([,v]) => typeof v === 'string' ? `'${v}'` : v).join(', ')}` :
                  `'${new Date(row.create_date).toISOString()}', '${JSON.stringify(row.protocol_header)}', '${row.raw || ''}'`}
              )`).join(', ')}
            ) t(${buffer.isLineProtocol ? 
              `timestamp, tags, ${Object.keys(buffer.rows[0]).filter(k => !['timestamp', 'tags'].includes(k)).join(', ')}` : 
              'timestamp, rcinfo, payload'})
          `);

          // Union buffer with parquet data
          query = `
            WITH parquet_data AS (
              SELECT ${parsed.columns}
              FROM read_parquet([${files.map(f => `'${f.path}'`).join(', ')}])
              ${parsed.timeRange ? `WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
                AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'` : ''}
              ${parsed.conditions}
            )
            SELECT * FROM (
              SELECT * FROM parquet_data
              UNION ALL
              SELECT ${parsed.columns} FROM buffer_data
              WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
              AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'
              ${parsed.conditions}
            )
            ${parsed.orderBy}
            ${parsed.limit}
          `;
        } else {
          // No buffer data, just query parquet
          query = `
            SELECT ${parsed.columns}
            FROM read_parquet([${files.map(f => `'${f.path}'`).join(', ')}])
            ${parsed.timeRange ? `WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
              AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'` : ''}
            ${parsed.conditions}
            ${parsed.orderBy}
            ${parsed.limit}
          `;
        }

        const reader = await connection.runAndReadAll(query);
        return reader.getRows().map(row => {
          const obj = {};
          reader.columnNames().forEach((col, i) => {
            obj[col] = row[i];
          });
          return obj;
        });
      } finally {
        await connection.close();
      }
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


