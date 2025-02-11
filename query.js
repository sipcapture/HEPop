import { DuckDBInstance } from '@duckdb/node-api';
import path from 'path';
import fs from 'fs';

class QueryClient {
  constructor(baseDir = './data', bufferManager = null) {
    this.baseDir = baseDir;
    this.db = null;
    this.connection = null;
    this.defaultTimeRange = 10 * 60 * 1000000000; // 10 minutes in nanoseconds
    this.buffer = bufferManager; // Store reference to buffer manager
  }

  async initialize() {
    try {
      this.db = await DuckDBInstance.create(':memory:');
      // Create initial connection
      this.connection = await this.db.connect();
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

  // Helper to determine SQL type from JavaScript value
  getColumnType(value) {
    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        return 'BIGINT';
      }
      return 'DOUBLE';
    }
    if (typeof value === 'boolean') {
      return 'BOOLEAN';
    }
    return 'VARCHAR';
  }

  async query(sql, options = {}) {
    const parsed = this.parseQuery(sql);
    
    if (!this.buffer) {
      throw new Error('No buffer manager available');
    }

    try {
      const dbName = options.db || 'hep';
      const files = await this.getFilesForTimeRange(parsed.timeRange, dbName);

      await this.connection.runAndReadAll(`DROP TABLE IF EXISTS buffer_data`);

      const buffer = this.buffer.buffers.get(parsed.type);
      if (buffer?.rows?.length) {
        // Get column types from first row
        const columnTypes = new Map();
        const firstRow = buffer.rows[0];
        Object.entries(firstRow).forEach(([key, value]) => {
          if (!['timestamp', 'tags'].includes(key)) {
            columnTypes.set(key, this.getColumnType(value));
          }
        });

        // Create temp table with proper types
        await this.connection.runAndReadAll(`
          CREATE TEMP TABLE buffer_data (
            timestamp TIMESTAMP,
            ${buffer.isLineProtocol ? 
              `tags VARCHAR,
               ${Array.from(columnTypes.entries())
                 .map(([key, type]) => `${key} ${type}`)
                 .join(',\n               ')}` : 
              `rcinfo VARCHAR,
               payload VARCHAR`}
          )
        `);

        // Insert data in batches
        const batchSize = 1000;
        for (let i = 0; i < buffer.rows.length; i += batchSize) {
          const batch = buffer.rows.slice(i, i + batchSize);
          await this.connection.runAndReadAll(`
            INSERT INTO buffer_data 
            SELECT * FROM (VALUES ${batch.map(row => {
              if (buffer.isLineProtocol) {
                return `(
                  TIMESTAMP '${row.timestamp.toISOString()}',
                  '${row.tags}',
                  ${Object.entries(row)
                    .filter(([k]) => !['timestamp', 'tags'].includes(k))
                    .map(([k, v]) => {
                      const type = columnTypes.get(k);
                      if (type === 'VARCHAR') {
                        return `'${v}'`;
                      }
                      return v === null ? 'NULL' : v;
                    })
                    .join(', ')}
                )`;
              } else {
                return `(
                  TIMESTAMP '${new Date(row.create_date).toISOString()}',
                  '${JSON.stringify(row.protocol_header)}',
                  '${row.raw || ''}'
                )`;
              }
            }).join(', ')})
          `);
        }
      } else {
        // Create empty table with proper types
        await this.connection.runAndReadAll(`
          CREATE TEMP TABLE buffer_data (
            timestamp TIMESTAMP,
            ${buffer?.isLineProtocol ? 
              `tags VARCHAR,
               ${Object.entries(buffer?.schema?.fields || {})
                 .filter(([k]) => !['timestamp', 'tags'].includes(k))
                 .map(([k, f]) => `${k} ${f.type === 'DOUBLE' ? 'DOUBLE' : 'VARCHAR'}`)
                 .join(',\n               ')}` : 
              `rcinfo VARCHAR,
               payload VARCHAR`}
          )
        `);
      }

      // Build query with union_by_name=true
      let query;
      if (files.length > 0) {
        const isAggregateQuery = parsed.columns.toLowerCase().includes('count(') || 
                               parsed.columns.toLowerCase().includes('avg(');
        
        if (isAggregateQuery) {
          query = `
            WITH all_data AS (
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
            FROM all_data
            ${parsed.orderBy}
            ${parsed.limit}
          `;
        } else {
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
      const result = await this.connection.runAndReadAll(query);
      
      // Convert result to array of objects
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

  async getFilesForTimeRange(timeRange, dbName = 'hep') {
    if (!this.buffer) {
      throw new Error('No buffer manager available');
    }

    // Use buffer manager's metadata
    const dbPath = path.join(
      this.baseDir,
      this.buffer.writerId,
      'dbs',
      `${dbName}-${this.buffer.metadata.next_db_id}`
    );

    try {
      const files = [];
      const types = await fs.promises.readdir(dbPath);
      
      for (const type of types) {
        if (!type.startsWith('hep_')) continue;
        
        const typePath = path.join(dbPath, type);
        // Use buffer manager's method to get metadata
        const metadata = await this.buffer.getTypeMetadata(type);
        
        // Filter files within time range
        const relevantFiles = metadata.files.filter(file => {
          return file.min_time <= timeRange.end && file.max_time >= timeRange.start;
        });
        
        files.push(...relevantFiles);
      }
      
      return files;
    } catch (error) {
      if (error.code === 'ENOENT') {
        return [];
      }
      throw error;
    }
  }

  async close() {
    if (this.connection) {
      await this.connection.close();
    }
    if (this.db) {
      await this.db.close();
    }
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


