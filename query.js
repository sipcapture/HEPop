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
        const metadataPath = path.join(typePath, 'metadata.json');
        const metadata = JSON.parse(await fs.promises.readFile(metadataPath, 'utf8'));

        const relevantFiles = metadata.files.filter(file => {
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

    // Extract type
    const typeMatch = sql.match(/FROM\s+hep_(\d+)/i);
    const type = typeMatch ? parseInt(typeMatch[1]) : null;

    // Extract time range
    const timeMatch = sql.match(/time\s*(>=|>|<=|<|=)\s*'([^']+)'/i);
    let timeRange;

    if (timeMatch) {
      const operator = timeMatch[1];
      const timestamp = new Date(timeMatch[2]).getTime() * 1000000; // to nanoseconds

      switch (operator) {
        case '>=':
        case '>':
          timeRange = { start: timestamp, end: null };
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
        throw new Error('Could not determine HEP type from query');
      }

      console.log('Parsed query:', parsed);

      const files = await this.findRelevantFiles(parsed.type, parsed.timeRange);
      if (!files.length) {
        console.log('No files found matching query criteria');
        return [];
      }

      console.log(`Found ${files.length} relevant files`);

      // Build the query using the parsed components
      const timeRangeCondition = `time >= ${parsed.timeRange.start} AND time <= ${parsed.timeRange.end}`;
      
      const query = `
        WITH source AS (
          ${files.map(f => `SELECT * FROM '${f.path}'`).join('\nUNION ALL\n')}
        )
        SELECT ${parsed.columns}
        FROM source
        WHERE ${timeRangeCondition} ${parsed.conditions}
        ${parsed.orderBy}
        ${parsed.limit}
      `;

      console.log('Executing query:', query);
      return await this.db.query(query);
    } catch (error) {
      console.error('Query error:', error);
      throw error;
    }
  }

  async close() {
    if (this.db) {
      await this.db.close();
      this.db = null;
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
