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

      console.log(`Found ${files.length} relevant files:`, files.map(f => f.path));

      // Get a connection
      const connection = await this.db.connect();

      try {
        // Build the query based on requested columns
        let selectClause;
        if (parsed.columns === '*') {
          selectClause = `
            timestamp,
            rcinfo,
            payload
          `;
        } else {
          // Map specific columns to their source
          selectClause = parsed.columns
            .split(',')
            .map(col => {
              col = col.trim();
              switch (col) {
                case 'time': return 'timestamp as time';
                case 'src_ip': return "rcinfo::json->>'srcIp' as src_ip";
                case 'dst_ip': return "rcinfo::json->>'dstIp' as dst_ip";
                case 'src_port': return "rcinfo::json->>'srcPort' as src_port";
                case 'dst_port': return "rcinfo::json->>'dstPort' as dst_port";
                case 'time_sec': return "rcinfo::json->>'timeSeconds' as time_sec";
                case 'time_usec': return "rcinfo::json->>'timeUseconds' as time_usec";
                default: return col;
              }
            })
            .join(', ');
        }

        const query = `
          SELECT ${selectClause}
          FROM read_parquet([${files.map(f => `'${f.path}'`).join(', ')}])
          ${parsed.timeRange ? `WHERE timestamp >= TIMESTAMP '${new Date(parsed.timeRange.start / 1000000).toISOString()}'
            AND timestamp <= TIMESTAMP '${new Date(parsed.timeRange.end / 1000000).toISOString()}'` : ''}
          ${parsed.conditions}
          ${parsed.orderBy}
          ${parsed.limit}
        `;

        console.log('Executing query:', query);
        const result = await connection.query(query);
        
        // Convert result to array of objects
        const rows = [];
        for (const row of result) {
          const obj = {};
          for (const key in row) {
            // Convert timestamp to ISO string
            if (key === 'timestamp') {
              obj[key] = new Date(row[key]).toISOString();
            } else if (key === 'rcinfo' && typeof row[key] === 'string') {
              // Parse rcinfo JSON if it's a string
              try {
                obj[key] = JSON.parse(row[key]);
              } catch (e) {
                obj[key] = row[key];
              }
            } else {
              obj[key] = row[key];
            }
          }
          rows.push(obj);
        }

        return rows;
      } finally {
        await connection.close();
      }
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
