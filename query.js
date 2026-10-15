import { DuckDBInstance } from '@duckdb/node-api';
import path from 'path';
import fs from 'fs';

/** Max in-memory rows materialized into SQL per query (safety cap). */
export const MAX_BUFFER_QUERY_ROWS = 5000;

export function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

export function escapeSqlString(value) {
  return String(value).replace(/'/g, "''");
}

export function stripTimePredicates(whereBody) {
  if (!whereBody) return '';
  let rest = whereBody.trim();
  rest = rest.replace(/\btime\s*(>=|>|<=|<|=)\s*'[^']*'/gi, '');
  rest = rest.replace(/^\s*(?:AND|OR)\s+/i, '');
  rest = rest.replace(/\s+(?:AND|OR)\s*$/i, '');
  rest = rest.replace(/\s+(?:AND|OR)\s+(?:AND|OR)\s+/gi, ' AND ');
  rest = rest.replace(/\s{2,}/g, ' ').trim();
  return rest;
}

export function buildTimestampWhereClause(timeRange) {
  const parts = [];
  if (timeRange.start != null) {
    parts.push(
      `timestamp >= TIMESTAMP '${new Date(timeRange.start / 1000000).toISOString()}'`
    );
  }
  if (timeRange.end != null) {
    parts.push(
      `timestamp <= TIMESTAMP '${new Date(timeRange.end / 1000000).toISOString()}'`
    );
  }
  return parts.length ? parts.join(' AND ') : 'TRUE';
}

function sqlLiteral(value) {
  if (value === null || value === undefined) return 'NULL';
  if (value instanceof Date) {
    return `TIMESTAMP '${value.toISOString()}'`;
  }
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
  if (typeof value === 'string') return `'${escapeSqlString(value)}'`;
  return `'${escapeSqlString(JSON.stringify(value))}'`;
}

class QueryClient {
  constructor(baseDir = './data', bufferManager = null) {
    this.baseDir = baseDir;
    this.db = null;
    this.defaultTimeRange = 10 * 60 * 1000000000; // 10 minutes in nanoseconds
    this.buffer = bufferManager;
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
    const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/i);
    const columns = selectMatch ? selectMatch[1].trim() : '*';

    const fromMatch = sql.match(/FROM\s+([a-zA-Z0-9_]+)/i);
    let type = null;

    if (fromMatch) {
      const tableName = fromMatch[1];
      const hepMatch = tableName.match(/^hep_(\d+)$/i);
      if (hepMatch) {
        type = parseInt(hepMatch[1], 10);
      } else {
        type = tableName;
      }
    }

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
        default:
          timeRange = { start: now - this.defaultTimeRange, end: now };
      }
    } else {
      const now = Date.now() * 1000000;
      timeRange = {
        start: now - this.defaultTimeRange,
        end: now
      };
    }

    const whereClause = sql.match(/WHERE\s+(.*?)(?:\s+(?:ORDER|GROUP|LIMIT|$))/i);
    let conditions = '';
    if (whereClause) {
      const rest = stripTimePredicates(whereClause[1]);
      if (rest) {
        conditions = `AND ${rest}`;
      }
    }

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
      limit,
      timestampWhere: buildTimestampWhereClause(timeRange)
    };
  }

  async createBufferTempTable(connection, bufferRows) {
    if (!bufferRows.length) return false;

    const capped = bufferRows.slice(0, MAX_BUFFER_QUERY_ROWS);
    if (bufferRows.length > MAX_BUFFER_QUERY_ROWS) {
      console.warn(
        `Buffer query capped at ${MAX_BUFFER_QUERY_ROWS} rows (${bufferRows.length} in memory)`
      );
    }

    await connection.runAndReadAll('DROP TABLE IF EXISTS buffer_data');

    const firstRow = capped[0];
    const columns = Object.keys(firstRow);
    const columnList = columns.map(quoteIdent).join(', ');

    const valuesSql = capped.map(row => `(
            ${columns.map(col => sqlLiteral(row[col])).join(', ')}
          )`).join(', ');

    await connection.runAndReadAll(`
          CREATE TEMP TABLE buffer_data AS
          SELECT * FROM (VALUES ${valuesSql})
          AS t(${columnList})
        `);
    return true;
  }

  async query(sql, options = {}) {
    const parsed = this.parseQuery(sql);

    if (!this.buffer) {
      throw new Error('No buffer manager available');
    }

    const connection = await this.db.connect();
    try {
      const files = await this.findRelevantFiles(parsed.type, parsed.timeRange);
      const bufferRows = this.buffer.getBufferSnapshot(parsed.type);
      const hasBuffer = await this.createBufferTempTable(connection, bufferRows);

      let query;
      if (files.length > 0) {
        const isAggregateQuery = parsed.columns.toLowerCase().includes('count(') ||
          parsed.columns.toLowerCase().includes('avg(');

        const parquetPaths = files.map(f => `'${escapeSqlString(f.path)}'`).join(', ');
        const bufferUnion = hasBuffer ? `
                UNION ALL
                SELECT * FROM buffer_data
                WHERE ${parsed.timestampWhere}
                ${parsed.conditions}
              ` : '';

        if (isAggregateQuery) {
          query = `
            WITH filtered_data AS (
              SELECT * FROM read_parquet([${parquetPaths}], union_by_name=true)
              WHERE ${parsed.timestampWhere}
              ${parsed.conditions}
              ${bufferUnion}
            )
            SELECT ${parsed.columns}
            FROM filtered_data
          `;
        } else {
          query = `
            SELECT ${parsed.columns}
            FROM (
              SELECT * FROM read_parquet([${parquetPaths}], union_by_name=true)
              WHERE ${parsed.timestampWhere}
              ${parsed.conditions}
              ${bufferUnion}
            ) combined_data
            ${parsed.orderBy}
            ${parsed.limit}
          `;
        }
      } else if (hasBuffer) {
        query = `
          SELECT ${parsed.columns}
          FROM buffer_data
          WHERE ${parsed.timestampWhere}
          ${parsed.conditions}
          ${parsed.orderBy}
          ${parsed.limit}
        `;
      } else {
        return [];
      }

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
    } finally {
      await connection.close();
    }
  }

  async close() {
    if (this.db) {
      await this.db.close();
      this.db = null;
    }
  }
}

export default QueryClient;
