import * as arrow from "apache-arrow";
import initWasm, {
  Compression,
  readParquet,
  Table,
  writeParquet,
  WriterPropertiesBuilder,
} from "parquet-wasm";
import hepjs from 'hep-js';
import { getSIP } from 'parsip';
import path from 'path';
import fs from 'fs';
import duckdb from '@duckdb/node-api';
import QueryClient from './query.js';

class ParquetBufferManager {
  constructor(flushInterval = 10000, bufferSize = 1000) {
    this.buffers = new Map();
    this.flushInterval = flushInterval;
    this.bufferSize = bufferSize;
    this.metadata = this.initializeMetadata();
    this.baseDir = process.env.PARQUET_DIR || './data';
    this.writerProperties = new WriterPropertiesBuilder()
      .setCompression(Compression.ZSTD)
      .build();

    // Initialize WebAssembly
    this.initializeWasm();
    this.startFlushInterval();
  }

  async initializeWasm() {
    await initWasm();
    console.log('Parquet WASM initialized');
  }

  initializeMetadata() {
    const hostname = process.env.WRITER_ID || require('os').hostname();
    return {
      writer_id: hostname,
      next_file_id: 0,
      next_db_id: 0,
      next_table_id: 0,
      next_column_id: 0,
      snapshot_sequence_number: 1,
      wal_file_sequence_number: 0,
      catalog_sequence_number: 0,
      parquet_size_bytes: 0,
      row_count: 0,
      min_time: null,
      max_time: null,
      databases: [[0, { tables: new Map() }]]
    };
  }

  add(type, data) {
    if (!this.buffers.has(type)) {
      this.buffers.set(type, []);
    }
    this.buffers.get(type).push(data);
    
    if (this.buffers.get(type).length >= this.bufferSize) {
      this.flush(type);
    }
  }

  startFlushInterval() {
    setInterval(() => {
      for (const type of this.buffers.keys()) {
        this.flush(type);
      }
    }, this.flushInterval);
  }

  getFilePath(type, timestamp) {
    const date = new Date(timestamp);
    const datePath = date.toISOString().split('T')[0];
    const hour = date.getHours().toString().padStart(2, '0');
    const minute = Math.floor(date.getMinutes() / 10) * 10;
    const minutePath = minute.toString().padStart(2, '0');
    
    return path.join(
      this.baseDir,
      this.metadata.writer_id,
      'dbs',
      `hep-${this.metadata.next_db_id}`,
      `hep_${type}-${this.metadata.next_table_id}`,
      datePath,
      `${hour}-${minutePath}`,
      `${this.metadata.wal_file_sequence_number.toString().padStart(10, '0')}.parquet`
    );
  }

  async flush(type) {
    const buffer = this.buffers.get(type);
    if (!buffer?.length) return;

    try {
      const timestamps = [];
      const rcinfos = [];
      const payloads = [];

      buffer.forEach(data => {
        timestamps.push(new Date(data.create_date));
        rcinfos.push(JSON.stringify(data.protocol_header));
        payloads.push(data.raw || '');
      });

      const table = arrow.tableFromArrays({
        timestamp: timestamps,
        rcinfo: rcinfos,
        payload: payloads
      });

      const wasmTable = Table.fromIPCStream(arrow.tableToIPC(table, "stream"));
      const parquetData = writeParquet(wasmTable, this.writerProperties);

      // Get file path based on first timestamp
      const filePath = this.getFilePath(type, timestamps[0]);
      
      // Ensure directory exists
      await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
      
      // Write parquet file
      await fs.promises.writeFile(filePath, parquetData);

      // Update metadata
      this.updateMetadata(type, filePath, parquetData.length, buffer.length, timestamps);
      
      // Clear buffer
      this.buffers.set(type, []);
      
      console.log(`Wrote ${buffer.length} records to ${filePath}`);
    } catch (error) {
      console.error(`Parquet flush error:`, error);
    }
  }

  updateMetadata(type, filePath, sizeBytes, rowCount, timestamps) {
    const minTime = Math.min(...timestamps.map(t => t.getTime() * 1000000));
    const maxTime = Math.max(...timestamps.map(t => t.getTime() * 1000000));
    const chunkTime = Math.floor(minTime / 600000000000) * 600000000000;

    const fileInfo = {
      id: this.metadata.next_file_id++,
      path: filePath,
      size_bytes: sizeBytes,
      row_count: rowCount,
      chunk_time: chunkTime,
      min_time: minTime,
      max_time: maxTime
    };

    // Update tables map
    const tables = this.metadata.databases[0][1].tables;
    if (!tables.has(type)) {
      tables.set(type, []);
    }
    tables.get(type).push(fileInfo);

    // Update global metadata
    this.metadata.parquet_size_bytes += sizeBytes;
    this.metadata.row_count += rowCount;
    this.metadata.min_time = this.metadata.min_time ? 
      Math.min(this.metadata.min_time, minTime) : minTime;
    this.metadata.max_time = this.metadata.max_time ?
      Math.max(this.metadata.max_time, maxTime) : maxTime;
    this.metadata.wal_file_sequence_number++;

    // Write metadata file
    const metadataPath = path.join(this.baseDir, this.metadata.writer_id, 'metadata.json');
    fs.writeFileSync(metadataPath, JSON.stringify(this.metadata, null, 2));
  }

  async close() {
    for (const type of this.buffers.keys()) {
      await this.flush(type);
    }
  }
}

class CompactionManager {
  constructor(bufferManager) {
    this.bufferManager = bufferManager;
    this.compactionIntervals = {
      '10m': 10 * 60 * 1000,
      '1h': 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000
    };
    
    // Initialize DuckDB
    this.db = new duckdb.Database(':memory:');
    this.startCompactionJobs();
    console.log(`Initialized DuckDB ${duckdb.version()} for compaction`);
  }

  startCompactionJobs() {
    // Run compaction checks every minute
    setInterval(() => this.checkAndCompact(), 60 * 1000);
  }

  async checkAndCompact() {
    const metadata = this.bufferManager.metadata;
    const tables = metadata.databases[0][1].tables;

    for (const [type, files] of tables.entries()) {
      await this.compactTimeRange(type, files, '10m', '1h');
      await this.compactTimeRange(type, files, '1h', '24h');
    }
  }

  async compactTimeRange(type, files, fromRange, toRange) {
    const now = Date.now() * 1000000; // Convert to nanoseconds
    const interval = this.compactionIntervals[fromRange];
    const targetInterval = this.compactionIntervals[toRange];
    
    // Group files by their target interval
    const groups = new Map();
    
    files.forEach(file => {
      // Skip files that are too new
      if (now - file.max_time < interval) return;
      
      // Calculate target group timestamp
      const groupTime = Math.floor(file.chunk_time / targetInterval) * targetInterval;
      if (!groups.has(groupTime)) {
        groups.set(groupTime, []);
      }
      groups.get(groupTime).push(file);
    });

    // Compact each group that has enough files
    for (const [groupTime, groupFiles] of groups) {
      if (groupFiles.length < 2) continue;

      await this.compactFiles(type, groupFiles, toRange);
    }
  }

  async compactFiles(type, files, targetRange) {
    try {
      const newPath = this.getCompactedFilePath(type, new Date(files[0].chunk_time / 1000000), targetRange);
      await fs.promises.mkdir(path.dirname(newPath), { recursive: true });

      // Create file list for DuckDB query
      const fileListQuery = files
        .map(f => `'${f.path}'`)
        .join(',');

      // Execute merge query with time-based sorting
      const mergeQuery = `COPY (
        SELECT * FROM read_parquet([${fileListQuery}]) 
        ORDER BY timestamp
      ) TO '${newPath}' (
        FORMAT 'parquet',
        COMPRESSION 'ZSTD',
        ROW_GROUP_SIZE 100000
      );`;

      await new Promise((resolve, reject) => {
        this.db.exec(mergeQuery, (err) => {
          if (err) reject(err);
          else resolve();
        });
      });

      // Get stats from merged file for metadata
      const stats = await this.getFileStats(newPath);
      
      // Perform cleanup and metadata update atomically
      await this.finalizeCompaction(type, files, {
        path: newPath,
        size_bytes: stats.size_bytes,
        row_count: stats.row_count,
        chunk_time: files[0].chunk_time,
        min_time: stats.min_time,
        max_time: stats.max_time,
        range: targetRange
      });

      console.log(`Compacted ${files.length} files into ${newPath} (${stats.row_count} rows)`);
    } catch (error) {
      console.error(`Compaction error for type ${type}:`, error);
      // Cleanup failed compaction file if it exists
      try {
        await fs.promises.unlink(newPath);
      } catch (e) {
        // Ignore cleanup errors
      }
    }
  }

  async getFileStats(filePath) {
    const statsQuery = `
      SELECT 
        COUNT(*) as row_count,
        MIN(timestamp) as min_time,
        MAX(timestamp) as max_time
      FROM read_parquet('${filePath}');
    `;

    const stats = await new Promise((resolve, reject) => {
      this.db.all(statsQuery, (err, rows) => {
        if (err) reject(err);
        else resolve(rows[0]);
      });
    });

    const { size: sizeBytes } = await fs.promises.stat(filePath);

    return {
      size_bytes: sizeBytes,
      row_count: stats.row_count,
      min_time: new Date(stats.min_time).getTime() * 1000000,
      max_time: new Date(stats.max_time).getTime() * 1000000
    };
  }

  async finalizeCompaction(type, oldFiles, newFile) {
    try {
      // Update metadata first
      this.updateCompactionMetadata(type, oldFiles, newFile);

      // Clean up old files and their parent directories
      await this.cleanupCompactedFiles(oldFiles);

      // Write updated metadata to disk
      await this.writeMetadata();
    } catch (error) {
      console.error('Error during compaction finalization:', error);
      // Attempt to rollback by removing the new file
      try {
        await fs.promises.unlink(newFile.path);
      } catch (e) {
        console.error('Error during rollback:', e);
      }
      throw error;
    }
  }

  async cleanupCompactedFiles(files) {
    const dirsToCheck = new Set();

    // Delete files first
    await Promise.all(files.map(async (file) => {
      try {
        await fs.promises.unlink(file.path);
        // Add parent directories for cleanup check
        let dirPath = path.dirname(file.path);
        while (dirPath.startsWith(this.bufferManager.baseDir)) {
          dirsToCheck.add(dirPath);
          dirPath = path.dirname(dirPath);
        }
      } catch (error) {
        console.error(`Error deleting file ${file.path}:`, error);
      }
    }));

    // Clean up empty directories from deepest to shallowest
    const sortedDirs = Array.from(dirsToCheck)
      .sort((a, b) => b.split(path.sep).length - a.split(path.sep).length);

    for (const dir of sortedDirs) {
      try {
        const files = await fs.promises.readdir(dir);
        if (files.length === 0) {
          await fs.promises.rmdir(dir);
        }
      } catch (error) {
        // Ignore errors during directory cleanup
      }
    }
  }

  updateCompactionMetadata(type, oldFiles, newFile) {
    const tables = this.bufferManager.metadata.databases[0][1].tables;
    const fileList = tables.get(type);

    // Remove old files from metadata
    oldFiles.forEach(oldFile => {
      const index = fileList.findIndex(f => f.path === oldFile.path);
      if (index !== -1) {
        // Subtract old file stats from global metadata
        this.bufferManager.metadata.parquet_size_bytes -= oldFile.size_bytes;
        this.bufferManager.metadata.row_count -= oldFile.row_count;
        fileList.splice(index, 1);
      }
    });

    // Add new compacted file
    const newFileEntry = {
      id: this.bufferManager.metadata.next_file_id++,
      ...newFile,
      compaction_level: newFile.range
    };
    fileList.push(newFileEntry);

    // Update global metadata
    this.bufferManager.metadata.parquet_size_bytes += newFile.size_bytes;
    this.bufferManager.metadata.row_count += newFile.row_count;
    this.bufferManager.metadata.min_time = this.bufferManager.metadata.min_time ? 
      Math.min(this.bufferManager.metadata.min_time, newFile.min_time) : newFile.min_time;
    this.bufferManager.metadata.max_time = this.bufferManager.metadata.max_time ?
      Math.max(this.bufferManager.metadata.max_time, newFile.max_time) : newFile.max_time;
  }

  async writeMetadata() {
    const metadataPath = path.join(
      this.bufferManager.baseDir, 
      this.bufferManager.metadata.writer_id, 
      'metadata.json'
    );
    
    // Write to temporary file first
    const tempPath = `${metadataPath}.tmp`;
    await fs.promises.writeFile(
      tempPath, 
      JSON.stringify(this.bufferManager.metadata, null, 2)
    );
    
    // Atomic rename
    await fs.promises.rename(tempPath, metadataPath);
  }

  getCompactedFilePath(type, timestamp, range) {
    const date = timestamp.toISOString().split('T')[0];
    const hour = timestamp.getHours().toString().padStart(2, '0');
    
    return path.join(
      this.bufferManager.baseDir,
      this.bufferManager.metadata.writer_id,
      'compacted',
      range,
      `hep-${this.bufferManager.metadata.next_db_id}`,
      `hep_${type}-${this.bufferManager.metadata.next_table_id}`,
      date,
      hour,
      `${this.bufferManager.metadata.wal_file_sequence_number.toString().padStart(10, '0')}.parquet`
    );
  }

  async close() {
    return new Promise((resolve, reject) => {
      this.db.close((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}

class HEPServer {
  constructor(config = {}) {
    this.debug = config.debug || false;
    this.queryClient = null;
  }

  async initialize() {
    try {
      this.buffer = new ParquetBufferManager();
      await this.buffer.initialize();
      
      this.compaction = new CompactionManager(this.buffer);
      await this.compaction.initialize();

      this.queryClient = new QueryClient(this.buffer.baseDir);
      await this.queryClient.initialize();
      
      await this.startServers();
    } catch (error) {
      console.error('Failed to initialize HEPServer:', error);
      throw error;
    }
  }

  async startServers() {
    const port = parseInt(process.env.PORT) || 9069;
    const httpPort = parseInt(process.env.HTTP_PORT) || (port + 1);
    const host = process.env.HOST || "0.0.0.0";

    try {
      // Start HEP servers (TCP/UDP)
      const tcpServer = Bun.listen({
        hostname: host,
        port: port,
        socket: {
          data: (socket, data) => this.handleData(data, socket),
          error: (socket, error) => console.error('TCP error:', error),
        }
      });

      const udpServer = Bun.udpSocket({
        hostname: host,
        port: port,
        udp: true,
        socket: {
          data: (socket, data) => this.handleData(data, socket),
          error: (socket, error) => console.error('UDP error:', error),
        }
      });

      // Start HTTP server for queries
      const self = this; // Store reference to this
      const httpServer = Bun.serve({
        hostname: host,
        port: httpPort,
        async fetch(req) {
          const url = new URL(req.url);
          
          // Handle query endpoint
          if (url.pathname === '/query') {
            try {
              let query;
              
              if (req.method === 'GET') {
                query = url.searchParams.get('q');
                if (!query) {
                  return new Response('Missing query parameter "q"', { status: 400 });
                }
              } else if (req.method === 'POST') {
                const body = await req.json();
                query = body.query;
                if (!query) {
                  return new Response('Missing query in request body', { status: 400 });
                }
              } else {
                return new Response('Method not allowed', { status: 405 });
              }

              const result = await self.queryClient.query(query);
              return new Response(JSON.stringify(result), {
                headers: { 'Content-Type': 'application/json' }
              });
            } catch (error) {
              console.error('Query error:', error);
              return new Response(JSON.stringify({ error: error.message }), {
                status: 500,
                headers: { 'Content-Type': 'application/json' }
              });
            }
          }

          // Handle other endpoints or 404
          return new Response('Not found', { status: 404 });
        }
      });

      console.log(`HEP Server listening on ${host}:${port} (TCP/UDP)`);
      console.log(`Query API listening on ${host}:${httpPort} (HTTP)`);

      // Store server references
      this.tcpServer = tcpServer;
      this.udpServer = udpServer;
      this.httpServer = httpServer;

      // Handle graceful shutdown
      process.on('SIGTERM', this.shutdown.bind(this));
      process.on('SIGINT', this.shutdown.bind(this));
    } catch (error) {
      console.error('Failed to start servers:', error);
      throw error;
    }
  }

  async shutdown() {
    console.log('Shutting down HEP server...');
    
    if (this.tcpServer) this.tcpServer.close();
    if (this.udpServer) this.udpServer.close();
    if (this.httpServer) this.httpServer.close();
    
    await this.buffer.close();
    await this.compaction.close();
    await this.queryClient.close();
    
    process.exit(0);
  }

  handleData(data, socket) {
    try {
      console.log(`Received ${data.length} bytes from ${socket.remoteAddress}`);
      const processed = this.processHep(data, socket);
      const type = processed.type;
      console.log(`Processed HEP type ${type}, adding to buffer`);
      this.buffer.add(type, processed);
    } catch (error) {
      console.error('Handle data error:', error);
    }
  }

  processHep(data, socket) {
    try {
      const decoded = hepjs.decapsulate(data);
      
      const insert = {
        protocol_header: decoded.rcinfo,
        create_date: this.getHepTimestamp(decoded.rcinfo),
        raw: decoded.payload || "",
        type: decoded.rcinfo.payload_type || decoded.rcinfo.payloadType || 0
      };

      return insert;
    } catch(err) {
      console.error('HEP Processing Error:', err);
      throw err;
    }
  }

  getHepTimestamp(rcinfo) {
    if (!rcinfo.timeSeconds) return new Date();
    return new Date(
      (rcinfo.timeSeconds * 1000) + 
      (((100000 + rcinfo.timeUseconds) / 1000) - 100)
    );
  }
}

// Create and initialize server
async function startServer() {
  try {
    const server = new HEPServer({ debug: true });
    await server.initialize();  // Now we properly wait for initialization
    return server;
  } catch (error) {
    console.error('Failed to start HEP server:', error);
    process.exit(1);
  }
}

// Start server and export for module usage
const serverPromise = startServer();
export { HEPServer, hepjs, serverPromise };
