import * as arrow from "apache-arrow";
import parquet from '@dsnp/parquetjs';
import { DuckDBInstance } from '@duckdb/node-api';
import hepjs from 'hep-js';
import { getSIP } from 'parsip';
import path from 'path';
import fs from 'fs';
import duckdb from '@duckdb/node-api';

class ParquetBufferManager {
  constructor(flushInterval = 10000, bufferSize = 1000) {
    this.buffers = new Map();
    this.flushInterval = flushInterval;
    this.bufferSize = bufferSize;
    this.baseDir = process.env.PARQUET_DIR || './data';
    this.writerId = process.env.WRITER_ID || require('os').hostname();
    
    // Define schema for HEP data
    this.schema = new parquet.ParquetSchema({
      timestamp: { type: 'TIMESTAMP_MILLIS' },
      rcinfo: { type: 'UTF8' },
      payload: { type: 'UTF8' }
    });

    // Add bloom filters for better query performance
    this.writerOptions = {
      bloomFilters: [
        {
          column: 'timestamp',
          numFilterBytes: 1024
        }
      ]
    };
  }

  async initialize() {
    // Ensure base directories exist
    await this.ensureDirectories();
    
    // Load or create global metadata
    await this.initializeMetadata();
    
    // Start flush interval after initialization
    this.startFlushInterval();
  }

  async initializeMetadata() {
    const metadataPath = path.join(this.baseDir, this.writerId, 'metadata.json');
    try {
      const data = await fs.promises.readFile(metadataPath, 'utf8');
      this.metadata = JSON.parse(data);
    } catch (error) {
      if (error.code === 'ENOENT') {
        this.metadata = {
          writer_id: this.writerId,
          next_db_id: 0,
          next_table_id: 0
        };
        await this.writeGlobalMetadata();
      } else {
        throw error;
      }
    }
  }

  async writeGlobalMetadata() {
    const metadataPath = path.join(this.baseDir, this.writerId, 'metadata.json');
    const tempPath = `${metadataPath}.tmp`;
    try {
      await fs.promises.writeFile(tempPath, JSON.stringify(this.metadata, null, 2));
      await fs.promises.rename(tempPath, metadataPath);
    } catch (error) {
      try {
        await fs.promises.unlink(tempPath);
      } catch (e) {
        // Ignore cleanup errors
      }
      throw error;
    }
  }

  async getTypeMetadata(type) {
    const metadataPath = this.getTypeMetadataPath(type);
    try {
      const data = await fs.promises.readFile(metadataPath, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      if (error.code === 'ENOENT') {
        // Initialize new type metadata
        const metadata = {
          type,
          parquet_size_bytes: 0,
          row_count: 0,
          min_time: null,
          max_time: null,
          wal_sequence: 0,
          files: [] // Array of file entries
        };
        await this.writeTypeMetadata(type, metadata);
        return metadata;
      }
      throw error;
    }
  }

  async getFilePath(type, timestamp) {
    const typeMetadata = await this.getTypeMetadata(type);
    const date = new Date(timestamp);
    const datePath = date.toISOString().split('T')[0];
    const hour = date.getHours().toString().padStart(2, '0');
    const minute = Math.floor(date.getMinutes() / 10) * 10;
    const minutePath = minute.toString().padStart(2, '0');
    
    return path.join(
      this.baseDir,
      this.writerId,
      'dbs',
      `hep-${this.metadata.next_db_id}`,
      `hep_${type}-${this.metadata.next_table_id}`,
      datePath,
      `${hour}-${minutePath}`,
      `${typeMetadata.wal_sequence.toString().padStart(10, '0')}.parquet`
    );
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

  async flush(type) {
    const buffer = this.buffers.get(type);
    if (!buffer?.length) return;
    
    try {
      const filePath = await this.getFilePath(type, buffer[0].create_date);
      await fs.promises.mkdir(path.dirname(filePath), { recursive: true });

      // Create writer
      const writer = await parquet.ParquetWriter.openFile(
        this.schema,
        filePath,
        this.writerOptions
      );

      // Write rows
      for (const data of buffer) {
        await writer.appendRow({
          timestamp: new Date(data.create_date),
          rcinfo: JSON.stringify(data.protocol_header),
          payload: data.raw || ''
        });
      }

      // Close writer to flush data
      await writer.close();

      // Get file stats
      const stats = await fs.promises.stat(filePath);
      
      // Update metadata
      await this.updateMetadata(
        type, 
        filePath, 
        stats.size, 
        buffer.length, 
        buffer.map(d => new Date(d.create_date))
      );
      
      // Clear buffer
      this.buffers.set(type, []);
      
      console.log(`Wrote ${buffer.length} records to ${filePath}`);
    } catch (error) {
      console.error(`Parquet flush error:`, error);
    }
  }

  getTypeMetadataPath(type) {
    return path.join(
      this.baseDir,
      this.writerId,
      'dbs',
      `hep-${this.metadata.next_db_id}`,
      `hep_${type}-${this.metadata.next_table_id}`,
      'metadata.json'
    );
  }

  async writeTypeMetadata(type, metadata) {
    const metadataPath = this.getTypeMetadataPath(type);
    await fs.promises.mkdir(path.dirname(metadataPath), { recursive: true });
    
    const tempPath = `${metadataPath}.tmp`;
    try {
      await fs.promises.writeFile(tempPath, JSON.stringify(metadata, null, 2));
      await fs.promises.rename(tempPath, metadataPath);
    } catch (error) {
      try {
        await fs.promises.unlink(tempPath);
      } catch (e) {
        // Ignore cleanup errors
      }
      throw error;
    }
  }

  async updateMetadata(type, filePath, sizeBytes, rowCount, timestamps) {
    const minTime = Math.min(...timestamps.map(t => t.getTime() * 1000000));
    const maxTime = Math.max(...timestamps.map(t => t.getTime() * 1000000));
    const chunkTime = Math.floor(minTime / 600000000000) * 600000000000;

    // Get current type metadata
    const typeMetadata = await this.getTypeMetadata(type);

    const fileInfo = {
      id: typeMetadata.files.length,
      path: filePath,
      size_bytes: sizeBytes,
      row_count: rowCount,
      chunk_time: chunkTime,
      min_time: minTime,
      max_time: maxTime,
      type: 'raw'
    };

    // Update type metadata
    typeMetadata.files.push(fileInfo);
    typeMetadata.parquet_size_bytes += sizeBytes;
    typeMetadata.row_count += rowCount;
    typeMetadata.min_time = typeMetadata.min_time ? 
      Math.min(typeMetadata.min_time, minTime) : minTime;
    typeMetadata.max_time = typeMetadata.max_time ?
      Math.max(typeMetadata.max_time, maxTime) : maxTime;
    typeMetadata.wal_sequence++;

    // Write updated metadata
    await this.writeTypeMetadata(type, typeMetadata);
  }

  async close() {
    for (const type of this.buffers.keys()) {
      await this.flush(type);
    }
  }

  async ensureDirectories() {
    const metadataDir = path.join(this.baseDir, this.writerId);
    await fs.promises.mkdir(metadataDir, { recursive: true });
    
    // Write initial metadata file if it doesn't exist
    const metadataPath = path.join(metadataDir, 'metadata.json');
    if (!fs.existsSync(metadataPath)) {
      await fs.promises.writeFile(
        metadataPath,
        JSON.stringify(this.metadata, null, 2)
      );
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
    this.compactionLock = new Map(); // Add lock for each type
    
    this.initialize();
  }

  async initialize() {
    try {
      // Initialize DuckDB
      this.db = await DuckDBInstance.create(':memory:');
      console.log(`Initialized DuckDB for parquet compaction`);
      
      // Start compaction jobs after initialization
      this.startCompactionJobs();
    } catch (error) {
      console.error('Failed to initialize CompactionManager:', error);
      throw error;
    }
  }

  startCompactionJobs() {
    // Run compaction checks every minute
    this.compactionInterval = setInterval(() => {
      this.checkAndCompact().catch(error => {
        console.error('Compaction job error:', error);
      });
    }, 60 * 1000);
  }

  async checkAndCompact() {
    // Get list of types from base directory
    const typeDirs = await this.getTypeDirectories();

    for (const type of typeDirs) {
      // Skip if compaction is already running for this type
      if (this.compactionLock.get(type)) {
        continue;
      }

      try {
        this.compactionLock.set(type, true);
        const metadata = await this.bufferManager.getTypeMetadata(type);
        await this.compactTimeRange(type, metadata.files, '10m', '1h');
        await this.compactTimeRange(type, metadata.files, '1h', '24h');
      } finally {
        this.compactionLock.set(type, false);
      }
    }
  }

  async getTypeDirectories() {
    const baseDir = path.join(
      this.bufferManager.baseDir,
      this.bufferManager.writerId,
      'dbs'
    );
    
    try {
      const entries = await fs.promises.readdir(baseDir, { withFileTypes: true });
      const types = new Set();
      
      for (const entry of entries) {
        if (entry.isDirectory() && entry.name.startsWith('hep-')) {
          const subEntries = await fs.promises.readdir(path.join(baseDir, entry.name));
          for (const subEntry of subEntries) {
            if (subEntry.startsWith('hep_')) {
              const type = subEntry.split('-')[1].split('_')[0];
              types.add(parseInt(type));
            }
          }
        }
      }
      
      return Array.from(types);
    } catch (error) {
      if (error.code === 'ENOENT') return [];
      throw error;
    }
  }

  async compactTimeRange(type, files, fromRange, toRange) {
    const now = Date.now() * 1000000;
    const interval = this.compactionIntervals[fromRange];
    
    // Group all files (including compacted) by hour
    const groups = new Map();
    
    files.forEach(file => {
      // Skip files that are too new
      if (now - file.max_time < interval) return;
      
      // Calculate target hour timestamp (floor to hour)
      const timestamp = new Date(file.chunk_time / 1000000);
      const hourTime = new Date(
        timestamp.getFullYear(),
        timestamp.getMonth(),
        timestamp.getDate(),
        timestamp.getHours()
      ).getTime();
      
      if (!groups.has(hourTime)) {
        groups.set(hourTime, {
          raw: [],
          compacted: []
        });
      }

      // Separate raw and compacted files
      if (path.basename(file.path).startsWith('c_')) {
        groups.get(hourTime).compacted.push(file);
      } else {
        groups.get(hourTime).raw.push(file);
      }
    });

    // Process each hour group
    for (const [hourTime, { raw, compacted }] of groups) {
      try {
        let filesToCompact = [];

        // If we have raw files to compact
        if (raw.length >= 2) {
          filesToCompact.push(...raw);
        }

        // If we have multiple compacted files, include them in the merge
        if (compacted.length > 0) {
          filesToCompact.push(...compacted);
        }

        // Only proceed if we have files to compact
        if (filesToCompact.length >= 2) {
          await this.compactFiles(type, filesToCompact, toRange);
        }
      } catch (error) {
        console.error(`Error compacting files for hour ${new Date(hourTime).toISOString()}:`, error);
      }
    }
  }

  async getCompactedFilePath(type, timestamp, typeMetadata) {
    const date = timestamp.toISOString().split('T')[0];
    const hour = timestamp.getHours().toString().padStart(2, '0');
    
    return path.join(
      this.bufferManager.baseDir,
      this.bufferManager.writerId,
      'dbs',
      `hep-${this.bufferManager.metadata.next_db_id}`,
      `hep_${type}-${this.bufferManager.metadata.next_table_id}`,
      date,
      `${hour}-00`,  // Always use top of the hour for compacted files
      `c_${typeMetadata.wal_sequence.toString().padStart(10, '0')}.parquet`
    );
  }

  async compactFiles(type, files, targetRange) {
    // Sort files by timestamp to ensure consistent ordering
    files.sort((a, b) => a.min_time - b.min_time);
    
    const timestamp = new Date(files[0].chunk_time / 1000000);
    const typeMetadata = await this.bufferManager.getTypeMetadata(type);
    const newPath = await this.getCompactedFilePath(type, timestamp, typeMetadata);
    
    try {
      // Check if all source files exist before starting
      for (const file of files) {
        await fs.promises.access(file.path);
      }

      // Create directory for new file
      await fs.promises.mkdir(path.dirname(newPath), { recursive: true });

      // Create new writer
      const writer = await parquet.ParquetWriter.openFile(
        this.bufferManager.schema,
        newPath,
        this.bufferManager.writerOptions
      );

      // Track total rows for logging
      let totalRows = 0;

      // Read and merge all files
      for (const file of files) {
        const reader = await parquet.ParquetReader.openFile(file.path);
        const cursor = reader.getCursor();
        
        let record = null;
        while (record = await cursor.next()) {
          await writer.appendRow(record);
          totalRows++;
        }
        
        await reader.close();
      }

      // Close writer and ensure file is written
      await writer.close();
      await fs.promises.access(newPath);

      // Get stats from new file
      const stats = await this.getFileStats(newPath);
      
      // Update metadata first
      await this.updateCompactionMetadata(type, files, {
        path: newPath,
        size_bytes: stats.size_bytes,
        row_count: stats.row_count,
        chunk_time: files[0].chunk_time,
        min_time: stats.min_time,
        max_time: stats.max_time,
        range: targetRange
      });

      // Write metadata
      await this.writeMetadata();

      // Only after metadata is written, clean up old files
      await this.cleanupCompactedFiles(files);

      const fileTypes = files.map(f => path.basename(f.path).startsWith('c_') ? 'compacted' : 'raw');
      const summary = `${fileTypes.filter(t => t === 'raw').length} raw, ${fileTypes.filter(t => t === 'compacted').length} compacted`;
      console.log(`Compacted ${files.length} files (${summary}) into ${newPath} (${totalRows} rows)`);
    } catch (error) {
      console.error(`Compaction error for type ${type}:`, error);
      try {
        await fs.promises.unlink(newPath);
      } catch (e) {
        // Ignore cleanup errors
      }
      throw error;
    }
  }

  async getFileStats(filePath) {
    const reader = await parquet.ParquetReader.openFile(filePath);
    const cursor = reader.getCursor();
    
    let rowCount = 0;
    let minTime = Infinity;
    let maxTime = -Infinity;
    
    let record = null;
    while (record = await cursor.next()) {
      rowCount++;
      const timestamp = record.timestamp.getTime();
      minTime = Math.min(minTime, timestamp);
      maxTime = Math.max(maxTime, timestamp);
    }
    
    await reader.close();
    
    const { size: sizeBytes } = await fs.promises.stat(filePath);

    return {
      size_bytes: sizeBytes,
      row_count: rowCount,
      min_time: minTime * 1000000, // Convert to nanoseconds
      max_time: maxTime * 1000000
    };
  }

  async updateCompactionMetadata(type, oldFiles, newFile) {
    const typeMetadata = await this.bufferManager.getTypeMetadata(type);

    // Remove old files from metadata
    oldFiles.forEach(oldFile => {
      const index = typeMetadata.files.findIndex(f => f.path === oldFile.path);
      if (index !== -1) {
        typeMetadata.parquet_size_bytes -= oldFile.size_bytes;
        typeMetadata.row_count -= oldFile.row_count;
        typeMetadata.files.splice(index, 1);
      }
    });

    // Add new compacted file
    const newFileEntry = {
      id: typeMetadata.files.length,
      ...newFile,
      type: path.basename(newFile.path).startsWith('c_') ? 'compacted' : 'raw'
    };
    typeMetadata.files.push(newFileEntry);

    // Update global metadata
    typeMetadata.parquet_size_bytes += newFile.size_bytes;
    typeMetadata.row_count += newFile.row_count;
    typeMetadata.min_time = typeMetadata.min_time ? 
      Math.min(typeMetadata.min_time, newFile.min_time) : newFile.min_time;
    typeMetadata.max_time = typeMetadata.max_time ?
      Math.max(typeMetadata.max_time, newFile.max_time) : newFile.max_time;

    // Write updated metadata
    await this.bufferManager.writeTypeMetadata(type, typeMetadata);
  }

  async writeMetadata() {
    const metadataDir = path.join(
      this.bufferManager.baseDir, 
      this.bufferManager.writerId
    );
    
    await fs.promises.mkdir(metadataDir, { recursive: true });
    
    const metadataPath = path.join(metadataDir, 'metadata.json');
    const tempPath = `${metadataPath}.tmp`;
    
    try {
      // Write metadata to temp file
      await fs.promises.writeFile(
        tempPath, 
        JSON.stringify(this.bufferManager.metadata, null, 2)
      );
      
      // Ensure temp file exists before rename
      await fs.promises.access(tempPath);
      
      // Atomic rename
      await fs.promises.rename(tempPath, metadataPath);
      
      // Verify metadata file exists
      await fs.promises.access(metadataPath);
    } catch (error) {
      // Cleanup temp file if it exists
      try {
        await fs.promises.unlink(tempPath);
      } catch (e) {
        // Ignore cleanup errors
      }
      throw error;
    }
  }

  async cleanupCompactedFiles(files) {
    // Delete files first
    for (const file of files) {
      try {
        await fs.promises.access(file.path); // Check if file exists
        await fs.promises.unlink(file.path);
      } catch (error) {
        if (error.code !== 'ENOENT') {
          console.error(`Error deleting file ${file.path}:`, error);
        }
      }
    }

    // Collect directories to check
    const dirsToCheck = new Set();
    files.forEach(file => {
      let dirPath = path.dirname(file.path);
      while (dirPath.startsWith(this.bufferManager.baseDir)) {
        dirsToCheck.add(dirPath);
        dirPath = path.dirname(dirPath);
      }
    });

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
        if (error.code !== 'ENOENT') {
          console.error(`Error cleaning up directory ${dir}:`, error);
        }
      }
    }
  }

  async close() {
    if (this.compactionInterval) {
      clearInterval(this.compactionInterval);
    }
    if (this.db) {
      await this.db.close();
    }
  }
}

class HEPServer {
  constructor(config = {}) {
    this.debug = config.debug || false;
  }

  async initialize() {
    try {
      this.buffer = new ParquetBufferManager();
      await this.buffer.initialize();
      
      this.compaction = new CompactionManager(this.buffer);
      await this.compaction.initialize();
      
      await this.startServers();
    } catch (error) {
      console.error('Failed to initialize HEPServer:', error);
      throw error;
    }
  }

  async startServers() {
    const port = parseInt(process.env.PORT) || 9069;
    const host = process.env.HOST || "0.0.0.0";
    const retryAttempts = 3;
    const retryDelay = 1000; // 1 second

    for (let attempt = 1; attempt <= retryAttempts; attempt++) {
      try {
        // Try to create TCP Server
        const tcpServer = Bun.listen({
      hostname: host,
      port: port,
      socket: {
        data: (socket, data) => this.handleData(data, socket),
        error: (socket, error) => console.error('TCP error:', error),
      }
    });

        // If TCP succeeds, create UDP Server
        const udpServer = Bun.udpSocket({
      hostname: host,
      port: port,
      udp: true,
      socket: {
        data: (socket, data) => this.handleData(data, socket),
        error: (socket, error) => console.error('UDP error:', error),
      }
    });

    console.log(`HEP Server listening on ${host}:${port} (TCP/UDP)`);
        
        // Store server references
        this.tcpServer = tcpServer;
        this.udpServer = udpServer;

    // Handle graceful shutdown
    process.on('SIGTERM', this.shutdown.bind(this));
    process.on('SIGINT', this.shutdown.bind(this));
        
        return; // Success, exit the retry loop
      } catch (error) {
        console.error(`Attempt ${attempt}/${retryAttempts} failed:`, error);
        
        if (attempt === retryAttempts) {
          throw new Error(`Failed to start server after ${retryAttempts} attempts: ${error.message}`);
        }
        
        // Wait before retrying
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      }
    }
  }

  async shutdown() {
    console.log('Shutting down HEP server...');
    
    // Close servers if they exist
    if (this.tcpServer) {
      this.tcpServer.close();
    }
    if (this.udpServer) {
      this.udpServer.close();
    }
    
    // Close other resources
    await this.buffer.close();
    await this.compaction.close();
    
    process.exit(0);
  }

  handleData(data, socket) {
    try {
      const processed = this.processHep(data, socket);
      const type = processed.type;
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
