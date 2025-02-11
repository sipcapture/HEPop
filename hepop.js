import parquet from '@dsnp/parquetjs';
import { DuckDBInstance } from '@duckdb/node-api';
import hepjs from 'hep-js';
import { getSIP } from 'parsip';
import path from 'path';
import fs from 'fs';
import duckdb from '@duckdb/node-api';
import QueryClient from './query.js';
import { parse } from './lineproto.js';

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

    // Add schema for Line Protocol data
    this.lpSchema = new parquet.ParquetSchema({
      timestamp: { type: 'TIMESTAMP_MILLIS' },
      tags: { type: 'UTF8' },  // JSON string of tags
      // Dynamic fields will be added based on data
    });
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
    
    // Handle nanosecond timestamps
    let date;
    if (typeof timestamp === 'number') {
      // Keep nanosecond precision by using floor division for date parts
      const ms = Math.floor(timestamp / 1000000); // Get milliseconds
      date = new Date(ms);
    } else if (typeof timestamp === 'string') {
      // Parse string timestamp
      date = new Date(timestamp);
    } else if (timestamp instanceof Date) {
      date = timestamp;
    } else {
      throw new Error('Invalid timestamp format');
    }

    if (isNaN(date.getTime())) {
      throw new Error(`Invalid date from timestamp: ${timestamp}`);
    }

    // Use date for directory structure only
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
      this.buffers.set(type, {
        rows: [],
        schema: this.schema,  // Use HEP schema
        isLineProtocol: false // Mark as HEP data
      });
    }

    const buffer = this.buffers.get(type);
    buffer.rows.push(data);
    
    if (buffer.rows.length >= this.bufferSize) {
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
    if (!buffer?.rows.length) return;
    
    try {
      const filePath = await this.getFilePath(type, buffer.isLineProtocol ? 
        buffer.rows[0].timestamp : buffer.rows[0].create_date);
      await fs.promises.mkdir(path.dirname(filePath), { recursive: true });

      // Create writer with appropriate schema
      const writer = await parquet.ParquetWriter.openFile(
        buffer.schema,
        filePath,
        this.writerOptions
      );

      // Write rows based on type
      for (const data of buffer.rows) {
        if (buffer.isLineProtocol) {
          await writer.appendRow(data);
        } else {
          await writer.appendRow({
            timestamp: new Date(data.create_date),
            rcinfo: JSON.stringify(data.protocol_header),
            payload: data.raw || ''
          });
        }
      }

      await writer.close();

      // Get file stats
      const stats = await fs.promises.stat(filePath);
      
      // Update metadata
      await this.updateMetadata(
        type, 
        filePath, 
        stats.size, 
        buffer.rows.length, 
        buffer.rows.map(d => buffer.isLineProtocol ? 
          d.timestamp : new Date(d.create_date))
      );
      
      // Clear buffer
      this.buffers.set(type, {
        rows: [],
        schema: buffer.schema,
        isLineProtocol: buffer.isLineProtocol
      });
      
      console.log(`Wrote ${buffer.rows.length} records to ${filePath}`);
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
      const initialMetadata = {
        writer_id: this.writerId,
        next_db_id: 0,
        next_table_id: 0
      };
      
      await fs.promises.writeFile(
        metadataPath,
        JSON.stringify(initialMetadata, null, 2)
      );
    }
  }

  async addLineProtocol(data) {
    const measurement = data.measurement;
    if (!this.buffers.has(measurement)) {
      // Create new schema for this measurement including its fields
      const schema = new parquet.ParquetSchema({
        timestamp: { type: 'TIMESTAMP_MILLIS' },
        tags: { type: 'UTF8' },
        ...Object.entries(data.fields).reduce((acc, [key, value]) => {
          acc[key] = { 
            type: typeof value === 'number' ? 'DOUBLE' : 
                  typeof value === 'boolean' ? 'BOOLEAN' : 'UTF8'
          };
          return acc;
        }, {})
      });

      this.buffers.set(measurement, {
        rows: [],
        schema
      });
    }

    const buffer = this.buffers.get(measurement);
    buffer.rows.push({
      timestamp: new Date(data.timestamp),
      tags: JSON.stringify(data.tags),
      ...data.fields
    });

    if (buffer.rows.length >= this.bufferSize) {
      await this.flushLineProtocol(measurement);
    }
  }

  async flushLineProtocol(measurement) {
    const buffer = this.buffers.get(measurement);
    if (!buffer?.rows.length) return;

    try {
      const filePath = await this.getFilePath(measurement, buffer.rows[0].timestamp);
      await fs.promises.mkdir(path.dirname(filePath), { recursive: true });

      // Create writer with measurement's schema
      const writer = await parquet.ParquetWriter.openFile(
        buffer.schema,
        filePath,
        this.writerOptions
      );

      // Write rows
      for (const row of buffer.rows) {
        await writer.appendRow(row);
      }

      await writer.close();

      // Update metadata similar to HEP data
      const stats = await fs.promises.stat(filePath);
      await this.updateMetadata(
        measurement,
        filePath,
        stats.size,
        buffer.rows.length,
        buffer.rows.map(r => r.timestamp)
      );

      buffer.rows = [];
    } catch (error) {
      console.error(`Line Protocol flush error:`, error);
    }
  }

  async addLineProtocolBulk(measurement, rows) {
    // Use measurement directly as type (like HEP types)
    const type = measurement;
    
    if (!this.buffers.has(type)) {
      // Create new schema for this measurement including its fields
      const schema = new parquet.ParquetSchema({
        timestamp: { type: 'TIMESTAMP_MILLIS' },
        tags: { type: 'UTF8' },
        ...Object.entries(rows[0]).reduce((acc, [key, value]) => {
          if (key !== 'timestamp' && key !== 'tags') {
            acc[key] = { 
              type: typeof value === 'number' ? 'DOUBLE' : 
                    typeof value === 'boolean' ? 'BOOLEAN' : 'UTF8'
            };
          }
          return acc;
        }, {})
      });

      this.buffers.set(type, {
        rows: [],
        schema,
        isLineProtocol: true  // Mark as Line Protocol data
      });
    }

    const buffer = this.buffers.get(type);
    buffer.rows.push(...rows);

    if (buffer.rows.length >= this.bufferSize) {
      await this.flush(type);  // Use the same flush method as HEP
    }
  }
}

class CompactionManager {
  constructor(bufferManager, debug = false) {
    this.bufferManager = bufferManager;
    this.compactionIntervals = {
      '10m': 10 * 60 * 1000,
      '1h': 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000
    };
    this.compactionLock = new Map();
    this.debug = debug;
  }

  async initialize() {
    try {
      // Initialize DuckDB
      this.db = await DuckDBInstance.create(':memory:');
      console.log(`Initialized DuckDB for parquet compaction`);
      
      // Run initial compaction check
      await this.checkAndCompact();
      
      // Start compaction jobs after initialization
      this.startCompactionJobs();
    } catch (error) {
      console.error('Failed to initialize CompactionManager:', error);
      throw error;
    }
  }

  startCompactionJobs() {
    // Run compaction checks every minute
    this.compactionInterval = setInterval(async () => {
      try {
        console.log('Running scheduled compaction check...');
        await this.checkAndCompact();
      } catch (error) {
        console.error('Compaction job error:', error);
      }
    }, 60 * 1000);
  }

  async verifyAndCleanMetadata(type, metadata) {
    const existingFiles = [];
    const removedFiles = [];
    
    for (const file of metadata.files) {
      try {
        await fs.promises.access(file.path);
        existingFiles.push(file);
      } catch (error) {
        if (error.code === 'ENOENT') {
          console.log(`Removing missing file from metadata: ${file.path}`);
          removedFiles.push(file);
        } else {
          throw error;
        }
      }
    }

    if (removedFiles.length > 0) {
      // Update metadata to remove missing files
      metadata.files = existingFiles;
      
      // Recalculate totals
      metadata.parquet_size_bytes = existingFiles.reduce((sum, f) => sum + f.size_bytes, 0);
      metadata.row_count = existingFiles.reduce((sum, f) => sum + f.row_count, 0);
      
      if (existingFiles.length > 0) {
        metadata.min_time = Math.min(...existingFiles.map(f => f.min_time));
        metadata.max_time = Math.max(...existingFiles.map(f => f.max_time));
    } else {
        metadata.min_time = null;
        metadata.max_time = null;
      }

      // Write updated metadata
      await this.bufferManager.writeTypeMetadata(type, metadata);
      console.log(`Cleaned up ${removedFiles.length} missing files from metadata`);
    }

    return metadata;
  }

  async checkAndCompact() {
    const typeDirs = await this.getTypeDirectories();
    if (this.debug || typeDirs.length > 0) {
      console.log('Found types for compaction:', typeDirs);
    }

    for (const type of typeDirs) {
      if (this.compactionLock.get(type)) {
        console.log(`Skipping compaction for type ${type} - already running`);
        continue;
      }

      try {
        this.compactionLock.set(type, true);
        let metadata = await this.bufferManager.getTypeMetadata(type);
        
        if (!metadata.files || !metadata.files.length) {
          if (this.debug) console.log(`No files found in metadata for type ${type}`);
          continue;
        }

        metadata = await this.verifyAndCleanMetadata(type, metadata);
        
        if (metadata.files.length > 0) {
          console.log(`Type ${type} has ${metadata.files.length} files to consider for compaction`);
          await this.compactTimeRange(type, metadata.files, '10m', '1h');
          await this.compactTimeRange(type, metadata.files, '1h', '24h');
        }
      } catch (error) {
        console.error(`Error during compaction for type ${type}:`, error);
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
              // Extract type from hep_TYPE-ID format
              const match = subEntry.match(/hep_([^-]+)-/);
              if (match) {
                types.add(match[1]); // Store the full type/measurement name
              }
            }
          }
        }
      }
      
      console.log('Found directories:', Array.from(types).map(type => ({
        type,
        path: path.join(baseDir, `hep-${this.bufferManager.metadata.next_db_id}`, `hep_${type}-${this.bufferManager.metadata.next_table_id}`)
      })));
      
      return Array.from(types);
    } catch (error) {
      if (error.code === 'ENOENT') {
        console.log('No dbs directory found at:', baseDir);
        return [];
      }
      console.error('Error reading type directories:', error);
      throw error;
    }
  }

  async compactTimeRange(type, files, fromRange, toRange) {
    const now = Date.now() * 1000000;
    const interval = this.compactionIntervals[fromRange];
    
    // Group all files (including compacted) by hour
    const groups = new Map();
    
    console.log(`Checking ${files.length} files for ${fromRange} compaction...`);
    
    files.forEach(file => {
      const isCompacted = path.basename(file.path).startsWith('c_');
      const fileAge = now - file.max_time;
      
      // Different rules for raw vs compacted files
      if (isCompacted) {
        // Compacted files are never too new, they're consolidation targets
        console.log(`Found compacted file ${path.basename(file.path)} as potential merge target`);
      } else {
        // Only raw files have age restrictions
        if (fileAge <= interval) {
          console.log(`File ${path.basename(file.path)} is too new for compaction (age: ${fileAge / 1000000}s)`);
          return;
        }
        if (fileAge > (interval * 2)) {
          console.log(`Found orphaned raw file ${path.basename(file.path)} (age: ${fileAge / 1000000}s)`);
        }
      }
      
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
      if (isCompacted) {
        groups.get(hourTime).compacted.push(file);
      } else {
        groups.get(hourTime).raw.push(file);
      }
    });

    // Log grouping results
    for (const [hourTime, { raw, compacted }] of groups) {
      console.log(`Hour ${new Date(hourTime).toISOString()}: ${raw.length} raw files, ${compacted.length} compacted files`);
    }

    // Process each hour group
    for (const [hourTime, { raw, compacted }] of groups) {
      try {
        let filesToCompact = [];
        let targetCompacted = null;

        // Verify files exist before including them
        for (const file of raw) {
          try {
            await fs.promises.access(file.path);
            filesToCompact.push(file);
          } catch (error) {
            if (error.code !== 'ENOENT') {
              throw error;
            }
          }
        }

        // Find the most recent compacted file as merge target
        for (const file of compacted.sort((a, b) => b.max_time - a.max_time)) {
          try {
            await fs.promises.access(file.path);
            targetCompacted = file;
            break;
          } catch (error) {
            if (error.code !== 'ENOENT') {
              throw error;
            }
          }
        }

        // Decide whether to compact based on files available
        if (filesToCompact.length >= 2 || (filesToCompact.length > 0 && targetCompacted)) {
          if (targetCompacted) {
            filesToCompact.push(targetCompacted);
            console.log(`Merging ${filesToCompact.length - 1} raw files into existing compacted file ${path.basename(targetCompacted.path)}`);
          } else {
            console.log(`Creating new compacted file from ${filesToCompact.length} raw files`);
          }
          await this.compactFiles(type, filesToCompact, toRange);
        } else {
          console.log(`Not enough files to compact for hour ${new Date(hourTime).toISOString()}`);
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

      // Determine if this is Line Protocol data by checking first file's schema
      const firstReader = await parquet.ParquetReader.openFile(files[0].path);
      const schema = firstReader.getSchema();
      await firstReader.close();

      // Create new writer with appropriate schema
      const writer = await parquet.ParquetWriter.openFile(
        schema,  // Use the schema from the source files
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
        await fs.promises.access(file.path);
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

    // Get current hour for comparison
    const now = new Date();
    const currentDate = now.toISOString().split('T')[0];
    const currentHour = now.getHours().toString().padStart(2, '0');
    const currentHourPath = `${currentDate}/${currentHour}`;

    // Clean up empty directories from deepest to shallowest
    const sortedDirs = Array.from(dirsToCheck)
      .sort((a, b) => b.split(path.sep).length - a.split(path.sep).length);

    for (const dir of sortedDirs) {
      try {
        // Skip if this is the current hour's directory
        if (dir.includes(currentHourPath)) {
          console.log(`Skipping cleanup of current hour directory: ${dir}`);
          continue;
        }

        const files = await fs.promises.readdir(dir);
        
        // For hour directories, also check if it's from a past hour
        if (dir.match(/\d{4}-\d{2}-\d{2}\/\d{2}-\d{2}/)) {
          const dirDate = path.basename(path.dirname(dir));
          const dirHour = dir.split('/').pop().split('-')[0];
          const dirPath = `${dirDate}/${dirHour}`;
          
          if (dirPath >= currentHourPath) {
            console.log(`Skipping cleanup of current or future hour directory: ${dir}`);
            continue;
          }
        }

        if (files.length === 0) {
          await fs.promises.rmdir(dir);
          console.log(`Cleaned up empty directory: ${dir}`);
        } else {
          console.log(`Directory not empty, skipping cleanup: ${dir} (${files.length} files)`);
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
  }
}

class HEPServer {
  constructor(config = {}) {
    this.debug = config.debug || false;
    this.queryClient = null;
    this.buffer = null;
    this.compaction = null;
  }

  async initialize() {
    try {
      // Initialize buffer manager
      this.buffer = new ParquetBufferManager();
      await this.buffer.initialize();

      // Initialize compaction manager with debug flag
      this.compaction = new CompactionManager(this.buffer, true); // Always show compaction logs
      await this.compaction.initialize();

      // Initialize query client with buffer manager
      this.queryClient = new QueryClient(this.buffer.baseDir, this.buffer);
      await this.queryClient.initialize();

      // Start servers
      await this.startServers();
    } catch (error) {
      console.error('Failed to initialize HEP server:', error);
      throw error;
    }
  }

  async startServers() {
    const port = parseInt(process.env.PORT) || 9069;
    const httpPort = parseInt(process.env.HTTP_PORT) || (port + 1);
    const host = process.env.HOST || "0.0.0.0";
    const retryAttempts = 3;
    const retryDelay = 1000;

    for (let attempt = 1; attempt <= retryAttempts; attempt++) {
      try {
        // Create TCP Server
        const tcpServer = Bun.listen({
      hostname: host,
      port: port,
      socket: {
        data: (socket, data) => this.handleData(data, socket),
        error: (socket, error) => console.error('TCP error:', error),
      }
    });

        // Create UDP Server
        const udpServer = Bun.udpSocket({
      hostname: host,
      port: port,
      udp: true,
      socket: {
        data: (socket, data) => this.handleData(data, socket),
        error: (socket, error) => console.error('UDP error:', error),
      }
    });

        // Create HTTP Server for queries and writes
        const self = this;
        const httpServer = Bun.serve({
          hostname: host,
          port: httpPort,
          async fetch(req) {
            const url = new URL(req.url);
            
            if (url.pathname === '/') {
              try {
                const html = await Bun.file('./index.html').text();
                return new Response(html, {
                  headers: { 'Content-Type': 'text/html' }
                });
              } catch (error) {
                console.error('Error serving index.html:', error);
                return new Response('Error loading interface', { status: 500 });
              }
            } else if (url.pathname === '/query') {
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
                
                // Handle BigInt serialization
                const safeResult = JSON.parse(JSON.stringify(result, (key, value) =>
                  typeof value === 'bigint' ? value.toString() : value
                ));

                return new Response(JSON.stringify(safeResult), {
                  headers: { 'Content-Type': 'application/json' }
                });
              } catch (error) {
                console.error('Query error:', error);
                return new Response(JSON.stringify({ error: error.message }), {
                  status: 500,
                  headers: { 'Content-Type': 'application/json' }
                });
              }
            } else if (url.pathname === '/write' && req.method === 'POST') {
              try {
                const body = await req.text();
                const lines = body.split('\n').filter(line => line.trim());
                                
                const config = {
                  addTimestamp: true,
                  typeMappings: [],
                  defaultTypeMapping: 'float'
                };

                // Process lines in bulk
                const bulkData = new Map(); // measurement -> rows
                
                for (const line of lines) {
                  const parsed = parse(line, config);
                  const measurement = parsed.measurement;
                  
                  if (!bulkData.has(measurement)) {
                    bulkData.set(measurement, []);
                  }
                  
                  bulkData.get(measurement).push({
                    timestamp: new Date(parsed.timestamp),
                    tags: JSON.stringify(parsed.tags),
                    ...parsed.fields
                  });
                }

                // Bulk insert by measurement
                for (const [measurement, rows] of bulkData) {
                  // console.log(`Writing ${rows.length} rows to measurement ${measurement}`);
                  await self.buffer.addLineProtocolBulk(measurement, rows);
                }

                return new Response(null, { status: 201 });
              } catch (error) {
                console.error('Write error:', error);
                return new Response(error.message, { status: 400 });
              }

            }

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
        
        return;
      } catch (error) {
        console.error(`Attempt ${attempt}/${retryAttempts} failed:`, error);
        
        if (attempt === retryAttempts) {
          throw new Error(`Failed to start server after ${retryAttempts} attempts: ${error.message}`);
        }
        
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      }
    }
  }

  async shutdown() {
    console.log('Shutting down HEP server...');
    
    // Stop compaction first
    if (this.compaction) {
      await this.compaction.close();
    }

    // Stop TCP server
    if (this.tcpServer) {
      try {
        this.tcpServer.stop(true);
        this.tcpServer.unref();
      } catch (error) {
        console.error('Error stopping TCP server:', error);
      }
    }

    // Stop UDP server
    if (this.udpServer) {
      try {
        // UDP sockets use close() not stop()
        if (this.udpSever?.close) this.udpServer.close();
      } catch (error) {
        console.error('Error stopping UDP server:', error);
      }
    }

    // Stop HTTP server
    if (this.httpServer) {
      try {
        this.httpServer.stop(true);
        this.httpServer.unref();
      } catch (error) {
        console.error('Error stopping HTTP server:', error);
      }
    }
    
    // Flush any remaining data
    try {
    await this.buffer.close();
    } catch (error) {
      console.error('Error flushing buffers:', error);
    }
    
    console.log('Server shutdown complete');
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
