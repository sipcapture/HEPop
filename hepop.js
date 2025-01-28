import hepjs from 'hep-js';
import { getSIP } from 'parsip';
import { InfluxDBClient } from '@influxdata/influxdb3-client';
import { influxDBController } from './src/database/influxdb'

class BufferManager {
  constructor(flushInterval = 2000, bufferSize = 100) {
    this.buffers = new Map();
    this.flushInterval = flushInterval;
    this.bufferSize = bufferSize;
    influxDBController.initializeModule(
      process.env.INFLUX_HOST || 'http://localhost:8181',
      process.env.INFLUX_TOKEN || '',
      process.env.INFLUX_DATABASE || 'hep'
    )
    this.host = process.env.INFLUX_HOST || 'http://localhost:8181';
    const token = process.env.INFLUX_TOKEN || '';
    this.database = process.env.INFLUX_DATABASE || 'hep';

    console.log(`Initializing InfluxDB client: ${this.host}/${this.database}`);
    this.client = new InfluxDBClient({
      host: this.host, 
      token: token,
      database: this.database
    });
    
    this.retryAttempts = 3;
    this.startFlushInterval();
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

  getRetryInterval(retry) {
    const token = retry.toString();
    let result = parseInt(token);
    if (isNaN(result)) {
      const now = new Date();
      const exp = new Date(token);
      if (isNaN(exp.valueOf())) {
        throw new Error(`Failed to parse retry value: ${retry}`);
      }
      result = exp.getTime() - now.getTime();
    } else {
      result *= 1000;
    }
    return result;
  }

  async writeWithRetry(points, retryCount = this.retryAttempts) {
    /* TODO: call database module */
    try {
      console.log(`Attempting to write points to InfluxDB (${this.host}/${this.database})`);
      await this.client.write(points, this.database);
      return true;
    } catch (error) {
      if (error.headers?.['retry-after']) {
        console.log(`Warning: Write failed - ${error.message}`);
        if (retryCount > 0) {
          const interval = this.getRetryInterval(error.headers['retry-after']) + 1000;
          console.log(`Retrying in ${interval}ms`);
          await new Promise(resolve => setTimeout(resolve, interval));
          return this.writeWithRetry(points, retryCount - 1);
        }
      }
      console.error('Write error:', error);
      return false;
    }
  }

  async flush(type) {
    const buffer = this.buffers.get(type);
    if (!buffer?.length) return;
    
    try {
      /* TODO: call database module */
      const points = buffer.map(data => {
        const clean = str => str?.replace(/["\\\r\n]/g, '') || '';
        const rcinfo = data.protocol_header || {};
        
        const tags = [
          `type=${type}`
        ].join(',');

        const fields = [
          `ip_family=${rcinfo.protocolFamily || 0}i`,
          `protocol=${rcinfo.protocol || 0}i`,
          `proto_type=${rcinfo.proto_type || 0}i`,
          `capture_id=${rcinfo.captureId || 0}i`,
          `correlation_id="${clean(rcinfo.correlation_id)}"`,
          `capture_pass="${clean(rcinfo.capturePass)}"`,
          `src_ip="${clean(rcinfo.srcIp)}"`,
          `dst_ip="${clean(rcinfo.dstIp)}"`,
          `src_port=${rcinfo.srcPort || 0}i`,
          `dst_port=${rcinfo.dstPort || 0}i`,
          `time_sec=${rcinfo.timeSeconds || 0}i`,
          `time_usec=${rcinfo.timeUseconds || 0}i`,
          `payload="${clean(data.raw)}"`
        ].join(',');

        const timestamp = new Date(data.create_date).getTime() * 1000000;
        return `hep_${type},${tags} ${fields} ${timestamp}`;
      });

      console.log(`Writing ${points.length} points, sample:`, points[0]);
      await this.writeWithRetry(points.join('\n'));
      this.buffers.set(type, []);
    } catch (error) {
      console.error(`Buffer flush error:`, error);
    }
  }

  async close() {
    for (const type of this.buffers.keys()) {
      await this.flush(type);
    }
    await this.client.close();
  }
}

class HEPServer {
  constructor(config = {}) {
    this.debug = config.debug || false;
    this.buffer = new BufferManager();
    this.startServers();
  }

  startServers() {
    const port = parseInt(process.env.PORT) || 9069;
    const host = process.env.HOST || "0.0.0.0";

    // TCP Server
    Bun.listen({
      hostname: host,
      port: port,
      socket: {
        data: (socket, data) => this.handleData(data, socket),
        error: (socket, error) => console.error('TCP error:', error),
      }
    });

    // UDP Server
    Bun.udpSocket({
      hostname: host,
      port: port,
      udp: true,
      socket: {
        data: (socket, data) => this.handleData(data, socket),
        error: (socket, error) => console.error('UDP error:', error),
      }
    });

    console.log(`HEP Server listening on ${host}:${port} (TCP/UDP)`);

    // Handle graceful shutdown
    process.on('SIGTERM', this.shutdown.bind(this));
    process.on('SIGINT', this.shutdown.bind(this));
  }

  async shutdown() {
    console.log('Shutting down HEP server...');
    await this.buffer.close();
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

// Start the server
const server = new HEPServer({ debug: true });

export { HEPServer, hepjs };
