/**
 * InfluxDB Controller
 * A module to handle InfluxDB operations for HEPoP
 */

/* Imports */
import { InfluxDBClient } from '@influxdata/influxdb3-client'

export const influxDBController = {}

/**
 * Client Interface to database
 * @type {{write: (lines,database)=>{}, query: (sql_query)=>{}, close: ()=>{}}}
 */
influxDBController.client = {}

influxDBController.initializeModule = function (host, token, database_name) {
    console.log(`Initializing InfluxDB client: ${host}/${database_name}`)
    influxDBController.client = new InfluxDBClient({
        host: host,
        token: token,
        database: database_name
    })
}

influxDBController.writeData = async function (buffer) {
    /* Transform raw data to InfluxDB lines */
    try {
        const points = buffer.map(data => {
          const clean = str => str?.replace(/["\\\r\n]/g, '') || ''
          const rcinfo = data.protocol_header || {}
          
          const tags = [
            `type=${type}`
          ].join(',')
  
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
          ].join(',')
  
          const timestamp = new Date(data.create_date).getTime() * 1000000
          return `hep_${type},${tags} ${fields} ${timestamp}`
        });
  
        console.log(`Writing ${points.length} points, sample:`, points[0])
        await this.writeWithRetry(points.join('\n'))
        this.buffers.set(type, []);
      } catch (error) {
        console.error(`Buffer flush error:`, error)
      }

}



