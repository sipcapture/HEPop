/*
 * HEP Op
 * (c) 2018 QXIP BV
 * See LICENSE for details
 */

//'use strict';

// https://www.npmjs.com/package/@mysql/xdevapi
// const mysqlx = require('@mysql/xdevapi');

const program = require('commander');
const setConfig = require('./src/config').setConfig;
const getConfig = require('./src/config').getConfig;

const pkg = require('./package.json');
const servers = require('./src/servers');
const select = servers.select;

program
  .version(pkg.version)
  .option('-p, --port <number>', 'port to listen on', Number, 9060)
  .option('-a, --address <address>', 'network address to listen on', String, '127.0.0.1')
  .option('-d, --dbName <address>', 'database name', String, 'hepic')
  .option('-t, --tableName <address>', 'database table name', String, 'hep')
  .option('-c, --configfile <configfile>', 'configuration file', String)
  .option('-s, --socket <socket>', 'socket service (udp,tcp,http,sipfix)', String, 'udp')
  .parse(process.argv)

if (!program.socket||!program.configfile) {
  program.help()
} else {
  setConfig(program);
  select(getConfig());
}

