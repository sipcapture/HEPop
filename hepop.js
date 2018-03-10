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

const http = servers.http;
const tcp = servers.tcp;
const udp = servers.udp;
const sipfix = servers.sipfix;

program
  .version(pkg.version)
  .option('-p, --port <number>', 'port to listen on', Number)
  .option('-a, --address <address>', 'network address to listen on', String)
  .option('-d, --database <address>', 'database name', String)
  .option('-t, --table <address>', 'database table name', String)
  .option('-c, --configfile <configfile>', 'configuration file', String)

program
  .command('http')
  .description('start HTTP HEP server')
  .action(() => setConfig(program))
  .action(() => http(getConfig()))

program
  .command('tcp')
  .description('start TCP HEP server')
  .action(() => setConfig(program))
  .action(() => tcp(getConfig()))

program
  .command('udp')
  .description('start UDP HEP server')
  .action(() => setConfig(program))
  .action(() => udp(getConfig()))

program
  .command('sipfix')
  .description('start UDP SIPFIX server')
  .action(() => setConfig(program))
  .action(() => sipfix(getConfig()))

program
  .command('*', false, { noHelp: true })
  .action(() => program.help())

program
  .parse(process.argv)

if (!process.argv.slice(2).length) {
  program.help()
}

