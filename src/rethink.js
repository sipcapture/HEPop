/*
 * ReThinkDB Connector
 */

var log = require('./logger');
var db,table;

exports.connect = function(config){
  if (!config) config = { servers: [ { host: '127.0.0.1', port: 28015 }] };
  var r = require('rethinkdbdash')(config);

  r.getPoolMaster().on('healthy', function(healthy) {
    if (healthy) { log('%start:green RethinkDB healthy'); }
    else { log('%stop:red RethinkDB unhealthy'); }
  });
  exports.r = r;
  return r;
}

exports.createDb = function(dbName,tableName){
  if(dbName) { r.dbCreate(dbName).run(); db = dbname; }
  if(tableName) { r.db(dbName).tableCreate(tableName).run(); table = tableName; }
}
exports.createTable = function(tableName){
  if(tableName) { r.db(db).tableCreate(tableName).run(); table = tableName; }
}

/* SELECT FILTER
r.table('hep').filter( r.row("payload").match(`ej338i@127.0.0.1_b2b-1`) )
*/
