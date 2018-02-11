/*
 * ReThinkDB Connector
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
var db,table,r;

exports.connect = function(){
  var rtconfig = require('./config').getConfig();
  if(!rtconfig.rethink) {
    log('%stop:red Failed Initializing RethinkDB [%s:blue]');
    return;
  }
  log('%start:cyan Initializing RethinkDB [%s:blue]',stringify(rtconfig.rethink));
  r = require('rethinkdbdash')(rtconfig.rethink);
  r.getPoolMaster().on('healthy', function(healthy) {
    if (healthy === true) { log('%start:green RethinkDB healthy'); }
    else { log('%stop:red RethinkDB unhealthy');}
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
