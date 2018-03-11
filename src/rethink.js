/*
 * ReThinkDB Connector
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
var db,table,r;

exports.connect = function(){
  var rtconfig = require('./config').getConfig();
  if(!rtconfig.db.rethink) {
    log('%stop:red Failed Initializing RethinkDB [%s:blue]');
    process.exit();
    //return;
  }
  log('%start:cyan Initializing RethinkDB [%s:blue]',stringify(rtconfig.db.rethink));
  r = require('rethinkdbdash')(rtconfig.db.rethink);
  r.getPoolMaster().on('healthy', function(healthy) {
    if (healthy === true) { log('%start:green RethinkDB healthy'); }
    else { log('%stop:red RethinkDB unhealthy');}
  });

  if(rtconfig.dbName) r.dbCreate(rtconfig.dbName).run().catch(function(err) {});
  if(rtconfig.tableName) r.db(rtconfig.dbName).tableCreate(rtconfig.tableName).run().catch(function(err) {});

  exports.r = r;
  return r;
}


exports.createDb = function(r,dbName,tableName){
  if(dbName) try { r.dbCreate(dbName).run(); db = dbName; } catch(e) { log('%s:red',e) }
  if(tableName) try { r.db(dbName).tableCreate(tableName).run(); table = tableName; } catch(e) { log('%s:red',e) }
}
exports.createTable = function(tableName){
  if(tableName) { r.db(db).tableCreate(tableName).run(); table = tableName; }
}

/* SELECT FILTER
r.table('hep').filter( r.row("payload").match(`ej338i@127.0.0.1_b2b-1`) )
*/
