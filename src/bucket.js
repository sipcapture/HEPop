/*
 * Bulk Bucket Emitters
 * bucket.push(object);
 */

var config = require('./config').getConfig();
const log = require('./logger');
const bucket_emitter = require('./bulk-emitter')
const stringify = require('safe-stable-stringify');
var r = false;
var pgp = false;

var r_bucket = false;
var p_bucket = false;

// RethinkDB
if (config.db.rethink){
 try {
  r = require('./rethink').connect();
  log('%start:green Initialize RethinkDB driver' );

  log('%start:green Initializing Bulk bucket...');
  r_bucket = bucket_emitter.create(config.queue);
  r_bucket.on('data', function(data) {
    // Bulk ready to emit!
    if (config.debug) log('%data:cyan RethinkDB BULK Out [%s:blue]', stringify(data) );
    // TODO: add chain emitter to multiple backends or pipelines
    r.db(config.dbName).table(config.tableName).insert(data).run(durability="soft", noreply=true);
  }).on('error', function(err) {
    log('%error:red %s', err.toString() )
  });

 } catch(e){ log('%stop:red Failed to Initialize RethinkDB driver/queue',e); return; }
}

exports.bucket = r_bucket;
exports.r = r;

// PGSql
if (config.db.pgsql){
 try {
  pgp = require('./pgsql');
  pgp.setTemplate(config.db.pgsql.schema);
  p_bucket = bucket_emitter.create(config.queue);
  p_bucket.on('data', function(data) {
    // Bulk ready to emit!
    if (config.debug) log('%data:cyan PGSQL BULK Out [%s:blue]', stringify(data) );
    // TODO: add chain emitter to multiple backends or pipelines
    pgp.insert(data);
  }).on('error', function(err) {
    log('%error:red %s', err.toString() )
  });

 } catch(e){ log('%stop:red Failed to Initialize PGSql driver/queue',e); return; }
}

exports.pgp_bucket = p_bucket;
exports.pgp = pgp;



process.on('beforeExit', function() {
  bucket.close(function(leftData) {
    if (config.debug) log('%data:red BULK Leftover [%s:blue]', stringify(leftData) );
    if (r) r.db(config.dbName).table(config.tableName).insert(leftData).run(durability="soft", noreply=True);
  });
});

