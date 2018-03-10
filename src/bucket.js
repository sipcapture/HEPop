/*
 * Bulk Bucket Emitters
 * bucket.push(object);
 */

var config = require('./config').getConfig();
const log = require('./logger');
const bucket_emitter = require('./bulk-emitter')
const stringify = require('safe-stable-stringify');
var r;

try {
  r = require('./rethink').connect();
  log('%start:green Initialize RethinkDB driver' );
} catch(e){ log('%stop:red Failed to Initialize RethinkDB driver',e); return; }

log('%start:green Initializing Bulk bucket...');
const bucket = bucket_emitter.create(config.queue);

bucket.on('data', function(data) {
  // Bulk ready to emit!
  log('%data:cyan BULK Out [%s:blue]', stringify(data) );
  log('%data:cyan CONFIG [%s:yellow]', stringify(config) );
  // TODO: add chain emitter to multiple backends or pipelines
  r.db(config.dbName).table(config.tableName).insert(data).run();
}).on('error', function(err) {
  log('%error:red %s', err.toString() )
});

process.on('beforeExit', function() {
  bucket.close(function(leftData) {
    log('%data:red BULK Leftover [%s:blue]', stringify(leftData) );
    r.db(config.dbName).table(config.tableName).insert(leftData).run();
  });
});

exports.bucket = bucket;
exports.r = r;
