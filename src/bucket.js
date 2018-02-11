/*
 * Bulk Bucket Emitters
 * bucket.push(object);
 */

var getConfig = require('./config').getConfig;
const log = require('./logger');
const bucket_emitter = require('./bulk-emitter')
const stringify = require('safe-stable-stringify');
var r;

try {
  r = require('./rethink').connect();
  log('%start:green Initialize RethinkDB driver' );
} catch(e){ log('%stop:red Failed to Initialize RethinkDB driver',e); return; }

log('%start:green Initializing Bulk bucket...');
const bucket = bucket_emitter.create({
    timeout: 2000, //if there's no data input until timeout, emit data forcefully.
    maxSize: 1000, //data emitted count
    useInterval: true //if this value is true, data event is emitted even If new data is pushed.
});

bucket.on('data', function(data) {
  // Bulk ready to emit!
  log('%data:cyan BULK Out [%s:blue]', stringify(data) );
  r.table('hep').insert(data).run();
}).on('error', function(err) {
  log('%error:red %s', err.toString() )
});

process.on('beforeExit', function() {
  bucket.close(function(leftData) {
    log('%data:red BULK Leftover [%s:blue]', stringify(leftData) );
    r.table('hep').insert(leftData).run();
  });
});

exports.bucket = bucket;
