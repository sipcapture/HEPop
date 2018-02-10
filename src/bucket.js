/*
 * Bulk Bucket Emitters
 * bucket.push(object);
 */

const log = require('./logger');
const bucket_emitter = require('bucket-emitter')

const bucket = bucket_emitter.create({
    timeout: 1000, //if there's no data input until timeout, emit data forcefully. 
    maxSize: 5000, //data emitted count 
    useInterval: false //if this value is true, data event is emitted even If new data is pushed. 
});
 
bucket.on('data', function(data) {
  // Bulk ready to emit!
  log('%data:orange BULK [%s:blue]', JSON.stringify(data) );
}).on('error', function(err) {
  log('%error:red %s', err.toString() )
});
 
process.on('beforeExit', function() {
  bucket.close(function(leftData) {
    log('%data:red BULK Leftover [%s:blue]', JSON.stringify(leftData) );
  });
});

exports.bucket = bucket;
