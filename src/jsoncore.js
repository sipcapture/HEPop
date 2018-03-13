const log = require('./logger');
const r_bucket = require('./bucket').bucket;
const pgp_bucket = require('./bucket').pgp_bucket;
const r = require('./bucket').r;
const stringify = require('safe-stable-stringify');
const flatten = require('flat')
const config = require('./config').getConfig();

exports.processJson = function processHep(data,socket) {
	try {
  	  if (config.debug) log('%data:cyan JSON Net [%s:blue][%s:green]', stringify(socket) );
	  if (!data||data.length===0) return;
	  var dec = data.toString();
	  if (r_bucket) r_bucket.push(JSON.parse(dec));
	  if (pgp_bucket) pgp_bucket.push(dec);

	} catch(err) { log('%error:red %s', err.toString() ) }
};
