const hepjs = require('hep-js');
const log = require('./logger');
const sip = require('sip');
const r_bucket = require('./bucket').bucket;
const pgp_bucket = require('./bucket').pgp_bucket;
const mdb_bucket = require('./bucket').mdb_bucket;
const r = require('./bucket').r;
const stringify = require('safe-stable-stringify');
const flatten = require('flat')
const config = require('./config').getConfig();

exports.encapsulate = hepjs.encapsulate;
exports.decapsulate = hepjs.decapsulate;
		    
exports.processHep = function processHep(data,socket) {
	try {
	  //var decoded = hepjs.decapsulate(data);
  	  if (config.debug) log('%data:cyan HEP Net [%s:blue]', JSON.stringify(socket) );
	  try { var decoded = hepjs.decapsulate(data); decoded = flatten(decoded); } catch(e) { log('%s:red',e); }
	  switch(decoded['rcinfo.payloadType']) {
		case 1:
		  try { decoded.sip = sip.parse(decoded.payload); } catch(e) {}
	  	  if (config.debug) {
			log('%data:cyan HEP Type [%s:blue]', 'SIP' );
		  	log('%data:cyan HEP Payload [%s:yellow]', stringify( decoded.sip, null, 2) );
		  }
		  break;
		default:
	  	  if (config.debug) {
			log('%data:cyan HEP Type [%s:blue]', decoded.payloadType );
		  	log('%data:cyan HEP Payload [%s:yellow]', stringify(decoded.payload) );
		  }
	  }
	  // if(decoded['rcinfo.timeSeconds']) decoded['rcinfo.ts'] = r.epochTime(decoded['rcinfo.timeSeconds']);
	  if (r_bucket) r_bucket.push(decoded);
	  if (pgp_bucket) pgp_bucket.push(decoded);
	  if (mdb_bucket) mdb_bucket.push(decoded);
		
	} catch(err) { log('%error:red %s', err.toString() ) }
};
