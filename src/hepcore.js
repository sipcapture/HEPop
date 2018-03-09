const hepjs = require('hep-js');
const log = require('./logger');
const sip = require('sip');
const bucket = require('./bucket').bucket;
const r = require('./bucket').r;
const stringify = require('safe-stable-stringify');

exports.encapsulate = hepjs.encapsulate;
exports.decapsulate = hepjs.decapsulate;
		    
exports.processHep = function processHep(data,socket) {
	try {
	  var decoded = hepjs.decapsulate(data);
  	  log('%data:cyan HEP [%s:blue]', JSON.stringify(socket) );
	  switch(decoded.payloadType) {
		case 1:
	  	  log('%data:cyan HEP Type [%s:blue]', 'SIP' );
		  log('%data:cyan HEP Payload [%s:yellow]', stringify( sip.parse(decoded.payload)) );
		  break;
		default:
	  	  log('%data:cyan HEP Type [%s:blue]', decoded.payloadType );
		  log('%data:cyan HEP Payload [%s:yellow]', stringify(decoded.payload) );
	  }
	  if(decoded.rcinfo.timeSeconds) decoded.rcinfo.ts = r.epochTime(decoded.rcinfo.timeSeconds);
	  bucket.push(decoded);
		
	} catch(err) { log('%error:red %s', err.toString() ) }
};
