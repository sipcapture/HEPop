const hepjs = require('hep-js');
const log = require('./logger);

exports.encapsulate = hepjs.encapsulate;
exports.decapsulate = hepjs.decapsulate;
exports.processHep = function processHep(data,socket) {
	try {
	  var decoded = hepjs.decapsulate(data);
  	  log('%data:cyan HEP [%s:blue]', JSON.stringify(socket) );
	  switch(decoded.payloadType) {
		case 1:
	  	  log('%data:cyan HEP Type [%s:blue]', 'SIP' );
		  log('%data:cyan HEP Payload [%s:yellow]', sip.parse(decoded.payload) );
		  break;
		default:
	  	  log('%data:cyan HEP Type [%s:blue]', decoded.payloadType );
		  log('%data:cyan HEP Payload [%s:yellow]', decoded.payload );
	  }
	} catch(err) { log('%error:red %s', err.toString() ) }
};
