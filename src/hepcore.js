const hepjs = require('hep-js');
const log = require('./logger');
const sipdec = require('sip');
const r_bucket = require('./bucket').bucket;
const pgp_bucket = require('./bucket').pgp_bucket;
const mdb_bucket = require('./bucket').mdb_bucket;
const r = require('./bucket').r;
const stringify = require('safe-stable-stringify');
const flatten = require('flat')
const config = require('./config').getConfig();

exports.encapsulate = hepjs.encapsulate;
exports.decapsulate = hepjs.decapsulate;

const metrics = require('./metrics').metrics;
var mm = true;

if(!config.metrics || !config.metrics.influx){
	log('%data:red Metrics disabled');
	mm = false;
} else {
	log('%data:green Metrics enabled %s', stringify(config.metrics.influx));
}


exports.processHep = function processHep(data,socket) {
	try {
  	  if (config.debug) log('%data:cyan HEP Net [%s:blue]', JSON.stringify(socket) );
	  try {
		var decoded = hepjs.decapsulate(data);
		//decoded = flatten(decoded);
		var insert = { "hep_header": decoded.rcinfo,
				"payload": {},
				"raw": decoded.payload || ""
		};

	  } catch(e) { log('%s:red',e); }

	  switch(insert.hep_header.payloadType) {
		case 1:
		  try { var sip = sipdec.parse(insert.raw);

		  	insert.payload.protocol = 'SIP';
		  	if (sip && sip.headers) {
				  var hdr = sip.headers;
				  if (hdr['call-id']) insert.payload.callid = hdr['call-id'];
				  if (hdr.cseq && hdr.cseq.seq) insert.payload.cseq = hdr.cseq.seq;
				  if (hdr.from) {
					if (hdr.from.name || hdr.from.params.uri ) insert.payload.from_user = hdr.from.name || hdr.from.params.uri;
					if (hdr.from.params.tag) insert.payload.from_tag = hdr.from.params.tag;
				  }
				  if (hdr.to) {
					if (hdr.to.name) insert.payload.to_user = hdr.to.name;
					if (hdr.to.params.tag) insert.payload.to_tag = hdr.to.params.tag;
				  }
				  if (sip.method||sip.status) {
					insert.payload.method = sip.method||sip.status;
					if(mm) metrics.increment(metrics.counter("method", { code: sip.method || sip.status }));
				  }
				  if (hdr['user-agent']) {
					insert.payload.uas = hdr['user-agent'];
					if (mm) metrics.increment(metrics.counter("uac", { name: hdr['user-agent'] }));
				  }
			  }
	  		  if (config.debug) {
				log('%data:cyan HEP Type [%s:blue]', 'SIP' );
			  	log('%data:cyan HEP Payload [%s:yellow]', stringify( insert.payload, null, 2) );
			  }
		  } catch(e) { log("%data:red %s",e);}
		  break;
		default:
	  	  if (config.debug) {
			log('%data:cyan HEP Type [%s:blue]', decoded.payloadType );
		  	log('%data:cyan HEP Payload [%s:yellow]', stringify(decoded.payload) );
		  }
	  }
	  if (mm) metrics.increment(metrics.counter("hep", { type: insert.hep_header.payloadType  }));

	  if (pgp_bucket) pgp_bucket.push(insert);

	  if (r_bucket) {
	     if(decoded['rcinfo.timeSeconds']) decoded['rcinfo.ts'] = r.epochTime(decoded['rcinfo.timeSeconds']);
	     r_bucket.push(decoded);
	  }
	  if (mdb_bucket) mdb_bucket.push(decoded);

	} catch(err) { log('%error:red %s', err.toString() ) }
};
