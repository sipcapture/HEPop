const hepjs = require('hep-js');
const log = require('./logger');
const sipdec = require('parsip');
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


var buckets = [];

exports.processHep = function processHep(data,socket) {
	try {
  	  if (config.debug) log('%data:cyan HEP Net [%s:blue]', JSON.stringify(socket) );
	  try {
		var decoded = hepjs.decapsulate(data);
		//decoded = flatten(decoded);
		var insert = { "protocol_header": decoded.rcinfo,
				"data_header": {},
				"raw": decoded.payload || ""
		};

	  } catch(e) { log('%s:red',e); }

	  // Create protocol bucket
	  var key = insert.protocol_header.payloadType +"_"+ (insert.protocol_header.transactionType || "default");
	  if (!buckets[key]) buckets[key] = require('./bucket').pgp_bucket;
	  buckets[key].set_id("hep_proto_"+key);

	  switch(insert.protocol_header.payloadType) {
		case 1:
		  // todo: move to modular function!
		  try { var sip = sipdec.getSIP(insert.raw);
			if (config.debug) log('%stop:red %s',stringify(sip));
		  	insert.data_header.protocol = 'SIP';
		  	if (sip && sip.headers) {
				  var hdr = sip.headers;
				  if (sip.call_id) insert.data_header.callid = sip.call_id;
				  if (sip.cseq) insert.data_header.cseq = sip.cseq;
				  if (sip.from) {
					if (sip.from.uri._user) insert.data_header.from_user = sip.from.uri._user;
					if (sip.from_tag) insert.data_header.from_tag = sip.from_tag;
				  }
				  if (sip.to) {
					if (sip.to.uri._user) insert.data_header.to_user = sip.to.uri._user;
					if (sip.to_tag) insert.data_header.to_tag = sip.to_tag;
				  }
				  if (sip.ruri) {
					if (sip.ruri_user) insert.data_header.ruri_user = sip.ruri._user;
				  }
				  if ( sip.method ) {
					insert.data_header.method = sip.method;
				  }
				  if ( sip.status_code ) {
					if (sip.status_code != sip.method) insert.data_header.method += ":"+ sip.status_code;
					else insert.data_header.method = sip.status_code;
				  }
				  if(mm) metrics.increment(metrics.counter("method", { "ip": socket.address || "0.0.0.0" }, insert.data_header.method  ) );

				  if (hdr['User-Agent'][0]) {
					insert.data_header.uas = hdr['User-Agent'][0].raw;
					if (mm) metrics.increment(metrics.counter("uac", { "ip": socket.address || "0.0.0.0" }, hdr['User-Agent'][0].raw ));
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
	  if (mm) metrics.increment(metrics.counter("hep", { "ip": socket.address || "0.0.0.0" }, insert.protocol_header.payloadType ));

	  if (pgp_bucket) buckets[key].push(insert);

	  if (r_bucket) {
	     if(decoded['rcinfo.timeSeconds']) decoded['rcinfo.ts'] = r.epochTime(decoded['rcinfo.timeSeconds']);
	     r_bucket.push(decoded);
	  }
	  if (mdb_bucket) mdb_bucket.push(decoded);

	} catch(err) { log('%error:red %s', err.toString() ) }
};
