const log = require('./logger');
const stringify = require('safe-stable-stringify');
const flatten = require('flat')
const config = require('./config').getConfig();

if (config.db.pgsql) { const pgp_bucket = require('./bucket').pgp_bucket; }
if (config.db.rethink) { const r_bucket = require('./bucket').bucket; const r = require('./bucket').r; }


const metrics = require('./metrics').metrics;
var mm = true;

if(!config.metrics || !config.metrics.influx){
	log('%data:red Metrics disabled');
	mm = false;
} else {
	log('%data:green Metrics enabled %s', stringify(config.metrics.influx));
}

var buckets = [];

exports.processJson = function(data,socket) {
	try {
  	  //if (config.debug) log('%data:cyan JSON Net [%s:blue][%s:green]', stringify(socket) );
	  if (!data) return;
	  data = JSON.parse(data.toString());
  	  if (config.debug) log('%data:cyan JSON Data [%s:blue][%s:green]', stringify(data) );
	  // DB Schema
	  var insert = { "protocol_header": socket,
			 "data_header": {},
			 "raw": data || ""
		};
	  var tags = {};
	  // Create protocol bucket
	  var key = 1000 + "_"+ (insert.protocol_header.type || "default");
	  if (config.db.pgsql && !buckets[key] ) { buckets[key] = require('./bucket').pgp_bucket; }
	  // else if (config.db.rethink &&!buckets[key]) { const r_bucket = require('./bucket').bucket; const r = require('./bucket').r; }
	  buckets[key].set_id("hep_proto_"+key);

	  if (data.type && data.event){
	    // Janus Media Reports
	    if (config.debug) log('%data:green JANUS REPORT [%s]',stringify(data) );
	    tags = { session: data.session_id, handle: data.handle_id };
	    if (data.type) tags.type = data.type;

	    switch(data.type) {
		case 32:
		  if (data.event.media) tags.medium = data.event.media;
		  if(data.event.receiving) {
		    metrics.increment(metrics.counter("janus", tags, 'Receiving') );
		  } else if(data.event.base) {
		    metrics.increment(metrics.counter("janus", tags, 'LSR' ), data.event["lsr"] );
		    metrics.increment(metrics.counter("janus", tags, 'lost' ), data.event["lost"] || 0 );
		    metrics.increment(metrics.counter("janus", tags, 'lost-by-remote' ), data.event["lost-by-remote"] || 0 );
		    metrics.increment(metrics.counter("janus", tags, 'jitter-local' ), data.event["jitter-local"] || 0 );
		    metrics.increment(metrics.counter("janus", tags, 'jitter-remote' ), data.event["jitter-remote"] || 0);
	            metrics.increment(metrics.counter("janus", tags, 'packets-sent' ), data.event["packets-sent"] || 0);
		    metrics.increment(metrics.counter("janus", tags, 'packets-received' ), data.event["packets-sent"] || 0);
		    metrics.increment(metrics.counter("janus", tags, 'bytes-sent' ), data.event["bytes-sent"] || 0);
		    metrics.increment(metrics.counter("janus", tags, 'bytes-received' ), data.event["bytes-received"]|| 0 );
		    metrics.increment(metrics.counter("janus", tags, 'nacks-sent' ), data.event["nacks-sent"] || 0);
		    metrics.increment(metrics.counter("janus", tags, 'nacks-received' ), data.event["nacks-received"] || 0);
		  }
		break;
	    }

	  } else if (data.event && data.event == 'producer.stats' &&  data.stats){
		// MediaSoup Media Reports
	        if (config.debug) log('%data:green MEDIASOUP REPORT [%s]',stringify(data) );
		tags = { roomId: data.roomId, peerName: data.peerName, producerId: data.producerId };
		    if (data.stats[0].mediaType) tags.media = data.stats[0].mediaType;
		    if (data.stats[0].type) tags.type = data.stats[0].type;
		    metrics.increment(metrics.counter("mediasoup", tags, 'bitrate' ), data.stats[0]["bitrate"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'byteCount' ), data.stats[0]["byteCount"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'firCount' ), data.stats[0]["firCount"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'fractionLost' ), data.stats[0]["fractionLost"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'jitter' ), data.stats[0]["jitter"] );
	            metrics.increment(metrics.counter("mediasoup", tags, 'nackCount' ), data.stats[0]["nackCount"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'packetCount' ), data.stats[0]["packetCount"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'packetsDiscarded' ), data.stats[0]["packetsDiscarded"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'packetsLost' ), data.stats[0]["packetsLost"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'packetsRepaired' ), data.stats[0]["packetsRepaired"] );
		    metrics.increment(metrics.counter("mediasoup", tags, 'nacks-received' ), data.stats[0]["nacks-received"] );
	  }

	  // Use Tags for Protocol Search
	  insert.data_header = tags;

	  //if (r_bucket) r_bucket.push(JSON.parse(dec));
	  if (pgp_bucket) buckets[key].push(insert);


	} catch(err) { log('%error:red %s', err.toString() ) }
};
