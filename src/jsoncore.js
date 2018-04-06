const log = require('./logger');
const r_bucket = require('./bucket').bucket;
const pgp_bucket = require('./bucket').pgp_bucket;
const r = require('./bucket').r;
const stringify = require('safe-stable-stringify');
const flatten = require('flat')
const config = require('./config').getConfig();

const metrics = require('./metrics').metrics;
var mm = true;

if(!config.metrics || !config.metrics.influx){
	log('%data:red Metrics disabled');
	mm = false;
} else {
	log('%data:green Metrics enabled %s', stringify(config.metrics.influx));
}

var buckets = [];

exports.processJson = function processHep(data,socket) {
	try {
  	  if (config.debug) log('%data:cyan JSON Net [%s:blue][%s:green]', stringify(socket) );
	  if (!data||data.length===0) return;
		
	  // DB Schema
	  var insert = { "protocol_header": socket,
			 "data_header": {},
			 "raw": data || ""
		};	
	  // Create protocol bucket
	  var key = 127 + "_"+ (insert.protocol_header.type || "default");
	  if (!buckets[key]) buckets[key] = require('./bucket').pgp_bucket;
	  buckets[key].set_id("hep_proto_"+key);	
		
	  if (data.type && data.event){	
	    // Janus Media Reports
	    var tags = { session: data.session_id, handle: data.handle_id };
	    switch(data.type) {
		case 32:
		  if (data.event.media) tags.medium = data.event.media;
		  if(data.event.receiving) {
		    metrics.increment(metrics.gauge("janus", tags, 'Receiving') );
		  } else if(data.event.base && mm) {
		    metrics.increment(metrics.gauge("janus", tags, 'LSR' ), data.event["lsr"] );
		    metrics.increment(metrics.gauge("janus", tags, 'lost' ), data.event["lost"] );
		    metrics.increment(metrics.gauge("janus", tags, 'lost-by-remote' ), data.event["lost-by-remote"] );
		    metrics.increment(metrics.gauge("janus", tags, 'jitter-local' ), data.event["jitter-local"] );
		    metrics.increment(metrics.gauge("janus", tags, 'jitter-remote' ), data.event["jitter-remote"] );
	            metrics.increment(metrics.gauge("janus", tags, 'packets-sent' ), data.event["packets-sent"] );
		    metrics.increment(metrics.gauge("janus", tags, 'packets-received' ), data.event["packets-sent"] );
		    metrics.increment(metrics.gauge("janus", tags, 'bytes-sent' ), data.event["bytes-sent"] );
		    metrics.increment(metrics.gauge("janus", tags, 'bytes-received' ), data.event["bytes-received"] );
		    metrics.increment(metrics.gauge("janus", tags, 'nacks-sent' ), data.event["nacks-sent"] );
		    metrics.increment(metrics.gauge("janus", tags, 'nacks-received' ), data.event["nacks-received"] );
		  }
		break;
	    }
		  
	  } else if (data.event && data.event == 'producer.stats' &&  data.stats){	
		// MediaSoup Media Reports
		var tags = { roomId: data.roomId, peerName: data.peerName, producerId: data.producerId };
		    if (data.stats[0].mediaType) tags.media = data.stats[0].mediaType;
		    if (data.stats[0].type) tags.media = data.stats[0].type;
		    metrics.increment(metrics.gauge("mediasoup", tags, 'bitrate' ), data.stats[0]["bitrate"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'byteCount' ), data.stats[0]["byteCount"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'firCount' ), data.stats[0]["firCount"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'fractionLost' ), data.stats[0]["fractionLost"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'jitter' ), data.stats[0]["jitter"] );
	            metrics.increment(metrics.gauge("mediasoup", tags, 'nackCount' ), data.stats[0]["nackCount"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'packetCount' ), data.stats[0]["packetCount"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'packetsDiscarded' ), data.stats[0]["packetsDiscarded"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'packetsLost' ), data.stats[0]["packetsLost"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'packetsRepaired' ), data.stats[0]["packetsRepaired"] );
		    metrics.increment(metrics.gauge("mediasoup", tags, 'nacks-received' ), data.stats[0]["nacks-received"] );
	  }
			  
	  //if (r_bucket) r_bucket.push(JSON.parse(dec));
	  //if (pgp_bucket) pgp_bucket.push(dec);

	} catch(err) { log('%error:red %s', err.toString() ) }
};
