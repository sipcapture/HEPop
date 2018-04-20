const log = require('./logger');
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

exports.processJson = function(data,socket) {
	try {
  	  //if (config.debug) log('%data:cyan JSON Net [%s:blue][%s:green]', stringify(socket) );
	  if (!data) return;
	  data = JSON.parse(data.toString());
  	  if (config.debug) log('%data:cyan JSON Data [%s:blue][%s:green]', stringify(data) );
	  // DB Schema
	  var insert = { "protocol_header": socket._peername || {},
			 "data_header": {},
			 "raw": data || ""
		};
	  var tags = {};
	  // Create protocol bucket
	  var key = 1000 + "_"+ (insert.protocol_header.type || "default");
	  if (config.db.pgsql && !buckets[key] ) { buckets[key] = require('./bucket').pgp_bucket; }
	  // else if (config.db.rethink &&!buckets[key]) { const r_bucket = require('./bucket').bucket; const r = require('./bucket').r; }
	  buckets[key].set_id("hep_proto_"+key);

	  if (data.type && data.event && data.session_id){
	    // Janus Media Reports
	    if (config.debug) log('%data:green JANUS REPORT [%s]',stringify(data) );
	    /* Static SID from session_id */
	    insert.sid = data.session_id || '000000000000';
	    tags = { session: data.session_id, handle: data.handle_id };
	    if (data.opaque_id) tags.opaque_id = data.opaque_id;
	    if (data.type) tags.type = data.type;

	    switch(data.type) {
		case 256:
		  break;
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
	    tags.source = "janus";
	  } else if (data.type && data.event && !data.session_id){
		
		console.log('JANUS CONFIG', data);
		return;

	  } else if (data.event && data.event == 'producer.stats' &&  data.stats){
		// MediaSoup Media Reports
		/* Hybrid SID */
		insert.sid = data.producerId+"_"+data.peerName;
	        if (config.debug) log('%data:green MEDIASOUP PRODUCER REPORT [%s]',stringify(data) );
		tags = { roomId: data.roomId, peerName: data.peerName, producerId: data.producerId, event: data.event };
		    if (data.stats[0].mediaType) tags.media = data.stats[0].mediaType;
		    if (data.stats[0].mediaType) tags.media = data.stats[0].mimeType;
		    if (data.stats[0].type) tags.type = data.stats[0].type;
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'bitrate' ), data.stats[0]["bitrate"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'byteCount' ), data.stats[0]["byteCount"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'firCount' ), data.stats[0]["firCount"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'fractionLost' ), data.stats[0]["fractionLost"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'jitter' ), data.stats[0]["jitter"] );
	            metrics.increment(metrics.counter("mediasoup_producer", tags, 'nackCount' ), data.stats[0]["nackCount"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'packetCount' ), data.stats[0]["packetCount"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'packetsDiscarded' ), data.stats[0]["packetsDiscarded"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'packetsLost' ), data.stats[0]["packetsLost"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'packetsRepaired' ), data.stats[0]["packetsRepaired"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'nacks-received' ), data.stats[0]["nacks-received"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'pliCount' ), data.stats[0]["pliCount"] );
		    metrics.increment(metrics.counter("mediasoup_producer", tags, 'sliCount' ), data.stats[0]["sliCount"] );
		/* Custom Fields */
		if (data.transportId) tags.transportId = data.transportId;

	  } else if (data.event && data.event == 'transport.stats' &&  data.stats){
		// MediaSoup Media Reports
		/* Hybrid SID */
		insert.sid = data.producerId+"_"+data.peerName;
	        if (config.debug) log('%data:green MEDIASOUP TRANSPORT REPORT [%s]',stringify(data) );
		tags = { roomId: data.roomId, peerName: data.peerName, producerId: data.producerId, event: data.event };
		    if (data.stats[0].type) tags.type = data.stats[0].type;
		    /* IP Fields */
	            if (data.stats[0].iceSelectedTuple) {
			/* correlate to stats via LRU? */
			insert.proto_header = data.stats[0].iceSelectedTuple;
		        //tags.source_ = data.stats[0].iceSelectedTuple.localIP+":"+data.stats[0].iceSelectedTuple.localPort;
		        //tags.source = data.stats[0].iceSelectedTuple.remoteIP+":"+data.stats[0].iceSelectedTuple.remotePort;
		    }
		    metrics.increment(metrics.counter("mediasoup_transport", tags, 'bytesReceived' ), data.stats[0]["bytesReceived"] );
		    metrics.increment(metrics.counter("mediasoup_transport", tags, 'bytesSent' ), data.stats[0]["bytesSent"] );

		/* Custom Fields */
	        if (data.stats[0].dtlsState) tags.media = data.stats[0].dtlsState;
	        tags.source = "mediasoup";
	  }

	  // Use Tags for Protocol Search
	  insert.data_header = tags;

	  //if (r_bucket) r_bucket.push(JSON.parse(dec));
	  if (buckets[key]) buckets[key].push(insert);


	} catch(err) { log('%error:red %s', err.toString() ) }
};
