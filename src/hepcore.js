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
const importFresh = require('import-fresh');

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

var forceJSON = function(decoded){
		if (decoded.payload && typeof decoded.payload != "object") {
		  try {
			const tmp = JSON.parse( "{"+ decoded.payload.split('{').pop().split('}').shift() + "}" );
			if (tmp.payload) decoded.payload = tmp.payload;
			else if (tmp) decoded.payload = tmp;
			return decoded;
		  } catch(e) { log('PAYLOAD RAW ERROR, NON CRITICAL',e); return decoded; }
		}
}

exports.processHep = function processHep(data,socket) {
	try {
  	  if (config.debug) log('%data:cyan HEP Net [%s:blue]', JSON.stringify(socket) );
	  try {
		var decoded = hepjs.decapsulate(data);
		//decoded = flatten(decoded);
		var insert = {
				"protocol_header": decoded.rcinfo,
				"data_header": {},
				"create_date": new Date(),
				"raw": decoded.payload || ""
		};
		/* HEP Correlation ID as SID */
		if (decoded.rcinfo.correlation_id) insert.sid = decoded.rcinfo.correlation_id;

		/* HEP Timestamps concat as CREATE_DATE */
		if (decoded.rcinfo.timeSeconds && decoded.rcinfo.timeUseconds) insert.create_date = new Date( (decoded.rcinfo.timeSeconds * 1000 )+(((100000+decoded.rcinfo.timeUseconds)/1000)-100));

	  } catch(e) { log('%s:red',e); }

	  // Create protocol bucket
	  var ukey = insert.protocol_header.payloadType +"_"+ (insert.protocol_header.transactionType || "default");
	  if (insert.protocol_header.payloadType == 1) ukey = insert.protocol_header.payloadType +"_"+ "call";
	  // if (!buckets[ukey]) buckets[ukey] = require('./bucket').pgp_bucket;
	  if (!buckets[ukey]) buckets[ukey] = importFresh('./bucket').pgp_bucket;
	  buckets[ukey].set_id("hep_proto_"+ukey);
	  var iptags = { "ip": socket.address || "0.0.0.0" };

	  switch(insert.protocol_header.payloadType) {
		case 1:
		  // todo: move to modular function!
		  try { var sip = sipdec.getSIP(insert.raw);
			if (config.debug) log('%stop:red %s',stringify(sip));
		  	insert.data_header.protocol = 'SIP';
		  	if (sip && sip.headers) {
				  var hdr = sip.headers;
				  if (sip.call_id) {
					insert.data_header.callid = sip.call_id;
					/* SID for SIP is always Call-ID */
					if (!insert.sid) insert.sid = sip.call_id;
				  }
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
				  if(mm) metrics.increment(metrics.counter("method", iptags, insert.data_header.method  ) );

				  if (hdr['User-Agent'] && hdr['User-Agent'][0]) {
					insert.data_header.uas = hdr['User-Agent'][0].raw;
					if (mm) metrics.increment(metrics.counter("uac", iptags, hdr['User-Agent'][0].raw ));
				  }

				  /* PUBLISH RTCPXR-VQ */
				  try {
				    if ((sip.headers['Content-Type'] && sip.headers['Content-Type'][0]) && sip.headers['Content-Type'][0].raw == 'application/vq-rtcpxr'){
					var temp;
					if (sip.headers.Packetloss){
						temp=sipdec.getVQ(sip.headers.Packetloss[0].raw)['NLR'];
						if (mm) metrics.increment(metrics.counter("rtcpxr", iptags, 'NLR' ), temp );
					}
					if (sip.headers.Delay){
						temp=sipdec.getVQ(sip.headers.Delay[0].raw)['IAJ'];
						if (mm) metrics.increment(metrics.counter("rtcpxr", iptags, 'IAJ' ), temp );
					}
					if (sip.headers.Qualityest){
						temp=sipdec.getVQ(sip.headers.Qualityest[0].raw)['MOSCQ'];
						if (mm) metrics.increment(metrics.counter("rtcpxr", iptags, 'MOSCQ' ), temp );
					}
				    }
				  } catch(e) { log('%s:red', e); }
				  /* X-RTP-STAT */
				  try {
				    if (hdr['X-Rtp-Stat'] && hdr['X-Rtp-Stat'][0]) {
					try {
						var xrtp = sipdec.getVQ(hdr['X-Rtp-Stat'][0].raw);
						Object.keys(xrtp).forEach(function(key){
							if (mm) metrics.increment(metrics.counter("xrtp", iptags, key ), xrtp[key] );
						})
					} catch(e) { log(e); }
				    }
				  } catch(e) { log('%s:red', e); }
				  /* P-RTP-STATS */
				  try {
				    if (hdr['P-Rtp-Stats'] && hdr['P-Rtp-Stats'][0]) {
					try {
						var prtp = sipdec.getVQ(hdr['P-Rtp-Stats'][0].raw);
						Object.keys(prtp).forEach(function(key){
							if (mm) metrics.increment(metrics.counter("xrtp", iptags, key ), prtp[key] );
						})
					} catch(e) { log(e); }
				    }
				  } catch(e) { log('%s:red', e); }
			  }
	  		  if (config.debug) {
				log('%data:cyan HEP Type [%s:blue]', 'SIP' );
			  	log('%data:cyan HEP Payload [%s:yellow]', stringify( insert.data_header, null, 2) );
			  }
		  } catch(e) { log("%data:red %s",e);}
		  break;
		case 5:
		  /* RTCP */
		  try {
			  var rtcp = JSON.parse(insert.raw);
			  if (config.debug){
				log('%data:cyan HEP Type [%s:blue]', insert.protocol_header.payloadType );
			  	log('%data:cyan HEP RTCP Payload [%s:yellow]', stringify( rtcp, null, 2) );
			  }
			  if (mm & rtcp.report_blocks.length){
				var tags = { "ip": socket.address || "0.0.0.0" };
				if (rtcp.type) tags.type = rtcp.type;
				for(i=0;i<rtcp.report_blocks.length;i++){
				  if (rtcp.report_blocks[i].fraction_lost) metrics.increment(metrics.counter("rtcp", tags, "fraction_lost" ), rtcp.report_blocks[i].fraction_lost);
				  if (rtcp.report_blocks[i].packets_lost) metrics.increment(metrics.counter("rtcp", tags, "packets_lost" ), rtcp.report_blocks[i].packets_lost);
				  if (rtcp.report_blocks[i].ia_jitter) metrics.increment(metrics.counter("rtcp", tags, "jitter" ), rtcp.report_blocks[i].ia_jitter);
				  if (rtcp.report_blocks[i].dlsr) metrics.increment(metrics.counter("rtcp", tags, "dlsr" ), rtcp.report_blocks[i].dlsr);
				}
			  }
		  } catch(e) { log("%data:red %s",e); }
		  break;
		case 34:
		  /* RTPAGENT */
		  try {
			  var rtp = JSON.parse(insert.raw);
			  if (config.debug){
				log('%data:cyan HEP Type [%s:blue]', insert.protocol_header.payloadType );
			  	log('%data:cyan HEP RTPAGENT Payload [%s:yellow]', stringify( rtp, null, 2) );
			  }
			  if (mm){
				var tags = { "ip": socket.address || "0.0.0.0"};
				if (rtp.TYPE) tags.type = rtp.TYPE;
				if (rtp.CODEC_NAME) tags.codec = rtp.CODEC_NAME;
				metrics.increment(metrics.counter("rtp", tags, "PACKET_LOSS" ), rtp.PACKET_LOSS);
				metrics.increment(metrics.counter("rtp", tags, "JITTER" ), rtp.JITTER);
				metrics.increment(metrics.counter("rtp", tags, "MOS" ), rtp.MOS);
			  }
		  } catch(e) { log("%data:red RTPAGENTSTATS ERROR: %s",e); }
		  break;
		case 86:
		  /* OpenSIPS Event */
	  	  if (config.debug) {
			log('%data:cyan HEP OpenSIPS Type [%s:blue]', insert.protocol_header.payloadType );
		  	log('%data:cyan HEP OpenSIPS Payload [%s:yellow]', stringify(decoded.payload) );
		  }
		  if (insert.raw && insert.raw.event) insert.data_header.event = insert.raw.event;
		  if (insert.raw && insert.raw.text) insert.data_header.text = insert.raw.text;

		case 81:
		  /* OpenSIPS CDR */
	  	  if (config.debug) {
			log('%data:cyan HEP OpenSIPS Type [%s:blue]', insert.protocol_header.payloadType );
		  	log('%data:cyan HEP OpenSIPS Payload [%s:yellow]', stringify(decoded.payload) );
		  }
		  decoded = forceJSON(decoded);
		  if (decoded.payload && decoded.payload.call_id) insert.sid = decoded.payload.call_id;
		  if (decoded.payload) insert.data_header = decoded.payload;

		default:
	  	  if (config.debug) {
			log('%data:cyan HEP Type [%s:blue]', insert.protocol_header.payloadType );
		  	log('%data:cyan HEP Payload [%s:yellow]', stringify(decoded.payload) );
		  }
	  }
	  if (mm) metrics.increment(metrics.counter("hep", iptags, insert.protocol_header.payloadType ));

	  if (pgp_bucket) buckets[ukey].push(insert);

	  if (r_bucket) {
	     if(decoded['rcinfo.timeSeconds']) decoded['rcinfo.ts'] = r.epochTime(decoded['rcinfo.timeSeconds']);
	     r_bucket.push(decoded);
	  }
	  if (mdb_bucket) mdb_bucket.push(decoded);

	} catch(err) { log('%error:red %s', err.toString() ) }
};
