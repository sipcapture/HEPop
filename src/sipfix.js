const log = require('./logger');
const sipfix = require('sipfix');
		    
exports.processIpfix = function processIpfix(data,socket) {
	try {		
		var dlen = data.byteLength;
		// Determine IPFIX Type
		var result = sipfix.readHeader(data);
		var decoded;
		log('%data:cyan SIPFIX Type [%s:blue]', result.setID );
		// HANDSHAKE
		if (result.SetID == 256) {
		  var shake = sipfix.readHandshake(data);
		  shake.SetID++
		  socket.write( sipfix.writeHandshake(shake) );
		  return;

		} else if (result.SetID >= 258 && result.SetID <= 261 ) {
		   if (dlen > result.Length ) {
			log('%data:cyan SIPFIX Multi-Payload [%s:yellow]', result.setID );
		  	switch(result.setID){
				case 258:
					decoded = sipfix.SipIn( data.slice(0,result.Length));
					break;
				case 259:
					decoded = sipfix.SipOut( data.slice(0,result.Length));
					break;
				case 260:
					decoded = sipfix.SipInTCP( data.slice(0,result.Length));
					break;
				case 261:
					decoded = sipfix.SipOutTCP( data.slice(0,result.Length));
					break;
			}
			if (decoded) {
				decoded.SrcIP = Array.prototype.join.call(decoded.SrcIP, '.');
				decoded.DstIP = Array.prototype.join.call(decoded.DstIP, '.');
				log('%data:cyan SIPFIX Payload [%s:yellow]', decoded );
				// Process ....
			}
			// Process Next
			processIpfix(data.slice(result.Length,data.length));
			return;
			   
		   } else {
			log('%data:cyan SIPFIX Single-Payload [%s:yellow]', result.setID );
		  	switch(result.setID){
				case 258:
					decoded = sipfix.SipIn( data.slice(0,result.Length));
					break;
				case 259:
					decoded = sipfix.SipOut( data.slice(0,result.Length));
					break;
				case 260:
					decoded = sipfix.SipInTCP( data.slice(0,result.Length));
					break;
				case 261:
					decoded = sipfix.SipOutTCP( data.slice(0,result.Length));
					break;
			}
			if (decoded) {
				decoded.SrcIP = Array.prototype.join.call(decoded.SrcIP, '.');
				decoded.DstIP = Array.prototype.join.call(decoded.DstIP, '.');
				log('%data:cyan SIPFIX Payload [%s:yellow]', decoded );
				// Process ....
			}
			return;
		   }
		// QOS REPORTS	
		} else if (result.SetID == 268) {
			var qos = sipfix.StatsQos(data);
			if (qos) {
				qos.CallerIncSrcIP = Array.prototype.join.call(qos.CallerIncSrcIP, '.');
				qos.CallerIncDstIP = Array.prototype.join.call(qos.CallerIncDstIP, '.');
				qos.CalleeIncSrcIP = Array.prototype.join.call(qos.CalleeIncSrcIP, '.');
				qos.CalleeIncDstIP = Array.prototype.join.call(qos.CalleeIncDstIP, '.');
				qos.CallerOutSrcIP = Array.prototype.join.call(qos.CallerOutSrcIP, '.');
				qos.CallerOutDstIP = Array.prototype.join.call(qos.CallerOutDstIP, '.');
				qos.CalleeOutSrcIP = Array.prototype.join.call(qos.CalleeOutSrcIP, '.');
				qos.CalleeOutDstIP = Array.prototype.join.call(qos.CalleeOutDstIP, '.');
				log('%data:cyan SIPFIX QOS Payload [%s:yellow]', qos );
				// Process ....
			}
		}
		
	} catch(err) { log('%error:red %s', err.toString() ); }
}
