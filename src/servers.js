const hepjs = require('hep-js');

const _http = require('http')
const dgram = require('dgram')
const logger = require('oh-my-log')
const net =require('net')

var sipfix = require('sipfix');
var sip = require('sip');

const log = logger('client', {
  prefix: '[%__date:magenta]',
  locals: {
    'connect': '⚪',
    'disconnect': '⚫️️',
    'error': '✖️',
    'data': '✔',
    'start': '▶️',
    'stop': '❌'
  }
})

const processHep = function processHep(data,socket) {
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
}

// TODO: Move to Module
const processIpfix = function processIpfix(data,socket) {
	try {		
		var dlen = data.byteLength;
		// Determine IPFIX Type
		var result = sipfix.readHeader(data);
		log('%data:cyan SIPFIX Type [%s:blue]', result.setID );
		// HANDSHAKE
		if (result.SetID == 256) {
		  var shake = sipfix.readHandshake(data);
		  shake.SetID++
		  socket.write(sipfix.writeHandshake(shake) );
		  return;
		// SIP: SINGLE + MULTI TYPES
		} else if (result.SetID => 258 && result.SetID <= 261 ) {
		   if (dlen > result.Length ) {
			log('%data:cyan SIPFIX Multi-Payload [%s:yellow]', result.setID );
			var decoded;
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
			var decoded;
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

exports.headerFormat = function(headers) {
  return Object.keys(headers).map(() => '%s:cyan: %s:yellow').join(' ')
}

exports.tcp = function({ port = undefined, address = '127.0.0.1' } = { address: '127.0.0.1' }) {
  let server = net.createServer()

  server.on('error', (err) => log('%error:red %s', err.toString()))
  server.on('listening', () => log('%start:green TCP %s:gray %d:yellow', server.address().address, server.address().port))
  server.on('close', () => log('%stop:red %s:gray %d:yellow', server.address().address, server.address().port))
  server.on('connection', (socket) => {
    log('%connect:green (%s:italic:dim %d:italic:gray)', socket.remoteAddress, socket.remotePort)

    socket.on('data', (data) => processHep(data,socket))
    socket.on('error', (err) => log('%error:red (%s:italic:dim %d:italic:gray) %s', socket.remoteAddress, socket.remotePort, err.toString()))
    socket.on('end', () => log('%disconnect:red️ (%s:italic:dim %d:italic:gray)', socket.remoteAddress, socket.remotePort))

    // socket.pipe(socket)
  })

  server.listen(port, address)
}

exports.udp = function({ port = undefined, address = '127.0.0.1' } = { address: '127.0.0.1' }) {
  var socket = dgram.createSocket('udp4')

  socket.on('error', (err) => log('error %s:yellow', err.message))
  socket.on('listening', () => log('%start:green UDP4 %s:gray %d:yellow', socket.address().address, socket.address().port))
  socket.on('close', () => log('%stop:red %s:gray %d:yellow', socket.address().address, socket.address().port))

  socket.on('message', (message, remoteAddress) => {
    processHep(message,remoteAddress);
    // socket.send(message, 0, message.length, remoteAddress.port, remoteAddress.address)
  })

  socket.bind(port, address)
}

exports.sipfix = function({ port = undefined, address = '127.0.0.1' } = { address: '127.0.0.1' }) {
  var socket = dgram.createSocket('udp4')

  socket.on('error', (err) => log('error %s:yellow', err.message))
  socket.on('listening', () => log('%start:green SIPFIX %s:gray %d:yellow', socket.address().address, socket.address().port))
  socket.on('close', () => log('%stop:red %s:gray %d:yellow', socket.address().address, socket.address().port))

  socket.on('message', (message, remoteAddress) => {
    processIpfix(message,socket);
  })

  socket.bind(port, address)
}

exports.http = function({ port = undefined, address = '127.0.0.1' } = { address: '127.0.0.1' }) {
  let server = _http.createServer()

  server.on('error', (err) => log('%error:red %s', err.toString()))
  server.on('listening', () => log('%start:green HTTP %s:gray %d:yellow', server.address().address, server.address().port))
  server.on('close', () => log('%stop:red %s:gray %d:yellow', server.address().address, server.address().port))
  server.on('connection', (socket) => log('%connect:green (%s:italic:dim %d:italic:gray)', socket.remoteAddress, socket.remotePort))
  server.on('request', (request, response) => {
    log('%data:cyan (%s:italic:dim %d:italic:gray) HTTP/%s:dim %s:green %s:blue', request.socket.remoteAddress, request.socket.remotePort, request.httpVersion, request.method, request.url)
    log(`%data:cyan (%s:italic:dim %d:italic:gray) ${headerFormat(request.headers)}`, request.socket.remoteAddress, request.socket.remotePort, ...request.rawHeaders)

    response.writeHead(200, { 'Content-Type': request.headers['content-type'] || 'text/plain' })

    request.on('data', (data) => {
      processHep(data,request.socket);
      // response.write(data)
    })

    if (request.rawTrailers.length > 0) {
      log(`%data:cyan (%s:italic:dim %d:italic:gray) ${headerFormat(request.trailers)}`, request.socket.remoteAddress, request.socket.remotePort, ...request.rawTrailers)
    }

    request.on('end', () => {
      log('%disconnect:red️ (%s:italic:dim %d:italic:gray)', request.socket.remoteAddress, request.socket.remotePort)
      response.end()
    })

    request.on('error', (err) => log('%error:red (%s:italic:dim %d:italic:gray) %s', request.socket.remoteAddress, request.socket.remotePort, err.toString()))
  })

  server.listen(port, address)
}
