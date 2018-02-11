const hepjs = require('hep-js');
const _http = require('http');
const dgram = require('dgram');
const net = require('net');

const log = require('./logger');

function init(){
  const hep = require('./hepcore');
  const sipfix = require('./sipfix');
  const processHep = hep.processHep;
  const processIpfix = sipfix.processIpfix;
}

exports.headerFormat = function(headers) {
  return Object.keys(headers).map(() => '%s:cyan: %s:yellow').join(' ')
}

exports.tcp = function({ port = undefined, address = '127.0.0.1' } = { address: '127.0.0.1' }) {
  init();
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
  init();
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
  init();
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
  init();
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

