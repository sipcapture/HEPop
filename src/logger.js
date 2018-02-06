const logger = require('oh-my-log')

exports.log = logger('client', {
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
