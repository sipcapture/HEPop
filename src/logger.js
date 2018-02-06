const logger = require('oh-my-log');
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
});

module.exports = log;
