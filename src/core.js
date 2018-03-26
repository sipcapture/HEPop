var argosy  = require('argosy')

var service = argosy()
var client = argosy()
// Pipeline
client.pipe(service).pipe(client)

// create a service queue 
var myRequest = service.accept({
    get     : 'echo',
    msg: argosy.pattern.match.defined
})

// process the requests for service
myRequest.process(function (msg, cb) {
            msg.newfield = "There!";
            cb(null, msg)
})

// use the service with argosy-client
client.invoke({ get: 'echo', msg: 'Hello' }, function (err, response) {
    if (err) return console.error(err);
    console.log(response);
})
