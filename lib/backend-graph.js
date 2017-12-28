var GraphEmitter = require('./graph-emitter'),
    Backend = require('./backend'),
    util = require('./util'),
    ot = require('./ot');

Backend.prototype.graphSubscribe = function(agent, to, options, callback){
  var channel = this.getDocChannel(to.graphName, to.vertex || to.from),
      db = this.db,
      backend = this,
      start = Date.now();

  if (!db) return callback({message: 'DB not found'});
  if (!backend.db.getNeighbors) return callback({message: 'DB.getNeighbors not defined'});

  var request = {
    backend: backend,
    graph: to.graphName,
    vertex: to.vertex,
    from: to.from,
    to: to.to,
    edge: to.edge,
    options: options,
    channel: channel,
    db: backend.db
  };

  backend.pubsub.subscribe(channel, function(err, stream) {
    if (err) return callback(err);

    stream.backend = backend;
    stream.agent = agent;

    var dbCallback = function(err, data) {
      if (err) {
        stream.destroy();
        return callback(err);
      }

      request.data = data;

      var graphEmitter = new GraphEmitter(request, stream);
      backend.emit('timing', 'graphSubscribe', Date.now() - start, request);
      callback(null, graphEmitter, data);
    }

    if (to.vertex) {
      backend.db.getNeighbors(to.graphName, to.vertex, to.edge, options, dbCallback);
    }
    else if (to.from && to.to) {
      backend.db.getEdge(to.graphName, to.from, to.to, to.edge, options, dbCallback);
    }
    else {
      return callback('[k-sync] combination of parameters in graphSubscribe not supported.');
    }    
  });
};

Backend.prototype.graphFetch = function(agent, to, options, callback){
  var db = this.db,
      backend = this,
      start = Date.now();

  if (!db) return callback({message: 'DB not found'});
  if (!backend.db.getNeighbors) return callback({message: 'DB.getNeighbors not defined'});

  var dbCallback = function(err, data) {
    if (err) {
      return callback(err);
    }

    callback(null, data);
  }

  if (to.vertex) {
    backend.db.getNeighbors(to.graphName, to.vertex, to.edge, options, dbCallback);
  }
  else if (to.from && to.to) {
    backend.db.getEdge(to.graphName, to.from, to.to, to.edge, options, dbCallback);
  }
  else {
    return callback('[k-sync] combination of parameters in graphFetch not supported.');
  }
};

// Submit an operation on the named graph/vertex.
//
// callback called with (err, snapshot, ops)
Backend.prototype.submitGop = function(agent, op, callback){
  var err = checkOp(op);
  
  if (err) return callback(err);

  var request = new SubmitGopRequest(this, agent, op);
  var backend = this;

  backend.trigger('submit', agent, request, function(err) {
    backend.trigger('apply', agent, request, function(err) {
      if (err) return callback(err);
      request.run(function(err) {
        if (err) return callback(err);
        backend.trigger('after submit', agent, request, function(err) {
          callback(err);
        });
      });
    });
  });
};

function SubmitGopRequest(backend, agent, op) {
  this.index = op.graph;
  this.op = op;
  this.backend = backend;
  this.backend = backend;
  this.agent = agent;
  if (op.from) {
    this.channels = [backend.getDocChannel(op.graph, op.from), backend.getDocChannel(op.graph, op.to)];
  }
  else {
    this.channels = [backend.getDocChannel(op.graph, op.vertex)];
  }
}

SubmitGopRequest.prototype.run = function(callback){
  var op = this.op;
  // Middleware may silently cancel the submission by clearing request.op

  if (!op) return callback();

  var request = this;
  var backend = this.backend;

  if (op.del) {
    if (op.vertex) {
      backend.db.removeVertex(op.graph, op.vertex, function(err) {
        backend.pubsub.publish(request.channels, op);
        callback(err);
      });
    }
    else {
      backend.db.removeEdge(op.graph, op.from, op.to, op.data, function(err) {
        backend.pubsub.publish(request.channels, op);
        callback(err);
      });
    }
  }
  else if (op.create) {
    backend.db.addEdge(op.graph, op.from, op.to, op.data, function(err) {
      backend.pubsub.publish(request.channels, op);
      callback(err);
    });
  }
  else if (op.set) {
    backend.db.setGraphData(op.graph, op.from, op.to, op.data, function(err) {
      backend.pubsub.publish(request.channels, Object.assign(op, { c: op.graph, d: op.from }));
      backend.pubsub.publish(request.channels, Object.assign(op, { c: op.graph, d: op.to }));
      callback(err);
    });
  }
};

function checkOp(op) {
    if (op.vertex) {
      return;
    }
    
    if (!op.from) {
      return { message: 'Missing from' };
    }
    else if (!op.to) {
      return { message: 'Missing to' };
    }
    else if (op.to === op.from) {
      return { message: 'from and top are the same' };
    }
}
