var GraphEmitter = require('./graph-emitter'),
    Backend = require('./backend'),
    util = require('./util'),
    ot = require('./ot');

Backend.prototype.graphSubscribe = function(agent, to, options, callback){
  var channel = this.getDocChannel(to.graphName, to.vertex),
      db = this.db,
      backend = this,
      start = Date.now();

  if (!db) return callback({message: 'DB not found'});
  if (!backend.db.getEdges) return callback({message: 'DB.getEdges not defined'});

  var request = {
    backend: backend,
    graph: to.graphName,
    vertex: to.vertex,
    edge: to.edge,
    options: options,
    channel: channel,
    db: backend.db
  };

  backend.pubsub.subscribe(channel, function(err, stream) {
    if (err) return callback(err);

    stream.backend = backend;
    stream.agent = agent;

    backend.db.getEdges(to.graphName, to.vertex, to.edge, options, function(err, data) {
      if (err) {
        stream.destroy();
        return callback(err);
      }

      request.data = data;

      var graphEmitter = new GraphEmitter(request, stream);
      backend.emit('timing', 'graphSubscribe', Date.now() - start, request);
      callback(null, graphEmitter, data);
    });
  });
};

Backend.prototype.graphFetch = function(agent, to, options, callback){
  var channel = this.getDocChannel(to.graphName, to.vertex),
      db = this.db,
      backend = this,
      start = Date.now();

  if (!db) return callback({message: 'DB not found'});
  if (!backend.db.getEdges) return callback({message: 'DB.getEdges not defined'});

  backend.db.getEdges(to.graphName, to.vertex, to.edge, options, function(err, data) {
    if (err) {
      return callback(err);
    }

    callback(null, data);
  });
};

// Submit an operation on the named graph/vertex.
//
// callback called with (err, snapshot, ops)
Backend.prototype.submitGop = function(agent, op, callback){
  var err = ot.checkOp(op) || checkOp(op);
  if (err) return callback(err);

  var request = new SubmitGopRequest(this, agent, op);
  var backend = this;

  backend.trigger('submit', agent, request, function(err) {
    if (err) return callback(err);
    request.run(function(err) {
      if (err) return callback(err);
      backend.trigger('after submit', agent, request, function(err) {
        callback(err);
      });
    });
  });
};

function SubmitGopRequest(backend, agent, op) {
  this.op = op;
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
