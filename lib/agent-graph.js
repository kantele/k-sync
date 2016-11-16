var Agent = require('./agent'),
    util = require('./util');

Agent.prototype._graphSubscribe = function(req, callback){
  var parsed = util.parseGraphCollection(req, this.backend.projections),
      agent = this;

  if (!parsed) {
    return callback({ message: 'Graph could not be parsed: '+req.c })
  }

  // there is no vertex, we will not actually subscribe to anything, this will
  // act only as a "connection"
  if (!parsed.vertex) {
    return callback(null, {});
  }

  // Subscribe to a query. The client is sent the query results and its
  // notified whenever there's a change
  var queryId = req.id,
      options = util.getGraphOptions(req);

  this.backend.graphSubscribe(this, parsed, options, function(err, graphEmitter, results) {
    if (err) return callback(err);

    agent._subscribeToGraph(graphEmitter, queryId, parsed.graphName, parsed.vertex);

    var message = {
      data: results
    };

    callback(null, message);
  });
};

Agent.prototype._graphUnsubscribe = function(req, callback){
  var emitter = this.subscribedGraphs[req.id];
  if (emitter) {
    emitter.destroy();
    delete this.subscribedGraphs[req.id];
  }
  process.nextTick(callback);
};

Agent.prototype._subscribeToGraph = function(emitter, queryId, graphName, vertex){
  if (this.closed) return emitter.destroy();

  var previous = this.subscribedGraphs[queryId];
  if (previous) previous.destroy();
  var agent = this;

  emitter.onDiff = function(diff) {
    // Consider stripping the graphName out of the data we send here
    // if it matches the query's graphName.
    agent.send({a: 'q', id: queryId, diff: diff});
  };

  emitter.onError = function(err) {
    // Log then silently ignore errors in a subscription stream, since these
    // may not be the client's fault, and they were not the result of a
    // direct request by the client
    console.error('Graph subscription stream error', graphName, vertex, err);
  };

  this.subscribedGraphs[queryId] = emitter;
};

Agent.prototype._graphFetch = function(req, callback){
  var parsed = util.parseGraphCollection(req, this.backend.projections),
      agent = this;

  if (!parsed) {
    return callback({ message: 'Graph operation could not be parsed: ' + req })
  }

  var queryId = req.id,
      options = util.getGraphOptions(req);

  this.backend.graphFetch(this, parsed, options, function(err, results, extra) {
    if (err) return callback(err);

    message = {
      data: results,
      extra: extra
    };

    callback(null, message);
  });
};

Agent.prototype._submitGop = function(req, callback){
  var op = new GraphOp(req);
  var agent = this;
  this.backend.submitGop(this, op, function(err, ops) {
    // Message to acknowledge the op was successfully submitted
    var ack = { src: op.src, seq: op.seq };
    if (err) {
      // Occassional 'Op already submitted' errors are expected to happen as
      // part of normal operation, since inflight ops need to be resent after
      // disconnect. In this case, ack the op so the client can proceed
      if (err.code === 4001) return callback(null, ack);
      return callback(err);
    }

    callback(null, ack);
  });
};

function GraphOp(req) {
  this.type = 'gop';
  this.graph = req.c;

  if (req.vertex) {
    this.vertex = req.vertex;
  } 
  else {
    this.from = req.from;
    this.to = req.to;
  }

  // there may be a data object associated with a graph edge
  if (req.data) {
    this.data = req.data;
  }
  
  this.seq = req.seq;
  this.src = req.src;

  if (req.del) {
    this.del = true;
  }
  else if (req.create) {
    this.create = true;
  }
  else if (req.get) {
    this.get = true;
  }
}
