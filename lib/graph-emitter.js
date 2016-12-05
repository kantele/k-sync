var deepEquals = require('deep-is');
var arraydiff = require('arraydiff');
var util = require('./util');

function GraphEmitter(request, stream, snapshots, extra) {
  this.backend = request.backend;
  this.agent = request.agent;
  this.db = request.db;
  this.index = request.index;
  this.query = request.query;
  this.graph = request.graph;
  this.vertex = request.vertex;
  this.to = request.to;
  this.from = request.from;
  this.edge = request.edge;
  this.fields = request.fields;
  this.data = request.data;
  this.options = request.options || {};
  this.snapshotProjection = request.snapshotProjection
  this.stream = stream;
  this.extra = extra;
  this.skipPoll = this.options.skipPoll || util.doNothing;

  this.pollDebounce =
    (this.options.pollDebounce != null) ? this.options.pollDebounce :
    (this.db.pollDebounce != null) ? this.db.pollDebounce : 0;

  this._polling = false;
  this._pollAgain = false;
  this._pollTimeout = null;

  this.startStream();
}
module.exports = GraphEmitter;

GraphEmitter.prototype.destroy = function(){
  this.stream.destroy();
};

GraphEmitter.prototype.setQueryEmitter = function(queryEmitter){
  this.queryEmitter = queryEmitter;
};

GraphEmitter.prototype.updateQueryEmitterQuery = function(query){
  if (this.queryEmitter) {
    this.queryEmitter.updateQuery(query);
  }
};

GraphEmitter.prototype.startStream = function(){
  var emitter = this;
  function readStream() {
    var data;
    while (data = emitter.stream.read()) {
      if (data.error) {
        console.error('Error in query op stream:', emitter.index, emitter.query);
        this.emitError(data.error);
        continue;
      }
      emitter.update(data);
    }
  }
  readStream();
  emitter.stream.on('readable', readStream);
};

GraphEmitter.prototype._emitTiming = function(action, start){
  this.backend.emit('timing', action, Date.now() - start, this.index, this.query);
};

GraphEmitter.prototype.update = function(op){
  var id = op.d;
  // Ignore if the user or database say we don't need to poll
  try {
    if (this.skipPoll(null, id, op, this.query)) return;
    if (this.db.skipPoll(null, id, op, this.query)) return;
  } catch (err) {
    console.error('Error evaluating skipPoll:', null, id, op, this.query);
    return this.emitError(err);
  }

  this.queryPoll();
};

GraphEmitter.prototype._flushPoll = function(){
  if (this._polling || this._pollTimeout) return;
  if (this._pollAgain) this.queryPoll();
};

GraphEmitter.prototype._finishPoll = function(err){
  this._polling = false;
  if (err) this.emitError(err);
  this._flushPoll();
};

GraphEmitter.prototype.queryPoll = function(){
  var emitter = this,
      self = this;

  // Only run a single polling check against mongo at a time per emitter. This
  // matters for two reasons: First, one callback could return before the
  // other. Thus, our result diffs could get out of order, and the clients
  // could end up with results in a funky order and the wrong results being
  // removed from the query. Second, only having one query executed
  // simultaneously per emitter will act as a natural adaptive rate limiting
  // in case the db is under load.
  //
  // This isn't neccessary for the document polling case, since they operate
  // on a given id and won't accidentally modify the wrong doc. Also, those
  // queries should be faster and we have to run all of them eventually, so
  // there is less benefit to load reduction.
  if (this._polling || this._pollTimeout) {
    console.log('************************', this._polling? 'polling': '', this._pollTimeout? '_pollTimeout': '');
    this._pollAgain = true;
    return;
  }

  this._polling = true;
  this._pollAgain = false;
  if (this.pollDebounce) {
    this._pollTimeout = setTimeout(function() {
      emitter._pollTimeout = null;
      emitter._flushPoll();
    }, this.pollDebounce);
  }

  var start = Date.now(),
      emitter = this;

  function arrayEquals(a1, a2) {
    if (a1 && a2 && a1.length == a2.length) {
      for (var i = 0; i < a1.length; i++) {
        if (a1[i].d !== a2[i].d) {
          return false;
        }
      }

      return true;
    }
  }

  var dbCallback = function(err, data) {
    if (err) {
      return emitter._finishPoll(err);
    }

    if (!arrayEquals(emitter.data, data)) {
      var idsDiff = arraydiff(emitter.data, data, function(a, b) { return (a && b && a.d === b.d); });

      if (idsDiff.length) {
        emitter.data = data;
        emitter.onDiff(idsDiff);
      }
    }

    emitter._finishPoll(err);
  };

  if (this.vertex) {
    this.backend.db.getNeighbors(this.graph, this.vertex, this.edge, this.options, dbCallback);
  }
  else {
    this.backend.db.getEdge(this.graph, this.from, this.to, this.edge, this.options, dbCallback);
  }
};

// Emit functions are called in response to operation events
GraphEmitter.prototype.emitError = function(err){
  this.onError(err);
};

GraphEmitter.prototype.emitDiff = function(diff){
};

GraphEmitter.prototype.emitExtra = function(extra){
};

GraphEmitter.prototype.emitOp = function(op){
};

// Clients should define these functions
GraphEmitter.prototype.onError =
GraphEmitter.prototype.onDiff =
GraphEmitter.prototype.onExtra =
GraphEmitter.prototype.onOp = function() {
  // Silently ignore if the op stream was destroyed already
  if (!this.stream.open) return;
  throw new Error('Required QueryEmitter listener not assigned');
};
