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
      if (data.set) {
        emitter.updateOneItem(data);
      }
      else {
        emitter.queryPoll(data);
      }
    }
  }
  readStream();
  emitter.stream.on('readable', readStream);
};

GraphEmitter.prototype._emitTiming = function(action, start){
  this.backend.emit('timing', action, Date.now() - start, this.index, this.query);
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

// only one item has been changed with model.graph.setData()
// we will update it here
GraphEmitter.prototype.updateOneItem = function(data) {
  var emitter = this;

  // create a duplicate array to which we compare the new array and send the difference to client
  var dataold = this.data.slice();

  this.backend.db.getEdge(this.graph, data.from, data.to, null, { direction: 'outbound', returnvertex: 'start' } , function(err, data) {

    // the result comes in array, we need just the one item (todo: change getEdge)
    if (data && data.length) {
      data = data.pop();
    }

    // replace the one item in emitter.data that we just got
    for (var i = 0; i < emitter.data.length; i++) {
      if (data.d == emitter.data[i].d) {
        emitter.data[i] =  Object.assign({}, emitter.data[i], { data: data.data });
      }
    }

    var idsDiff = arraydiff(dataold, emitter.data, function(a, b) {
      var idsMatch = (a && b && a.d === b.d);

      if (!idsMatch) {
        return false;
      }

      if (a.data) {
        for (k in a.data) {
          // console.log('a.data[k]', a.data[k])
          // console.log('b.data[k]', b.data[k])
          if (!b.data || a.data[k] !== b.data[k]) {
            return false;
          }
        }
      }

      return true;
    });

    if (idsDiff.length) {
      emitter.onDiff(idsDiff);
    }
  });
};

GraphEmitter.prototype.queryPoll = function(data) {
  var emitter = this,
      self = this;

  var start = Date.now(),
      emitter = this;

  function arrayEquals(a1, a2) {
    // console.log('')
    // console.log('a1', a1)
    // console.log('a2', a2)
    if (a1 && a2 && a1.length == a2.length) {
      for (var i = 0; i < a1.length; i++) {
        // check for "id"
        if (a1[i].d !== a2[i].d) {
          return false;
        }

        // check that data matches
        if (a1[i].data && a2[i].data) {
          for (k in a1[i].data) {
            if (a1[i].data[k] !== a2[i].data[k]) {
              return false;
            }
          }
        }
      }

      return true;
    }

    return false;
  }

  var dbCallback = function(err, data) {
    if (err) {
      console.log(err);
      return emitter._finishPoll(err);
    }

    // check by the 'id' and the containing data
    var idsDiff = arraydiff(emitter.data, data, function(a, b) {
      var idsMatch = (a && b && a.d === b.d);

      if (!idsMatch) {
        return false;
      }

      for (k in a.data) {
        if (!b.data || a.data[k] !== b.data[k]) {
          return false;
        }
      }

      return true;
    });

    if (idsDiff.length) {
      emitter.data = data;
      emitter.onDiff(idsDiff);
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


function deepCopy(o) {
  var res;

  if (typeof o === 'string') {
    res = o;
  }
  // object
  else {
    res = Object.assign({}, o);

    for (var i in o) {
      if (Array.isArray(o[i])) {
        res[i] = o[i].slice(0);
      }
      else if (typeof o[i] === 'object') {
        res[i] = deepCopy(o[i]);
      }
    }
  }

  return res;
}
