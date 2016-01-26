var Query = require('./query');

module.exports = Graph;

function Graph(type, connection, id, collection, query, options, callback) {
  this.inflightOps = null;
  this.pendingOps = [];

  Query.call(this, type, connection, id, collection, query, options, callback);
}

Graph.prototype = Object.create(Query.prototype)

// Graph.prototype._executeOrig = Query.prototype._execute;

Graph.prototype.constructor = Graph;

//Graph.prototype._handleSubscribeOrig = Query.prototype._handleSubscribe;

Graph.prototype._handleSubscribe = function(err, data, extra) {
  var callback = this.callback;
  this.callback = null;

  if (err) {
    // Cleanup the query if the initial subscribe returns an error
    this.connection._destroyQuery(this);
    this.emit('ready');
    if (callback) return callback(err);
    return this.emit('error', err);
  }
  // Subscribe will only return results if issuing a new query without
  // previous results. On a resubscribe, changes to the results or ops will
  // have already been sent as individual diff events
  if (data) {
    this.results = data;
    this.extra = extra
  }
  if (callback) callback(null, this.results, this.extra);
  this.emit('ready');
}

Graph.prototype._handleFetch = function(err, data, extra) {
  var callback = this.callback;
  this.callback = null;
  // Once a fetch query gets its data, it is destroyed.
  this.connection._destroyQuery(this);
  if (err) {
    this.emit('ready');
    if (callback) return callback(err);
    return this.emit('error', err);
  }
  if (callback) callback(null, data, extra);
  this.emit('ready');
};

Graph.prototype._handleDiff = function(diff) {
  // Query diff data (inserts and removes)
  if (diff) {
    for (var i = 0; i < diff.length; i++) {
      var d = diff[i];
      switch (d.type) {
        case 'insert':
          var newDocs = d.values;
          Array.prototype.splice.apply(this.results, [d.index, 0].concat(newDocs));
          this.emit('insert', newDocs, d.index);
          break;
        case 'remove':
          var howMany = d.howMany || 1;
          var removed = this.results.splice(d.index, howMany);
          this.emit('remove', removed, d.index);
          break;
        case 'move':
          var howMany = d.howMany || 1;
          var docs = this.results.splice(d.from, howMany);
          Array.prototype.splice.apply(this.results, [d.to, 0].concat(docs));
          this.emit('move', docs, d.from, d.to);
          break;
      }
    }
  }
};

Graph.prototype.submitOp = function(op) {
  this.pendingOps.push(op);
  this._submitOp();
}

Graph.prototype.handleGop = function(op) {
  this.inflightOps = null;
}

Graph.prototype._submitOp = function() {
  if (this.sent && (this.inflightOps || this.pendingOps.length)) {
    // Wait until we have a src id from the server
    var src = this.connection.id;
    if (!src) return;

    // When there is no inflightOps, send the first item in pendingOps. If
    // there is inflightOps, try sending it again
    if (!this.inflightOps) {
      // Send first pending op
      this.inflightOps = this.pendingOps.shift();
    }

    var data = this.inflightOps;

    // should not happen
    if (!data) {
      return;
    }

    // Track data for retrying ops
    data.sentAt = Date.now();
    data.retries = (data.retries == null) ? 0 : data.retries + 1;

    // The src + seq number is a unique ID representing this operation. This tuple
    // is used on the server to detect when ops have been sent multiple times and
    // on the client to match acknowledgement of an op back to the inflightOps.
    // Note that the src could be different from this.connection.id after a
    // reconnect, since an op may still be pending after the reconnection and
    // this.connection.id will change. In case an op is sent multiple times, we
    // also need to be careful not to override the original seq value.
    if (data.seq == null) data.seq = this.connection.seq++;
    if (data.src == null) data.src = src;
    if (data.id == null) data.id = this.id;

    this.connection.send(data);
  }
}
