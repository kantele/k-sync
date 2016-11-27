var Query = require('./query');

module.exports = Notification;

function Notification(type, connection, id, collection, query, options, callback) {
  this.inflightOps = [];

  Query.call(this, type, connection, id, collection, query, options, callback);
}

Notification.prototype = Object.create(Query.prototype)

Notification.prototype.constructor = Notification;


Notification.prototype._handleSubscribe = function(err, data, extra) {
  var callback = this.callback;
  this.callback = null;

  if (err) {
    console.error(err);
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

Notification.prototype._handleFetch = function(err, data, extra) {
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

Notification.prototype._handleDiff = function(diff) {
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

Notification.prototype.submitOp = function(op, callback) {
  // Wait until we have a src id from the server
  var src = this.connection.id;
  if (!src) return;

  // Track data for retrying ops
  op.sentAt = Date.now();
  op.retries = (op.retries == null) ? 0 : op.retries + 1;

  // The src + seq number is a unique ID representing this operation. This tuple
  // is used on the server to detect when ops have been sent multiple times and
  // on the client to match acknowledgement of an op back to the inflightOps.
  // Note that the src could be different from this.connection.id after a
  // reconnect, since an op may still be pending after the reconnection and
  // this.connection.id will change. In case an op is sent multiple times, we
  // also need to be careful not to override the original seq value.
  if (op.seq == null) op.seq = this.connection.seq++;
  if (op.src == null) op.src = src;
  if (op.id == null) op.id = this.id;

  if (callback) {
    op.callback = callback;
  }

  this.inflightOps.push(op);
  this.connection.send(op);
}

Notification.prototype.handleOp = function(op) {
  if (op.diff) this._handleDiff(op.diff);

  // call the callback if there was one
  // remove from inflightOps
  var index = this.inflightOps.findIndex(function(el) { return el.src === op.src && el.seq === op.seq; })
  if (index !== -1) {
    if (this.inflightOps[index].callback) {
      this.inflightOps[index].callback();
    }

    this.inflightOps = [];
  }

  if (this.options.deleteAfterOneSubmit) {
    this.connection._destroyQuery(this);
  }
}

