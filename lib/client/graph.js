var Query = require('./query');

module.exports = Graph;

function Graph(type, connection, id, collection, query, options, callback) {
  this.inflightOps = [];
  this.isGraph = true;

  Query.call(this, type, connection, id, collection, query, options, callback);
}

Graph.prototype = Object.create(Query.prototype)

Graph.prototype.constructor = Graph;

Graph.prototype.submitOp = function(op, callback) {
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

Graph.prototype.handleOp = function(op) {
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
}

