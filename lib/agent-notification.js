var Agent = require('./agent'),
    util = require('./util'),
    QueryEmitter = require('./query-emitter'),
    PathNotificationEmitter = require('./pathnotification-emitter');

Agent.prototype._pathNotificationSubscribe = function(queryId, collection, ids, options, callback) {
  var emitter = this.getExistingEmitter(queryId);

  if (emitter) {
    emitter.updateQuery(ids, callback);
  }
  else {
    this.subscribedNotifications[queryId] = new PathNotificationEmitter(this, this.backend, queryId, collection, ids, callback)
  }
}

Agent.prototype._notificationSubscribe = function(req, callback){
  var collection = req.c,
      id = req.q.$i,
      queryId = req.id,
      options = {},
      agent = this;

  if (req.o && req.o.deleteAfterOneSubmit) {
    return callback(null, {});
  }

  var previous = this.getExistingEmitter(queryId, options);
  if (previous) options.emitter = previous;

  if (Array.isArray(id)) {
    this._pathNotificationSubscribe(queryId, collection, id, options, callback);
  }
  else {
    this.backend.notificationSubscribe(this, collection, id, options, function(err, emitter, results) {
      if (err) return callback(err);

      agent._subscribeToNotification(emitter, queryId, collection, id);

      var message = {
        data: results
      };

      callback(null, message);
    });
  }
};

Agent.prototype._notificationUnsubscribe = function(req, callback){
  var emitter = this.subscribedNotifications[req.id];
  if (emitter) {
    emitter.destroy();
    delete this.subscribedNotifications[req.id];
  }
  process.nextTick(callback);
};

Agent.prototype._notificationFetch = function(req, callback){
  var collection = req.c,
      id = req.q.$i,
      queryId = req.id;

  var message = {
    data: []
  };

  callback(null, message);
};

Agent.prototype._subscribeToNotification = function(emitter, queryId, collection, id){
  if (this.closed) return emitter.destroy();

  var previous = this.subscribedNotifications[queryId];
  if (previous) previous.destroy();
  var agent = this;

  emitter.onDiff = function(diff) {
    agent.send({a: 'nop', id: queryId, diff: diff});
  };

  emitter.onError = function(err) {
    // Log then silently ignore errors in a subscription stream, since these
    // may not be the client's fault, and they were not the result of a
    // direct request by the client
    console.error('Notification subscription stream error', collection, id, err);
  };

  this.subscribedNotifications[queryId] = emitter;
};

Agent.prototype._submitNop = function(req, callback) {
  var op = new NotificationOp(req);
  var agent = this;
  this.backend.submitNop(this, op, function(err, ops) {
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

function NotificationOp(req) {
  this.type = 'nop';
  this.c = req.c;
  this.d = req.index;
  this.data = req.data;
  this.seq = req.seq;
  this.src = req.src;
}
