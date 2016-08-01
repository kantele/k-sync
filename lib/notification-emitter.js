var deepEquals = require('deep-is');
var arraydiff = require('arraydiff');
var util = require('./util');

function NotificationEmitter(request, stream) {
  this.backend = request.backend;
  this.agent = request.agent;
  this.id = request.id;
  this.collection = request.collection;
  this.stream = stream;
  this.data = [];

  this._open();
}
module.exports = NotificationEmitter;

// Start processing events from the stream
NotificationEmitter.prototype._open = function() {
  var emitter = this;
  this._defaultCallback = function(err) {
    if (err) emitter.onError(err);
  }
  emitter.stream.on('data', function(data) {
    if (!data || data.error) {
      return emitter.onError(data.error);
    }
    if (data && data.data) {
      emitter._update(data.data);
    }
  });
  emitter.stream.on('end', function() {
    emitter.destroy();
  });
};

NotificationEmitter.prototype.destroy = function() {
  this.stream.destroy();
};

NotificationEmitter.prototype._update = function(data) {
  var idsDiff1 = arraydiff(this.data, []),
      idsDiff2 = arraydiff([], [ data ]),
      idsDiff = idsDiff1.concat(idsDiff2);

  this.data = [ data ];

  if (idsDiff.length) {
    this.onDiff(idsDiff);
  }
};

// Clients must assign each of these functions syncronously after constructing
// an instance of NotificationEmitter. The instance is subscribed to an op stream at
// construction time, and does not buffer emitted events. Diff events assume
// all messages are received and applied in order, so it is critical that none
// are dropped.
NotificationEmitter.prototype.onError =
NotificationEmitter.prototype.onDiff =
NotificationEmitter.prototype.onExtra =
NotificationEmitter.prototype.onOp = function() {
  throw new Error('Required NotificationEmitter listener not assigned');
};

