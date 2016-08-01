var emitter = require('../emitter');

module.exports = RPC;
function RPC(name, params, connection, callback) {
  emitter.EventEmitter.call(this);

  this.name = name;
  this.params = params;
  this.connection = connection;
  this.callback = callback;
  
  this.send();
}
emitter.mixin(RPC);

RPC.prototype.handle = function(err, data) {
  if (err) {
    this.connection.emit('rpc-error', err.message || err, this.callback);
  }
  else {
    // data.contexts = JSON.parse(data.contexts);
    this.connection.emit('rpc-bundle', data, this.callback);
  }
};

RPC.prototype.send = function() {
  this.connection.send({ a: 'rpc', n: this.name, p: this.params });
};
