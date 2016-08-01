var Graph = require('./graph');

module.exports = Notification;

function Notification(type, connection, id, collection, query, options, callback) {
  this.inflightOps = null;
  this.pendingOps = [];

  Graph.call(this, type, connection, id, collection, query, options, callback);
}

Notification.prototype = Object.create(Graph.prototype)

Notification.prototype.constructor = Notification;

