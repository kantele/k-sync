var util = require('./util');
var arraydiff = require('arraydiff');
var async = require('async');

function PathNotificationEmitter(agent, backend, queryId, collection, query, callback) {
  this.agent = agent;
  this.backend = backend;
  this.queryId = queryId;
  this.collection = collection;
  this.query = query;
  this.data = [];
  this.streams = {};
  this._subscribeBulk(query, callback);
}
module.exports = PathNotificationEmitter;

PathNotificationEmitter.prototype.destroy = function() {
  for (var id in this.streams) {
    this.streams[id].destroy();
    delete this.streams[id];
  }
};

PathNotificationEmitter.prototype.updateQuery = function(query, callback) {
  var diff1 = arraydiff(this.query, query);
  // same thing as diff1 but in a slightly different format
  // we could convert diff1 to get a better performance
  var diff2 = arraydiff2(this.query, query);
  var self = this;

  this.query = query;

  function subscribe(cb) {
    if (diff2.inserted.length) {
      self._subscribeBulk(diff2.inserted, cb);
    }
    else cb();
  };

  function unsubscribe(cb) {
    if (diff2.removed.length) {
      self._unsubscribeBulk(diff2.removed, cb);
    }
    else {
      cb();
    }
  }

  unsubscribe(function() {
    subscribe(function(err, data) {
      callback(null, data);
    });
  })
};

PathNotificationEmitter.prototype._unsubscribeBulk = function(ids, callback) {
  if (ids) {
    for (var i = 0; i < ids.length; i++) {
      if (this.streams[ids[i]]) {
        this.streams[ids[i]].destroy();
        delete this.streams[ids[i]];
      }
    }
  }
}

PathNotificationEmitter.prototype._subscribeBulk = function(ids, callback) {
  var agent = this.agent,
      backend = this.backend,
      self = this;

  async.each(ids, function(id, eachCb) {
    var channel = backend.getDocChannel(self.collection, id);
    backend.pubsub.subscribe(channel, function(err, stream) {
      if (err) return eachCb(err);
  
      stream.on('data', self._update.bind(self));

      stream.on('end', function() {
        delete self.streams[id];
      });

      self.streams[id] = stream;
      eachCb();
    });
  }, callback);
};

PathNotificationEmitter.prototype._update = function(data) {
  if (!data || data.error) {
    return emitter.onError(data.error);
  }
  if (data.data) {
    var idsDiff1 = arraydiff(this.data, []),
        idsDiff2 = arraydiff([], [ data.data ]),
        idsDiff = idsDiff1.concat(idsDiff2);

    this.data = [ data.data ];

    if (idsDiff.length) {
      this.onDiff(idsDiff);
    }
  }
}

PathNotificationEmitter.prototype.onDiff = function(diff) {
  // Consider stripping the collection out of the data we send here
  // if it matches the query's collection.
  this.agent.send({a: 'nop', id: this.queryId, diff: diff});
};

// calculate a difference (inserted and removed) of two arrays
function arraydiff2(a, b) {
  var removed = a.slice(),
      inserted = b.slice();

  for (var i = removed.length - 1; i >= 0; i--) {
    var indexInserted = inserted.indexOf(removed[i]);
    if (indexInserted >= 0) {
      removed.splice(i, 1);
      inserted.splice(indexInserted, 1);
    }
  }

  return { inserted: inserted, removed: removed };
}

function mapDiff(idsDiff, snapshotMap) {
  var diff = [];
  for (var i = 0; i < idsDiff.length; i++) {
    var item = idsDiff[i];
    if (item instanceof arraydiff.InsertDiff) {
      var values = [];
      for (var j = 0; j < item.values.length; j++) {
        var id = item.values[j];
        values.push(snapshotMap[id]);
      }
      diff.push(new arraydiff.InsertDiff(item.index, values));
    } else {
      diff.push(item);
    }
  }
  return diff;
}
