var util = require('./util');
var arraydiff = require('arraydiff');

function PathQueryEmitter(agent, backend, queryId, collection, query, options, callback) {
  this.agent = agent;
  this.backend = backend;
  this.queryId = queryId;
  this.collection = collection;
  this.query = query;
  this.options = options;
  this._subscribeBulk(this.collection, this.options && this.options.fetchOps || this.query, function(err, snapshotMap) {
    callback(null, { data: util.getResultsDataForMap(snapshotMap, query) });
  });
}
module.exports = PathQueryEmitter;

PathQueryEmitter.prototype.destroy = function() {
};

PathQueryEmitter.prototype.updateQuery = function(query, callback) {
  var diff1 = arraydiff(this.query, query);
  // same thing as diff1 but in a slightly different format
  // we could convert diff1 to get a better performance
  var diff2 = arraydiff2(this.query, query);
  var self = this;

  this.query = query;

  function subscribe(cb) {
    if (diff2.inserted.length) {
      self._subscribeBulk(self.collection, diff2.inserted, function(err, snapshotMap) {
        var diff = mapDiff(diff1, snapshotMap);
        self.onDiff(diff);
        cb();
      });
    }
    else { 
      cb();
    }
  };

  function unsubscribe(cb) {
    if (diff2.removed.length) {
      self.agent._unsubscribeBulk(self.collection, diff2.removed, function() {
        var diff = diff1.filter(function(el) { return el instanceof arraydiff.RemoveDiff; })
        self.onDiff(diff);
        cb()
      });
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

PathQueryEmitter.prototype._subscribeBulk = function(collection, versions, callback) {
  var agent = this.agent,
      self = this;

  this.backend.subscribeBulk(this.agent, collection, versions, function(err, streams, snapshotMap) {
    if (err) return console.error(err);
    if (err) return callback(err);
    for (var id in streams) {
      agent._subscribeToStream(self.collection, id, streams[id]);
    }
    if (snapshotMap) {
      callback(null, snapshotMap);
    } else {
      callback();
    }
  });
};

PathQueryEmitter.prototype.onDiff = function(diff) {
  for (var i = 0; i < diff.length; i++) {
    var item = diff[i];
    if (item.type === 'insert') {
      item.values = util.getResultsData(item.values);
    }
  }  
  // Consider stripping the collection out of the data we send here
  // if it matches the query's collection.
  this.agent.send({a: 'q', id: this.queryId, diff: diff});
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

function snapshotMapToArray(snapshotMap) {
  var values = [];
  for (var i in snapshotMap) {
    values.push(snapshotMap[i]);
  }
  return values;
}
