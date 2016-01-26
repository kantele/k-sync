
exports.doNothing = doNothing;
function doNothing() {}

exports.hasKeys = function(object) {
  for (var key in object) return true;
  return false;
};

exports.parseGraphCollection = function(req, projections) {
  if (req.c && typeof req.q === 'object' && req.q.$g && req.q.$v) {
    return {
      method: 'neighbors',
      collection: req.c,
      graphName: req.q.$g,
      vertex: req.q.$v,
      options: req.q.$o
    };
  }
};

exports.getResultsData = function(results) {
  var items = [];
  var lastType = null;
  if (results) {
    for (var i = 0; i < results.length; i++) {
      var result = results[i];
      var item = {
        d: result.id,
        v: result.v,
        data: result.data
      };
      if (lastType !== result.type) {
        lastType = item.type = result.type;
      }
      items.push(item);
    }
  }
  return items;
};

exports.getQueryFromGraphData = function(data) {
  return { _id: { $in: data }};
};

exports.queryChanged = function(q1, q2) {
  if (!q1._id || !q2._id) {
    return true;
  }

  if (!q1._id.$in || !q2._id.$in) {
    return true;
  }

  if (q1._id.$in.length !== q2._id.$in.length) {
    return true;
  }

  for (var i = 0; i < q1._id.$in.length; i++) {
    var item = q1._id.$in[i];

    if (q2._id.$in.indexOf(item) === -1) {
      return true;
    }
  }

  return false;
};

exports.getGraphOptions = function(req) {
  return req && req.q && req.q.$o || {};
}
