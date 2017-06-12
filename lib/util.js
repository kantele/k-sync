
exports.doNothing = doNothing;
function doNothing() {}

exports.hasKeys = function(object) {
  for (var key in object) return true;
  return false;
};

exports.parseGraphCollection = function(req, projections) {
  if (typeof req.q === 'object' && req.q.$g) {
    var o = {
      graphName: req.q.$g,
    };

    if (req.o) {
      o.options = req.o;
    }

    if (req.q.$d) {
      o.edge = req.q.$d;
    }

    if (req.q.$to) {
      o.to = req.q.$to;
    }

    if (req.q.$from) {
      o.from = req.q.$from;
    }

    if (req.q.$v) {
      o.vertex = req.q.$v;
    }

    return o;
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

exports.getGraphOptions = function(req) {
  return req && req.o || {};
}
