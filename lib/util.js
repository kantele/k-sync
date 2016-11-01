
exports.doNothing = doNothing;
function doNothing() {}

exports.hasKeys = function(object) {
  for (var key in object) return true;
  return false;
};

exports.parseGraphCollection = function(req, projections) {
  if (typeof req.q === 'object' && req.q.$g) {
    if (req.q.$v) {
      var o = {
        graphName: req.q.$g,
        vertex: req.q.$v,
        options: req.q.$o
      };

      if (req.q.$d) {
        o.edge = req.q.$d;
      }

      return o;
    }
    else {
      return {
        graphName: req.q.$g
      };
    }
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
  return req && req.q && req.q.$o || {};
}
