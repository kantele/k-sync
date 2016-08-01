function deepAssign(o) {
  var res = Object.assign({}, o);

  for (var i in o) {
    if (typeof o[i] === 'object') {
      res[i] = deepAssign(o[i]);
    }
  }

  return res;
}

module.exports = deepAssign;

