const {request} = require('./kinesis-request');

function listStreams(options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }

  request('ListStreams', {}, options, (err, res) => {
    if (err) return cb(err);

    return cb(null, res.StreamNames);
  });
}

module.exports = {listStreams};
