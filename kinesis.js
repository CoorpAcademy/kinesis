const {request} = require('./lib/kinesis-request');
const KinesisStream = require('./lib/kinesis-stream');

module.exports.stream = function(options) {
  return new KinesisStream(options);
};
module.exports.KinesisStream = KinesisStream;
module.exports.listStreams = listStreams;
module.exports.request = request;

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
