const {request} = require('./lib/kinesis-request');
const {listStreams} = require('./lib/kinesis-api');
const KinesisStream = require('./lib/kinesis-stream');

module.exports.stream = options => new KinesisStream(options);
module.exports.KinesisStream = KinesisStream;
module.exports.listStreams = listStreams;
module.exports.request = request;
