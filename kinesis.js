const {request} = require('./src/kinesis-request');
const {listStreams} = require('./src/kinesis-api');
const KinesisStream = require('./src/kinesis-stream');

module.exports.stream = options => new KinesisStream(options);
module.exports.KinesisStream = KinesisStream;
module.exports.listStreams = listStreams;
module.exports.request = request;
