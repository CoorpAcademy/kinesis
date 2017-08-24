const {Writable} = require('stream');
const kinesis = require('..');

require('https').globalAgent.maxSockets = Infinity;

const consoleOut = new Writable({objectMode: true});
consoleOut._write = function(chunk, encoding, cb) {
  chunk.Data = chunk.Data.slice(0, 10);
  console.dir(chunk);
  cb();
};

const kinesisStream = kinesis.stream({name: 'test', oldest: true});

kinesisStream.pipe(consoleOut);
