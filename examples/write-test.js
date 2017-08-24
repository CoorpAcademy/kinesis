const {Readable} = require('stream');
const kinesis = require('..');

require('https').globalAgent.maxSockets = Infinity;

const readable = new Readable({objectMode: true});
readable._read = function() {
  for (let i = 0; i < 100; i++) this.push({PartitionKey: i.toString(), Data: Buffer.from('a')});
  this.push(null);
};

const kinesisStream = kinesis.stream({name: 'test', writeConcurrency: 5});

readable.pipe(kinesisStream).on('end', function() {
  console.log('done');
});
