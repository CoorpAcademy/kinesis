import test from 'ava';
import kinesis from '..';
import {Duplex, Writable} from 'stream';
import kinesalite from 'kinesalite';

let ports = 4657;

test.cb.beforeEach('setup kinesalite', t => {
  const port = ports++;
  const kinesisServer = kinesalite({ssl: true});
  const kinesisOptions = {host: 'localhost', port};
  const kinesisStreamOptions = Object.assign({}, kinesisOptions, {name: 'test'})
  kinesisServer.listen(kinesisOptions.port, err => {
    if(err) return t.end(err);
    kinesis.request('CreateStream', {StreamName: 'test', ShardCount: 2}, kinesisOptions, err => {
      setTimeout(() => t.end(err), 1000);
      // delai to ensure creation of stream
    })
  });
  t.context = {kinesisServer, kinesisOptions, kinesisStreamOptions};
});

test.cb.afterEach('destroy the kinesalite', t => {
  t.context.kinesisServer.close(t.end)
})

test('I can create a stream with a name', t => {
  const stream = kinesis.stream('name');

  t.true(stream instanceof Duplex);
  t.deepEqual(stream.name, 'name')
});

test.cb('I can write to a kinesis stream', t => {
  const kinesisStream = kinesis.stream(t.context.kinesisStreamOptions);
  kinesisStream.write({PartitionKey: '12', Data: new Buffer('Hello Stream')}, null, t.end)
})

test.cb('I can read from a kinesis stream', t => {
  const writeableKinesisStream = kinesis.stream(t.context.kinesisStreamOptions);
  const kinesisStream = kinesis.stream(t.context.kinesisStreamOptions);

  var confirmRead = new Writable({objectMode: true})
  confirmRead._write = function(chunk, encoding, cb) {
    t.deepEqual(chunk.PartitionKey, '12')
    t.end()
    cb()
  }
  writeableKinesisStream.write({PartitionKey: '12', Data: new Buffer('Hello Stream')});
  writeableKinesisStream.write({PartitionKey: '12', Data: new Buffer('Hello Stream')});
  // note: normaly one should be enought but for strange reason probably linked to concurrency
  // two are needed to trigger the write
  kinesisStream.pipe(confirmRead)
})

// FIXME: there seems to be problem with writeConcurrency
