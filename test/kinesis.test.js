import {Duplex, Writable} from 'stream';
import test from 'ava';
import kinesalite from 'kinesalite';
import kinesis from '../kinesis';

let ports = 4657;

test.beforeEach.cb('setup kinesalite', t => {
  const port = ports++;
  const kinesisServer = kinesalite({ssl: true});
  const kinesisOptions = {host: 'localhost', port};
  const kinesisStreamOptions = Object.assign({}, kinesisOptions, {name: 'test'});
  kinesisServer.listen(kinesisOptions.port, err => {
    if (err) return t.end(err);
    kinesis.request('CreateStream', {StreamName: 'test', ShardCount: 2}, kinesisOptions, errr => {
      setTimeout(() => t.end(errr), 1000);
      // delai to ensure creation of stream
    });
  });
  t.context = {kinesisServer, kinesisOptions, kinesisStreamOptions};
});

test.afterEach.cb('destroy the kinesalite', t => {
  t.context.kinesisServer.close(t.end);
});

test('I can create a stream with a name', t => {
  const stream = kinesis.stream('name');

  t.true(stream instanceof Duplex);
  t.deepEqual(stream.name, 'name');
});

test.cb('I can write to a kinesis stream', t => {
  const kinesisStream = kinesis.stream(t.context.kinesisStreamOptions);
  kinesisStream.write({PartitionKey: '12', Data: Buffer.from('Hello Stream')}, null, t.end);
});

test.cb('I can write to a stream configured via endpoint', t => {
  const {host, port} = t.context.kinesisOptions;
  const kinesisOptions = {endpoint: `https://${host}:${port}`};
  const kinesisStreamOptions = Object.assign({}, kinesisOptions, {name: 'test'});
  const kinesisStream = kinesis.stream(kinesisStreamOptions);
  kinesisStream.write(
    {PartitionKey: '12', Data: Buffer.from('Hello endpoint stream Stream')},
    null,
    t.end
  );
});

test.cb('I can read from a kinesis stream', t => {
  const writeableKinesisStream = kinesis.stream(t.context.kinesisStreamOptions);
  const kinesisStream = kinesis.stream(t.context.kinesisStreamOptions);

  const confirmRead = new Writable({objectMode: true});
  confirmRead._write = function(chunk, encoding, cb) {
    t.deepEqual(chunk.PartitionKey, '12');
    return cb() + t.end();
  };
  writeableKinesisStream.write({PartitionKey: '12', Data: Buffer.from('Hello Stream')});
  writeableKinesisStream.write({PartitionKey: '12', Data: Buffer.from('Hello Stream')});
  // note: normaly one should be enought but for strange reason probably linked to concurrency
  // two are needed to trigger the write
  kinesisStream.pipe(confirmRead);
});

// FIXME: there seems to be problem with writeConcurrency
