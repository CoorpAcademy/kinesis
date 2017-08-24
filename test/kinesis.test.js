import test from 'ava';
import kinesis from '..';
import {Duplex} from 'stream';
import kinesalite from 'kinesalite';

let kinesisServer;
const kinesisOptions = {host: 'localhost', port: '5678'};

test.cb.before('set up the kinesalite', t => {
  kinesisServer = kinesalite({ssl: true});
  kinesisServer.listen(5678, t.end)
})

test('I can create a stream with a name', t => {
  const stream = kinesis.stream('name');

  t.true(stream instanceof Duplex);
  t.deepEqual(stream.name, 'name')
});

test.cb('I can list stream from a kinesis', t => {
  kinesis.listStreams(kinesisOptions, (err, streams) => {
    t.is(err, null);
    t.deepEqual(streams, [])
    t.end()
  })
})

test.cb('I can list create a stream in the kinesis', t => {
  kinesis.request('CreateStream', {StreamName: 'test', ShardCount: 2}, kinesisOptions, err => {
    t.is(err, null);
    kinesis.listStreams(kinesisOptions, (err, streams) => {
      t.is(err, null);
      t.deepEqual(streams, ['test'])
      t.end()
    })
  })
})

test.todo('I can write to a kinesis stream')
test.todo('I can read to a kinesis stream')
