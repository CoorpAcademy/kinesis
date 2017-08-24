import test from 'ava';
import kinesis from '..';
import {Duplex} from 'stream';
import kinesalite from 'kinesalite';

let ports = 4657;

test.cb.beforeEach('setup kinesalite', t => {
  const port = ports++;
  const kinesisServer = kinesalite({ssl: true});
  const kinesisOptions = {host: 'localhost', port};
  kinesisServer.listen(kinesisOptions.port, err => {
    if(err) return t.end(err);
    kinesis.request('CreateStream', {StreamName: 'test', ShardCount: 2}, kinesisOptions, t.end)
  });
  t.context = {kinesisServer, kinesisOptions};
});

test.cb.afterEach('destroy the kinesalite', t => {
  t.context.kinesisServer.close(t.end)
})

test('I can create a stream with a name', t => {
  const stream = kinesis.stream('name');

  t.true(stream instanceof Duplex);
  t.deepEqual(stream.name, 'name')
});

test.todo('I can write to a kinesis stream')
test.todo('I can read to a kinesis stream')
