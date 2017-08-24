import test from 'ava';
import kinesis from '..';
import {Duplex} from 'stream';
import kinesalite from 'kinesalite';

let ports = 4567;

test.cb.beforeEach('setup kinesalite', t => {
    const port = ports++;
    const kinesisServer = kinesalite({ssl: true});
    const kinesisOptions = {host: 'localhost', port}
    kinesisServer.listen(kinesisOptions.port, t.end);
    t.context = {kinesisServer, kinesisOptions};
  });

test.cb.afterEach('destroy the kinesalite', t => {
  t.context.kinesisServer.close(t.end)
})

test.cb('I can list stream from a kinesis', t => {
  kinesis.listStreams(t.context.kinesisOptions, (err, streams) => {
    t.is(err, null);
    t.deepEqual(streams, []);
    t.end();
  })
})

test.cb('I can list create a stream in the kinesis', t => {
  kinesis.request('CreateStream', {StreamName: 'test', ShardCount: 2}, t.context.kinesisOptions, err => {
    t.is(err, null);
    kinesis.listStreams(t.context.kinesisOptions, (err, streams) => {
      t.is(err, null);
      t.deepEqual(streams, ['test']);
      t.end();
    })
  })
})
