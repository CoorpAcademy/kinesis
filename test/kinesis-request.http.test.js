import test from 'ava';
import kinesalite from 'kinesalite';
import kinesis from '../kinesis';

let ports = 5678;

test.beforeEach.cb('setup kinesalite without SSL', t => {
  const port = ports++;
  const kinesisServer = kinesalite({ssl: false});
  const kinesisOptions = {host: 'localhost', port, https: false};
  kinesisServer.listen(kinesisOptions.port, t.end);
  t.context = {kinesisServer, kinesisOptions};
});

test.afterEach.cb('destroy the kinesalite', t => {
  t.context.kinesisServer.close(t.end);
});

test.cb('I can list stream from a kinesis', t => {
  kinesis.listStreams(t.context.kinesisOptions, (err, streams) => {
    t.is(err, null);
    t.deepEqual(streams, []);
    t.end();
  });
});

test.cb('I can list create a stream in the kinesis', t => {
  kinesis.request(
    'CreateStream',
    {StreamName: 'test', ShardCount: 2},
    t.context.kinesisOptions,
    err => {
      t.is(err, null);
      kinesis.listStreams(t.context.kinesisOptions, (errr, streams) => {
        t.is(errr, null);
        t.deepEqual(streams, ['test']);
        t.end();
      });
    }
  );
});
