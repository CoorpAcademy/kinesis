import test from 'ava';
import kinesis from '..';
import {Duplex} from 'stream';
import kinesalite from 'kinesalite';
import portfinder from 'portfinder';

async function createServer() {
  const port = 4222//random();
  return new Promise((resolve, reject) => {
    const kinesisServer = kinesalite({ssl: true});
    kinesisServer.listen(kinesisOptions.port, (err) => {
      if (err) return reject(err);
      return resolve({kinesisServer, kinesisOptions: {host: 'localhost', port}});
    })
  });
}

test.cb.afterEach('destroy the kinesalite', t => {
  kinesisServer.close(t.end)
})

test.cb('I can list stream from a kinesis', async t => {
  const {kinesisServer, kinesisOptions} = await createServer();
  kinesis.listStreams(kinesisOptions, (err, streams) => {
    t.is(err, null);
    t.deepEqual(streams, []);
    kinesisServer.close(t.end())
  })
})

test.cb('I can list create a stream in the kinesis', t => {
  kinesis.request('CreateStream', {StreamName: 'test', ShardCount: 2}, kinesisOptions, err => {
    t.is(err, null);
    kinesis.listStreams(kinesisOptions, (err, streams) => {
      t.is(err, null);
      t.deepEqual(streams, ['test']);
      t.end();
    })
  })
})
