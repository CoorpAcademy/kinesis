import test from 'ava';
import kinesis from '..';
import {Duplex} from 'stream';
import kinesalite from 'kinesalite';

let ports = 4657;

test.cb.beforeEach('setup kinesalite', t => {
  const port = ports++;
  const kinesisServer = kinesalite({ssl: true});
  const kinesisOptions = {host: 'localhost', port};
  const kinesisStreamOptions = Object.assign({}, kinesisOptions, {name: 'a'})
  kinesisServer.listen(kinesisOptions.port, err => {
    if(err) return t.end(err);
    kinesis.request('CreateStream', {StreamName: 'a', ShardCount: 2}, kinesisOptions, err => {
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
test.skip('I can read to a kinesis stream', t => {

  //const kinesisStream = kinesis.stream(t.context.kinesisStreamOptions);
 // kinesisStream.pipe(process.stdout)

})
test.todo('I can pipe streams')
