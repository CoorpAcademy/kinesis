import test from 'ava';
import kinesis from '..';
import {Duplex} from 'stream';
import kinesalite from 'kinesalite';
import portfinder from 'portfinder';

let kinesisServer;
let kinesisOptions;

test.cb.beforeEach('set up the kinesalite', t => {
  portfinder.getPort((err, port) => {
    if(err) t.end(err)
    kinesisOptions = {host: 'localhost', port: port};
    kinesisServer = kinesalite({ssl: true});
    kinesisServer.listen(kinesisOptions.port, t.end)
  });
})

test.cb.beforeEach('set up the kinesalite', t => {
  kinesisServer = kinesalite({ssl: true});
  kinesisServer.listen(kinesisOptions.port, err => {
    if(err) return t.end(err);
    kinesis.request('CreateStream', {StreamName: 'test', ShardCount: 2}, kinesisOptions, t.end)
    })
})
test.cb.afterEach('destroy the kinesalite', t => {
  kinesisServer.close(t.end)
})

test.todo('I can write to a kinesis stream')
test.todo('I can read to a kinesis stream')
