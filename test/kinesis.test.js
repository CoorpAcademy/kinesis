import test from 'ava';
import kinesis from '..';
import {Duplex} from 'stream';

test('I can create a stream with a name', t => {
  const stream = kinesis.stream('name');

  t.true(stream instanceof Duplex);
  t.deepEqual(stream.name, 'name')
});
