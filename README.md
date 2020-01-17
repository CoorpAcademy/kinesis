# kinesis

[![npm](https://img.shields.io/npm/v/@coorpacademy/kinesis)](https://www.npmjs.com/package/@coorpacademy/kinesis)
[![Build Status](https://travis-ci.com/CoorpAcademy/kinesis.svg?branch=master)](http://travis-ci.com/CoorpAcademy/kinesis)
[![codecov](https://codecov.io/gh/CoorpAcademy/kinesis/branch/master/graph/badge.svg)](https://codecov.io/gh/CoorpAcademy/kinesis)

> A Node.js stream implementation of [Amazon's Kinesis](http://docs.aws.amazon.com/kinesis/latest/APIReference/).

Allows the consumer to pump data directly into (and out of) a Kinesis stream.

This makes it trivial to setup Kinesis as a logging sink with [Bunyan](https://github.com/trentm/node-bunyan), or any other logging library.

For setting up a local Kinesis instance (eg for testing), check out [Kinesalite](https://github.com/mhart/kinesalite).

## Installation

```bash
npm install --save @coorpacademy/kinesis
```

Note, this is a fork from [@heroku kinesis](https://github.com/heroku/kinesis) which was a fork of [@mhart kinesis](https://github.com/mhart/kinesis) who is the author of [Kinesalite](https://github.com/mhart/kinesalite) the local implementation of kinesis.
Original kinesis library can be found [there](https://www.npmjs.com/package/kinesis)


## Example

```js
const fs = require('fs');
const {Transform} = require('stream');
const kinesis = require('@coorpacademy/kinesis');
const {KinesisStream} = kinesis;

// Uses credentials from process.env by default

kinesis.listStreams({region: 'us-west-1'}, function(err, streams) {
  if (err) throw err;

  console.log(streams); // ["http-logs", "click-logs"]
});

const kinesisSink = kinesis.stream('http-logs');
// OR new KinesisStream('http-logs')

fs.createReadStream('http.log').pipe(kinesisSink);

const kinesisSource = kinesis.stream({name: 'click-logs', oldest: true});

// Data is retrieved as Record objects, so let's transform into Buffers
const bufferify = new Transform({objectMode: true});
bufferify._transform = function(record, encoding, cb) {
  cb(null, record.Data);
};

kinesisSource.pipe(bufferify).pipe(fs.createWriteStream('click.log'));

// Create a new Kinesis stream using the raw API
kinesis.request('CreateStream', {StreamName: 'test', ShardCount: 2}, function(err) {
  if (err) throw err;

  kinesis.request('DescribeStream', {StreamName: 'test'}, function(err, data) {
    if (err) throw err;

    console.dir(data);
  });
});
```

## API

### kinesis.stream(options)
### new KinesisStream(options)

Returns a readable and writable Node.js stream for the given Kinesis stream

`options` include:

  - `region`: a string, or (deprecated) object with AWS credentials, host, port, etc (resolved from env or file by default)
  - `credentials`: an object with `accessKeyId`/`secretAccessKey` properties (resolved from env, file or IAM by default)
  - `shards`: an array of shard IDs, or shard objects. If not provided, these will be fetched and cached.
  - `oldest`: if truthy, then will start at the oldest records (using `TRIM_HORIZON`) instead of the latest
  - `writeConcurrency`: how many parallel writes to allow (`1` by default)
  - `cacheSize`: number of PartitionKey-to-SequenceNumber mappings to cache (`1000` by default)
  - `agent`: HTTP agent used (uses Node.js defaults otherwise)
  - `timeout`: HTTP request timeout (uses Node.js defaults otherwise)
  - `initialRetryMs`: first pause before retrying under the default policy (`50` by default)
  - `maxRetries`: max number of retries under the default policy (`10` by default)
  - `errorCodes`: array of Node.js error codes to retry on (`['EADDRINFO',
    'ETIMEDOUT', 'ECONNRESET', 'ESOCKETTIMEDOUT', 'ENOTFOUND', 'EMFILE']` by default)
  - `errorNames`: array of Kinesis exceptions to retry on
    (`['ProvisionedThroughputExceededException', 'ThrottlingException']` by default)
  - `retryPolicy`: a function to implement a retry policy different from the default one
  - `logger`: an object which implements a `log` method, e.g. `console`.
  - `endpoint`: **new** configuration to specify host, port and protocol (https or not) a once

### kinesis.listStreams([options], callback)

Calls the callback with an array of all stream names for the AWS account

### kinesis.request(action, [data], [options], callback)

Makes a generic Kinesis request with the given action (eg, `ListStreams`) and data as the body.

`options` include:

  - `region`: a string, or (deprecated) object with AWS credentials, host, port, etc (resolved from env or file by default)
  - `credentials`: an object with `accessKeyId`/`secretAccessKey` properties (resolved from env, file or IAM by default)
  - `agent`: HTTP agent used (uses Node.js defaults otherwise)
  - `timeout`: HTTP request timeout (uses Node.js defaults otherwise)
  - `initialRetryMs`: first pause before retrying under the default policy (`50` by default)
  - `maxRetries`: max number of retries under the default policy (`10` by default)
  - `errorCodes`: array of Node.js error codes to retry on (`['EADDRINFO',
    'ETIMEDOUT', 'ECONNRESET', 'ESOCKETTIMEDOUT', 'ENOTFOUND', 'EMFILE']` by default)
  - `errorNames`: array of Kinesis exceptions to retry on
    (`['ProvisionedThroughputExceededException', 'ThrottlingException']` by default)
  - `retryPolicy`: a function to implement a retry policy different from the default one
  - `endpoint`: **new** configuration to specify host, port and protocol (https or not) a once

Note, if targeting `localhost`/`127.0.0.1`, or having specified `http` as protocol,
you won't get any error for self-signed certificates. (which is what you need in a docker testing context)
