const util = require('util');
const stream = require('stream');
const https = require('https');
const crypto = require('crypto');
const async = require('async');
const once = require('once');
const lruCache = require('lru-cache');
const aws4 = require('aws4');
const awscred = require('awscred');

exports.stream = function(options) {
  return new KinesisStream(options);
};
exports.KinesisStream = KinesisStream;
exports.listStreams = listStreams;
exports.request = request;

const nullLogger = {log: function noop() {}};

function KinesisStream(options) {
  if (typeof options === 'string') options = {name: options};
  if (!options || !options.name) throw new Error('A stream name must be given');
  stream.Duplex.call(this, {objectMode: true});
  this.options = options;
  this.name = options.name;
  this.writeConcurrency = options.writeConcurrency || 1;
  this.sequenceCache = lruCache(options.cacheSize || 1000);
  this.currentWrites = 0;
  this.buffer = [];
  this.paused = true;
  this.fetching = false;
  this.shards = [];
  this.logger = options.logger || nullLogger;
}
util.inherits(KinesisStream, stream.Duplex);

KinesisStream.prototype._read = function() {
  this.paused = false;
  this.drainBuffer();
};

KinesisStream.prototype.drainBuffer = function() {
  if (this.paused) return;
  while (this.buffer.length) {
    if (!this.push(this.buffer.shift())) {
      this.paused = true;
      return;
    }
  }
  if (this.fetching) return;
  this.fetching = true;
  this.getNextRecords(err => {
    this.fetching = false;
    if (err) this.emit('error', err);

    // If all shards have been closed, the stream should end
    if (
      this.shards.every(function(shard) {
        return shard.ended;
      })
    )
      return this.push(null);

    if (this.options.backoffTime) {
      setTimeout(this.drainBuffer.bind(this), this.options.backoffTime);
    } else {
      this.drainBuffer();
    }
  });
};

KinesisStream.prototype.getNextRecords = function(cb) {
  this.resolveShards((err, shards) => {
    if (err) return cb(err);
    async.each(shards, this.getShardIteratorRecords.bind(this), cb);
  });
};

KinesisStream.prototype.resolveShards = function(cb) {
  if (this.shards.length > 0) return cb(null, this.shards);

  const getShards = this.options.shards
    ? callback => callback(null, this.options.shards)
    : this.getShardIds.bind(this);

  getShards((err, shards) => {
    if (err) return cb(err);

    this.shards = shards.map(function(shard) {
      return typeof shard === 'string'
        ? {
            id: shard,
            readSequenceNumber: null,
            writeSequenceNumber: null,
            nextShardIterator: null,
            ended: false
          }
        : shard;
    });

    cb(null, this.shards);
  });
};

KinesisStream.prototype.getShardIds = function(cb) {
  request('DescribeStream', {StreamName: this.name}, this.options, (err, res) => {
    if (err) return cb(err);
    cb(
      null,
      res.StreamDescription.Shards
        .filter(
          shard => !(shard.SequenceNumberRange && shard.SequenceNumberRange.EndingSequenceNumber)
        )
        .map(shard => shard.ShardId)
    );
  });
};

KinesisStream.prototype.getShardIteratorRecords = function(shard, cb) {
  const data = {StreamName: this.name, ShardId: shard.id};
  let getShardIterator;

  if (shard.nextShardIterator !== null) {
    getShardIterator = callback => callback(null, shard.nextShardIterator);
  } else {
    if (shard.readSequenceNumber !== null) {
      data.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
      data.StartingSequenceNumber = shard.readSequenceNumber;
    } else if (this.options.oldest) {
      data.ShardIteratorType = 'TRIM_HORIZON';
    } else {
      data.ShardIteratorType = 'LATEST';
    }
    getShardIterator = callback =>
      request('GetShardIterator', data, this.options, (err, res) => {
        if (err) return callback(err);
        callback(null, res.ShardIterator);
      });
  }

  getShardIterator((err, shardIterator) => {
    if (err) return cb(err);

    this.getRecords(shard, shardIterator, (err, records) => {
      if (err) {
        // Try again if the shard iterator has expired
        if (err.name === 'ExpiredIteratorException') {
          shard.nextShardIterator = null;
          return this.getShardIteratorRecords(shard, cb);
        }
        return cb(err);
      }

      if (records.length > 0) {
        shard.readSequenceNumber = records[records.length - 1].SequenceNumber;
        this.buffer = this.buffer.concat(records);

        if (this.options.backoffTime) {
          setTimeout(this.drainBuffer.bind(this), this.options.backoffTime);
        } else {
          this.drainBuffer();
        }
      }

      cb();
    });
  });
};

KinesisStream.prototype.getRecords = function(shard, shardIterator, cb) {
  const limit = this.options.limit || 25;
  const data = {ShardIterator: shardIterator, Limit: limit};

  this.logger.log({get_records: true, at: 'start'});
  request('GetRecords', data, this.options, (err, res) => {
    if (err) {
      this.logger.log({get_records: true, at: 'error', error: err});
      return cb(err);
    }

    // If the shard has been closed the requested iterator will not return any more data
    if (res.NextShardIterator === null) {
      this.logger.log({get_records: true, at: 'shard-ended'});
      shard.ended = true;
      return cb(null, []);
    }

    shard.nextShardIterator = res.NextShardIterator;
    this.logger.log({
      get_records: true,
      at: 'finish',
      record_count: res.Records.length
    });

    res.Records.forEach(record => {
      record.ShardId = shard.id;
      record.Data = Buffer.from(record.Data, 'base64');
    });

    return cb(null, res.Records);
  });
};

KinesisStream.prototype._write = function(data, encoding, cb) {
  let i;
  let sequenceNumber;

  if (Buffer.isBuffer(data)) data = {Data: data};

  if (Buffer.isBuffer(data.Data)) data.Data = data.Data.toString('base64');

  if (!data.StreamName) data.StreamName = this.name;

  if (!data.PartitionKey) data.PartitionKey = crypto.randomBytes(16).toString('hex');

  if (!data.SequenceNumberForOrdering) {
    // If we only have 1 shard then we can just use its sequence number
    if (this.shards.length === 1 && this.shards[0].writeSequenceNumber) {
      data.SequenceNumberForOrdering = this.shards[0].writeSequenceNumber;

      // Otherwise, if we have a shard ID already assigned, then use that
    } else if (data.ShardId) {
      for (i = 0; i < this.shards.length; i++) {
        if (this.shards[i].id === data.ShardId) {
          if (this.shards[i].writeSequenceNumber)
            data.SequenceNumberForOrdering = this.shards[i].writeSequenceNumber;
          break;
        }
      }
      // Not actually supposed to be part of PutRecord
      delete data.ShardId;

      // Otherwise check if we have it cached for this PartitionKey
    } else if ((sequenceNumber = this.sequenceCache.get(data.PartitionKey)) !== null) {
      data.SequenceNumberForOrdering = sequenceNumber;
    }
  }

  this.currentWrites++;

  request('PutRecord', data, this.options, (err, responseData) => {
    this.currentWrites--;
    if (err) {
      this.emit('putRecord', data);
      return this.emit('error', err);
    }
    sequenceNumber = responseData.SequenceNumber;

    if (bignumCompare(sequenceNumber, this.sequenceCache.get(data.PartitionKey)) > 0)
      this.sequenceCache.set(data.PartitionKey, sequenceNumber);

    this.resolveShards((err, shards) => {
      if (err) {
        this.emit('putRecord', data);
        return this.emit('error', err);
      }
      for (let i = 0; i < shards.length; i++) {
        if (shards[i].id !== responseData.ShardId) continue;

        if (bignumCompare(sequenceNumber, shards[i].writeSequenceNumber) > 0)
          shards[i].writeSequenceNumber = sequenceNumber;

        this.emit('putRecord', data);
      }
    });
  });

  if (this.currentWrites < this.writeConcurrency) return cb();

  const onPutRecord = _data => {
    if (data !== _data) return;
    this.removeListener('putRecord', onPutRecord);
    cb();
  };
  this.on('putRecord', onPutRecord);
};

function listStreams(options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }

  request('ListStreams', {}, options, (err, res) => {
    if (err) return cb(err);

    return cb(null, res.StreamNames);
  });
}

function request(action, data, options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }
  if (!cb) {
    cb = data;
    data = {};
  }

  cb = once(cb);

  options = resolveOptions(options);

  function loadCreds(cb) {
    const needRegion = !options.region;
    const needCreds =
      !options.credentials ||
      !options.credentials.accessKeyId ||
      !options.credentials.secretAccessKey;
    if (needRegion && needCreds) {
      return awscred.load(cb);
    } else if (needRegion) {
      return awscred.loadRegion(function(err, region) {
        cb(err, {region});
      });
    } else if (needCreds) {
      return awscred.loadCredentials(function(err, credentials) {
        cb(err, {credentials});
      });
    }
    cb(null, {});
  }

  loadCreds(function(err, creds) {
    if (err) return cb(err);

    if (creds.region) options.region = creds.region;
    if (creds.credentials) {
      if (!options.credentials) {
        options.credentials = creds.credentials;
      } else {
        Object.keys(creds.credentials).forEach(function(key) {
          if (!options.credentials[key]) options.credentials[key] = creds.credentials[key];
        });
      }
    }

    if (!options.region) options.region = (options.host || '').split('.', 2)[1] || 'us-east-1';
    if (!options.host) options.host = `kinesis.${options.region}.amazonaws.com`;

    const httpOptions = {};
    const body = JSON.stringify(data);
    const retryPolicy = options.retryPolicy || defaultRetryPolicy;

    httpOptions.host = options.host;
    httpOptions.port = options.port;
    if (options.agent) httpOptions.agent = options.agent;
    if (options.timeout) httpOptions.timeout = options.timeout;
    if (options.region) httpOptions.region = options.region;
    httpOptions.method = 'POST';
    httpOptions.path = '/';
    httpOptions.body = body;

    // Don't worry about self-signed certs for localhost/testing
    if (httpOptions.host === 'localhost' || httpOptions.host === '127.0.0.1')
      httpOptions.rejectUnauthorized = false;

    httpOptions.headers = {
      Host: httpOptions.host,
      'Content-Length': Buffer.byteLength(body),
      'Content-Type': 'application/x-amz-json-1.1',
      'X-Amz-Target': `Kinesis_${options.version}.${action}`
    };

    function makeRequest(cb) {
      httpOptions.headers.Date = new Date().toUTCString();

      aws4.sign(httpOptions, options.credentials);

      options.logger.log({
        kinesis_request: true,
        at: 'start',
        host: httpOptions.host,
        path: httpOptions.path,
        action
      });

      const req = https
        .request(httpOptions, function(res) {
          let json = '';

          res.setEncoding('utf8');

          res.on('error', function(error) {
            options.logger.log({
              kinesis_request: true,
              at: 'error',
              host: httpOptions.host,
              path: httpOptions.path,
              action,
              error
            });
          });

          res.on('error', cb);
          res.on('data', function(chunk) {
            json += chunk;
          });
          res.on('end', function() {
            let response, parseError;

            if (json)
              try {
                response = JSON.parse(json);
              } catch (e) {
                parseError = e;
              }

            if (res.statusCode === 200 && !parseError) {
              options.logger.log({
                kinesis_request: true,
                at: 'finish',
                host: httpOptions.host,
                path: httpOptions.path,
                action,
                status: res.statusCode,
                length: json.length,
                content_length: res.headers['content-length']
              });

              return cb(null, response);
            }

            const error = new Error();
            error.statusCode = res.statusCode;
            if (response !== null) {
              error.name = (response.__type || '').split('#').pop();
              error.message = response.message || response.Message || JSON.stringify(response);
            } else {
              if (res.statusCode === 413) json = 'Request Entity Too Large';
              error.message = `HTTP/1.1 ${res.statusCode} ${json}`;
            }

            options.logger.log({
              kinesis_request: true,
              at: 'error',
              host: httpOptions.host,
              path: httpOptions.path,
              action,
              status: error.statusCode,
              name: error.name,
              message: error.message
            });

            cb(error);
          });
        })
        .on('error', cb);

      if (options.timeout) {
        req.setTimeout(options.timeout, function() {
          options.logger.log({
            kinesis_request: true,
            at: 'timeout',
            host: httpOptions.host,
            path: httpOptions.path,
            action
          });
          req.abort();
        });
      }

      req.end(body);

      return req;
    }

    return retryPolicy(makeRequest, options, cb);
  });
}

function defaultRetryPolicy(makeRequest, options, cb) {
  const initialRetryMs = options.initialRetryMs || 50;
  const maxRetries = options.maxRetries || 10; // Timeout doubles each time => ~51 sec timeout
  const errorCodes = options.errorCodes || [
    'EADDRINFO',
    'ETIMEDOUT',
    'ECONNRESET',
    'ESOCKETTIMEDOUT',
    'ENOTFOUND',
    'EMFILE'
  ];
  const errorNames = options.errorNames || [
    'ProvisionedThroughputExceededException',
    'ThrottlingException'
  ];
  const expiredNames = options.expiredNames || [
    'ExpiredTokenException',
    'ExpiredToken',
    'RequestExpired'
  ];

  function retry(i) {
    return makeRequest(function(err, data) {
      if (!err || i >= maxRetries) return cb(err, data);

      if (err.statusCode === 400 && ~expiredNames.indexOf(err.name)) {
        return awscred.loadCredentials(function(err, credentials) {
          if (err) return cb(err);
          options.credentials = credentials;
          return makeRequest(cb);
        });
      }

      if (err.statusCode >= 500 || ~errorCodes.indexOf(err.code) || ~errorNames.indexOf(err.name))
        return setTimeout(retry, initialRetryMs << i, i + 1);

      return cb(err);
    });
  }

  return retry(0);
}

function resolveOptions(options) {
  const region = options.region;

  options = Object.keys(options).reduce(function(clone, key) {
    clone[key] = options[key];
    return clone;
  }, {});

  if (typeof region === 'object' && region !== null) {
    options.host = options.host || region.host;
    options.port = options.port || region.port;
    options.region = options.region || region.region;
    options.version = options.version || region.version;
    options.agent = options.agent || region.agent;
    options.https = options.https || region.https;
    options.credentials = options.credentials || region.credentials;
  } else if (/^[a-z]{2}\-[a-z]+\-\d$/.test(region)) {
    options.region = region;
  } else if (!options.host) {
    // Backwards compatibility for when 1st param was host
    options.host = region;
  }
  if (!options.version) options.version = '20131202';
  if (!options.logger) options.logger = nullLogger;

  return options;
}

function bignumCompare(a, b) {
  if (!a) return -1;
  if (!b) return 1;
  const lengthDiff = a.length - b.length;
  if (lengthDiff !== 0) return lengthDiff;
  return a.localeCompare(b);
}
