const crypto = require('crypto');
const util = require('util');
const stream = require('stream');
const async = require('async');
const lruCache = require('lru-cache');

const {bignumCompare, nullLogger} = require('./utils');
const {request} = require('./kinesis-request');

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
    // eslint-disable-next-line no-shadow
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
          // eslint-disable-next-line max-depth
          if (this.shards[i].writeSequenceNumber)
            data.SequenceNumberForOrdering = this.shards[i].writeSequenceNumber;
          break;
        }
      }
      // Not actually supposed to be part of PutRecord
      delete data.ShardId;

      // Otherwise check if we have it cached for this PartitionKey
    } else if ((sequenceNumber = this.sequenceCache.get(data.PartitionKey)) !== undefined) {
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
    // eslint-disable-next-line no-shadow
    this.resolveShards((err, shards) => {
      if (err) {
        this.emit('putRecord', data);
        return this.emit('error', err);
      }
      for (let j = 0; j < shards.length; j++) {
        if (shards[j].id === responseData.ShardId) {
          if (bignumCompare(sequenceNumber, shards[j].writeSequenceNumber) > 0)
            shards[j].writeSequenceNumber = sequenceNumber;

          this.emit('putRecord', data);
        }
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

module.exports = KinesisStream;
