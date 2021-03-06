const https = require('https');
const http = require('http');
const aws4 = require('aws4');
const awscred = require('awscred');
const once = require('once');

const {nullLogger} = require('./utils');

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
  } else if (options.endpoint) {
    const match = options.endpoint.match(/^(https?):\/\/([\w\-.]+)(?::(\d+))?$/);
    if (!match) throw new Error('Provided endpoint value is invalid');
    const [, protocol, host, port] = match;
    options.host = host;
    if (port) options.port = Number(port);
    options.https = protocol === 'https';
  } else if (/^[a-z]{2}-[a-z]+-\d$/.test(region)) {
    options.region = region;
  } else if (!options.host) {
    // Backwards compatibility for when 1st param was host
    options.host = region;
  }
  if (!options.version) options.version = '20131202';
  if (!options.logger) options.logger = nullLogger;

  return options;
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
    return makeRequest((err, data) => {
      if (!err || i >= maxRetries) return cb(err, data);

      if (err.statusCode === 400 && ~expiredNames.indexOf(err.name)) {
        // eslint-disable-next-line no-shadow
        return awscred.loadCredentials((err, credentials) => {
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

  function loadCreds(callback) {
    const needRegion = !options.region;
    const needCreds =
      !options.credentials ||
      !options.credentials.accessKeyId ||
      !options.credentials.secretAccessKey;
    if (needRegion && needCreds) return awscred.load(callback);

    if (needRegion) return awscred.loadRegion((err, region) => callback(err, {region}));

    if (needCreds)
      return awscred.loadCredentials((err, credentials) => callback(err, {credentials}));

    callback(null, {});
  }

  loadCreds((err, creds) => {
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
    if (options.agent !== undefined) httpOptions.agent = options.agent;
    if (options.timeout !== undefined) httpOptions.timeout = options.timeout;
    if (options.region !== undefined) httpOptions.region = options.region;
    httpOptions.method = 'POST';
    httpOptions.path = '/';
    httpOptions.body = body;

    // Don't worry about self-signed certs for localhost/testing and http
    if (httpOptions.host === 'localhost' || httpOptions.host === '127.0.0.1')
      httpOptions.rejectUnauthorized = false;

    httpOptions.headers = {
      Host: httpOptions.host,
      'Content-Length': Buffer.byteLength(body),
      'Content-Type': 'application/x-amz-json-1.1',
      'X-Amz-Target': `Kinesis_${options.version}.${action}`
    };

    function makeRequest(callback) {
      httpOptions.headers.Date = new Date().toUTCString();

      aws4.sign(httpOptions, options.credentials);

      options.logger.log({
        kinesis_request: true,
        at: 'start',
        host: httpOptions.host,
        path: httpOptions.path,
        action
      });

      const protocolLib = options.https === false ? http : https;
      const req = protocolLib
        .request(httpOptions, res => {
          let json = '';

          res.setEncoding('utf8');

          res.on('error', error => {
            options.logger.log({
              kinesis_request: true,
              at: 'error',
              host: httpOptions.host,
              path: httpOptions.path,
              action,
              error
            });
          });

          res.on('error', callback);
          res.on('data', chunk => {
            json += chunk;
          });
          res.on('end', () => {
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

              return callback(null, response);
            }

            const error = new Error();
            error.statusCode = res.statusCode;
            if (response !== undefined) {
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

            callback(error);
          });
        })
        .on('error', callback);

      if (options.timeout !== undefined) {
        req.setTimeout(options.timeout, () => {
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

module.exports = {request, defaultRetryPolicy, resolveOptions};
