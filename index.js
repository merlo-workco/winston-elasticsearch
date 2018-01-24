'use strict';

const util = require('util');
const fs = require('fs');
const path = require('path');
const Promise = require('promise');
const winston = require('winston');
const moment = require('moment');
const _ = require('lodash');
const retry = require('retry');
const elasticsearch = require('elasticsearch');
const errors = require('common-errors');

const defaultTransformer = require('./transformer');
const BulkWriter = require('./bulk_writer');

/**
 * Constructor
 */
const Elasticsearch = function Elasticsearch(options) {
  this.options = options || {};
  if (!options.timestamp) {
    this.options.timestamp = function timestamp() { return new Date().toISOString(); };
  }
  // Enforce context
  if (!(this instanceof Elasticsearch)) {
    return new Elasticsearch(options);
  }

  // Set defaults
  const defaults = {
    level: 'info',
    index: null,
    indexPrefix: 'logs',
    indexSuffixPattern: 'YYYY.MM.DD',
    messageType: 'log',
    transformer: defaultTransformer,
    ensureMappingTemplate: true,
    flushInterval: 2000,
    waitForActiveShards: 1,
    handleExceptions: false,
    pipeline: null
  };
  _.defaults(options, defaults);
  winston.Transport.call(this, options);

  // Use given client or create one
  if (options.client) {
    this.client = options.client;
  } else {
    const defaultClientOpts = {
      clientOpts: {
        log: [
          {
            type: 'console',
            level: 'error',
          }
        ]
      }
    };
    _.defaults(options, defaultClientOpts);

    // Create a new ES client
    // http://localhost:9200 is the default of the client already
    this.client = new elasticsearch.Client(this.options.clientOpts);
  }

  const bulkWriterOptions = {
    interval: options.flushInterval,
    waitForActiveShards: options.waitForActiveShards,
    pipeline: options.pipeline
  };

  this.bulkWriter = new BulkWriter(
    this.client,
    bulkWriterOptions
  );

  // Pass through bulk writer errors.
  this.bulkWriter.on('error', (err) => {
    this.emit('error', err);
  });
  this.bulkWriter.start();

  // Conduct initial connection check (sets connection state for further use)
  this.checkEsConnection()
      .catch((err) => {
        this.emit('error', err);
      });

  return this;
};

util.inherits(Elasticsearch, winston.Transport);

Elasticsearch.prototype.name = 'elasticsearch';

/**
 * log() method
 */
Elasticsearch.prototype.log = function log(level, message, meta, callback) {
  const logData = {
    message,
    level,
    meta,
    timestamp: this.options.timestamp()
  };
  const entry = this.options.transformer(logData);

  this.bulkWriter.append(
    this.getIndexName(this.options),
    this.options.messageType,
    entry
  );

  callback(); // write is deferred, so no room for errors here :)
};

Elasticsearch.prototype.getIndexName = function getIndexName(options) {
  let indexName = options.index;
  if (indexName === null) {
    const now = moment();
    const dateString = now.format(options.indexSuffixPattern);
    indexName = options.indexPrefix + '-' + dateString;
  }
  return indexName;
};

Elasticsearch.prototype.checkEsConnection = function checkEsConnection() {
  const thiz = this;
  thiz.esConnection = false;

  const operation = retry.operation({
    retries: 3,
    factor: 3,
    minTimeout: 1 * 1000,
    maxTimeout: 60 * 1000,
    randomize: false
  });

  return new Promise((fulfill, reject) => {
    operation.attempt((currentAttempt) => {
      thiz.client.ping().then(
        (res) => {
          thiz.esConnection = true;
          // Ensure mapping template is existing if desired
          if (thiz.options.ensureMappingTemplate) {
            thiz.ensureMappingTemplate(fulfill, reject);
          } else {
            fulfill(true);
          }
        },
        (err) => {
          if (operation.retry(err)) {
            return;
          }
          thiz.esConnection = false;
          thiz.emit('error', err);
          reject(new Error('Cannot connect to ES'));
        }
      );
    });
  });
};

Elasticsearch.prototype.search = function search(q) {
  const index = this.getIndexName(this.options);
  const query = {
    index,
    q
  };
  return this.client.search(query);
};

Elasticsearch.prototype.ensureMappingTemplate = function ensureMappingTemplate(fulfill, reject) {
  const thiz = this;
  // eslint-disable-next-line prefer-destructuring
  let mappingTemplate = thiz.options.mappingTemplate;
  if (mappingTemplate === null || typeof mappingTemplate === 'undefined') {
    const rawdata = fs.readFileSync(path.join(__dirname, 'index-template-mapping.json'));
    mappingTemplate = JSON.parse(rawdata);
  }
  const tmplCheckMessage = {
    name: 'template_' + thiz.options.indexPrefix
  };
  thiz.client.indices.getTemplate(tmplCheckMessage).then(
    (res) => {
      fulfill(res);
    },
   (resOrError) => {
      if (resOrError.status) {
        // On 404 try to create the index and then retry.
       if (resOrError.status === 404) {
          const tmplMessage = {
            name: 'template_' + thiz.options.indexPrefix,
            create: true,
            body: mappingTemplate
          };
          thiz.client.indices.putTemplate(tmplMessage).then(
            (res1) => {
              fulfill(res1);
            },
            (err1) => {
              reject(err1);
            });
        } else {
          thiz.emit('error', new errors.HttpStatusError(resOrError.status));
        }
      } else {
        thiz.emit('error', resOrError);
      }
   });
};

winston.transports.Elasticsearch = Elasticsearch;
module.exports = Elasticsearch;
