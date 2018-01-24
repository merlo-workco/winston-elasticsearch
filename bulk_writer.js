'use strict';
const Promise = require('promise');
const debug = require('debug')('winston:elasticsearch');
const EventEmitter = require('events');
const util = require('util');

const BulkWriter = function BulkWriter(client, options) {
  this.client = client;
  this.interval = options.interval || 5000;
  this.waitForActiveShards = options.waitForActiveShards;
  this.pipeline = options.pipeline;

  this.bulk = []; // bulk to be flushed
  this.running = false;
  this.timer = false;
  debug('created', this);
};

BulkWriter.prototype.start = function start() {
  this.stop();
  this.running = true;
  this.tick();
  debug('started');
};

BulkWriter.prototype.stop = function stop() {
  this.running = false;
  if (!this.timer) { return; }
  clearTimeout(this.timer);
  this.timer = null;
  debug('stopped');
};

BulkWriter.prototype.schedule = function schedule() {
  const thiz = this;
  this.timer = setTimeout(() => {
    thiz.tick();
  }, this.interval);
};

BulkWriter.prototype.tick = function tick() {
  debug('tick');
  const thiz = this;
  if (!this.running) { return; }
  this.flush()
    .then(() => {
      // Emulate finally with last .then()
    })
    .catch((e) => {
      // We emit the error but don't throw it again - there is nothing "above" us (as this was
      // invoked from `setTimeout`) *and* we want to reschedule even in the case of an error.
      thiz.emit('error', e);
    })
    .then(() => { // finally()
      thiz.schedule();
    });
};

BulkWriter.prototype.flush = function flush() {
  // write bulk to elasticsearch
  const thiz = this;
  if (this.bulk.length === 0) {
    debug('nothing to flush');
    return new Promise((resolve) => {
      return resolve();
    });
  }

  const bulk = this.bulk.concat();
  this.bulk = [];
  debug('going to write', bulk);
  return this.client.bulk({
    body: bulk,
    waitForActiveShards: this.waitForActiveShards,
    timeout: this.interval + 'ms',
    type: this.type
  }).catch((e) => {
    // rollback this.bulk array
    thiz.bulk = bulk.concat(thiz.bulk);
    // eslint-disable-next-line no-console
    console.error(e);
  });
};

BulkWriter.prototype.append = function append(index, type, doc) {
  this.bulk.push({
    index: {
      _index: index, _type: type, pipeline: this.pipeline
    }
  });
  this.bulk.push(doc);
};

util.inherits(BulkWriter, EventEmitter);

module.exports = BulkWriter;
