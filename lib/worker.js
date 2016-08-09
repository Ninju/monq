var events = require('events');
var util = require('util');
var Queue = require('./queue');
var Profiler = require('./profiler');
var fork = require('child_process').fork;
var path = require('path');

module.exports = Worker;

function Worker(queues, options) {
    options || (options = {});
    options.queues || (options.queues = []);

    this.parallel = options.parallel || false;
    this.empty = 0;
    this.queues = queues;
    this.interval = options.interval || 5000;

    this.callbacks = options.callbacks || {};
    this.strategies = options.strategies || {};

    // Default retry strategies
    this.strategies.linear || (this.strategies.linear = linear);
    this.strategies.exponential || (this.strategies.exponential = exponential);

    // This worker will only process jobs of this priority or higher
    this.minPriority = options.minPriority;

    if (this.queues.length === 0) {
        throw new Error('Worker must have at least one queue.');
    }
}

util.inherits(Worker, events.EventEmitter);

Worker.prototype.register = function (callbacks) {
    for (var name in callbacks) {
        this.callbacks[name] = callbacks[name];
    }
};

Worker.prototype.strategies = function (strategies) {
    for (var name in strategies) {
        this.strategies[name] = strategies[name];
    }
};

Worker.prototype.start = function () {
    this.working = true;
    this.poll();
};

Worker.prototype.stop = function (callback) {
    var self = this;

    function done() {
        if (callback) callback();
    }

    if (!this.working) done();
    this.working = false;

    if (this.pollTimeout) {
      clearTimeout(this.pollTimeout);
      this.pollTimeout = null;
      return done();
    }

    this.once('stopped', done);
};

Worker.prototype.poll = function () {
    if (!this.working) {
        return this.emit('stopped');
    }

    var self = this;

    this.dequeue(function (err, job) {
        if (err) return self.emit('error', err);

        if (job) {
            self.empty = 0;
            self.emit('dequeued', job.data);
            self.work(job);
            if (self.parallel) process.nextTick(function(){self.poll()});
        } else {
            self.emit('empty');

            if (self.empty < self.queues.length) {
                self.empty++;
            }

            if (self.empty === self.queues.length) {
                // All queues are empty, wait a bit
                self.pollTimeout = setTimeout(function () {
                    self.pollTimeout = null;
                    self.poll();
                }, self.interval);
            } else {
                self.poll();
            }
        }
    });
};

Worker.prototype.dequeue = function (callback) {
    var queue = this.queues.shift();
    this.queues.push(queue);
    queue.dequeue({ minPriority: this.minPriority, callbacks: this.callbacks }, callback);
};

Worker.prototype.work = function (job) {
    var self = this;
    var finished = false;
    let backupProfiler = new Profiler(job);

    if (job.data.timeout) {
        var timer = setTimeout(function () {
            done(new Error('timeout'));
        }, job.data.timeout);
    }

    var runningJob = this.process(job.data, done);

    function done(err, result, profilerStats) {
        if (profiler === undefined) {
          profiler = backupProfiler.getStats();
        }

        if (runningJob && runningJob.kill) {
          runningJob.kill();
        }

        if (finished) {
            return;
        } else {
            finished = true;
        }

        clearTimeout(timer);
        self.emit('done', job.data);
        job.data.stats = profilerStats;

        if (err) {
            self.error(job, err, function (err) {
                if (err) return self.emit('error', err);

                self.emit('failed', job.data);
                self.poll();
            });
        } else {
            job.complete(result, function (err) {
                if (err) return self.emit('error', err);

                self.emit('complete', job.data);
                self.poll();
            });
        }
    };
};

Worker.prototype.process = function (data, done) {
    var callback = this.callbacks[data.name];

    if (!callback) {
        done(new Error('No callback registered for `' + data.name + '`'));
        return null;
    }

    callback.callCount = callback.callCount || 0;
    callback.callCount += 1;

    let workerProcess = fork(path.join(__dirname, './worker/run.js'), [callback.toString(), JSON.stringify(data)], { stdio: 'inherit' });

    workerProcess.on('message', ({ err, data, profilerStats }) => done(err, data, profilerStats));

    return workerProcess;
};

Worker.prototype.error = function (job, err, callback) {
    var attempts = job.data.attempts;
    var remaining = 0;

    if (attempts) {
        remaining = attempts.remaining = (attempts.remaining || attempts.count) - 1;
    }

    if (remaining > 0) {
        var strategy = this.strategies[attempts.strategy || 'linear'];
        if (!strategy) {
            strategy = linear;

            console.error('No such retry strategy: `' + attempts.strategy + '`');
            console.error('Using linear strategy');
        }

        if (attempts.delay !== undefined) {
            var wait = strategy(attempts);
        } else {
            var wait = 0;
        }

        job.delay(wait, callback)
    } else {
        job.fail(err, callback);
    }
};

// Strategies
// ---------------

function linear(attempts) {
    return attempts.delay;
}

function exponential(attempts) {
    return attempts.delay * (attempts.count - attempts.remaining);
}
