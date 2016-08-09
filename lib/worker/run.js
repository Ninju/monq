'use strict';

let Profiler = require('./../profiler');

const jobFunctionAsString = process.argv[2],
      args = process.argv[3];

function processFunc(callback, jobDataAsString) {
    let jobData = JSON.parse(jobDataAsString);
    let { params } = jobData;
    let job = { data: jobData }

    let profiler = new Profiler(job);

    callback.call(profiler, params, function(err, data) {
        let profilerStats = profiler.getStats();
        process.send({ profilerStats, err, data });
    });
}

let callProcessFunc = '(' + processFunc + ')' + '(' + jobFunctionAsString + ', ' + args + ')'

eval(callProcessFunc);
