//-------------------------------------------------------------------------------
// Copyright IBM Corp. 2016
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-------------------------------------------------------------------------------
'use strict';

var Stream = require('stream').Writable;
var util = require('util');
var _ = require('lodash');

/**
  * Simple stream that writes log level information and message text to the console stream(s)
  * @param config future use
  */
var consoleStream = function (config) {

    config = config || {};

    this.streamConfig = {};

    if(config.stderrThreshold) {
        this.streamConfig.stderrThreshold =  _.find([10,20,30,40,50,60], function(level) {
                                                                                            if(config.stderrThreshold === level) {
                                                                                              return true;
                                                                                            }
                                                                                            else {
                                                                                              return false;
                                                                                            }
                                              }); 
    }


    /**
      * Returns a string representation of the bunyan log levels
      * @param number bunyanLoggingLevel - integer representing a bunyan logging level
      * @return string representation of bunyanLoggingLevel
      * @see https://github.com/trentm/node-bunyan 
      */
    this.getLoggingLevelString = function(bunyanLoggingLevel) {

        switch(bunyanLoggingLevel) {
            case 10:
                return 'TRACE';
            case 20:
                return 'DEBUG';
            case 30:
                return 'INFO';
            case 40:
                return 'WARN';
            case 50:
                return 'ERROR';
            case 60:
                return 'FATAL';
            default:
                return 'INFO';
        }
    };

    /**
      * Write data.msg to console (stderr or stdout, depending on whether data.level > stderrThreshold)
      * @param data bunyan log record
      */
    this.write = function (data) {

        if((data) && (data.msg)) {
            if((this.streamConfig.stderrThreshold)&&(data.level >= this.streamConfig.stderrThreshold)) {
                console.error(this.getLoggingLevelString(data.level) + ': ' + data.msg);
            }
            else {
                console.log(this.getLoggingLevelString(data.level) + ': ' + data.msg);    
            }            
        }
    };

};

util.inherits(consoleStream, Stream);

var createStream = function(config) {
    return new consoleStream(config);
};

module.exports.createStream = createStream;
