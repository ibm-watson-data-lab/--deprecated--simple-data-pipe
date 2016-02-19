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

var bunyan = require('bunyan'); 
var moment = require('moment');
var consoleStream = require('bunyan-console-stream');
var bluemixHelperConfig = require('bluemix-helper-config');

var fs = require('fs');
var loggers = {};


	var isDebug = function(loggerName) {

		if(! loggerName) {
			return false;
		}

		var debug_env_var = bluemixHelperConfig.configManager.get('DEBUG') || '';

		var re = new RegExp('\s*' + loggerName + '\s*','i');

	    if((debug_env_var.match(/^[\s]*\*[\s]*$/i)) || (debug_env_var.match(/[\s,]+\*[\s,]*/i)) || (debug_env_var.match(re))) {
				// enable lowest level of logging for if the DEBUG environment variable is set for loggerName
				return true;
		}

		return false;
	};

	var logPath = function( loggerName ){

		var logDir = require('path').resolve( __dirname, '../../', 'logs');
		var createDir = false;
		try{
			createDir = !fs.lstatSync( logDir ).isDirectory();
		}catch(e){
			createDir = true;
		}
		if ( createDir ){
			fs.mkdirSync( logDir );
		}
		return logDir + '/' + loggerName + '.' + moment().format('YYYYMMDD-HHmm') + '.log';

	};

	
	var getLogger = function(loggerName) {

		if(! loggers.hasOwnProperty([loggerName])) {

			var filePath = logPath(loggerName);

			var logger = bunyan.createLogger({
												name: loggerName,
												src: false,
												streams: [
												    		{
												    			path: filePath,
												    			level: 'trace'
												    		},
												    		{
												    			type: 'raw',
												    			stream: consoleStream.createStream({stderrThreshold:40})
												    		}
												    	]
											});

			//Remember the logPath so we write it as an attachment to the run document
			logger.logPath = filePath;

			// set default log level
			if(! isDebug(loggerName)) {
				logger.level('info');	
			}
			else {
				logger.level('trace');
			}

			loggers[loggerName] = logger;

			console.log('Created new logger named ' + loggerName + ' (log level ' + logger.level() + ') using log file ' + logger.logPath);

		}


		return loggers[loggerName];

	};	

module.exports.getLogger = getLogger;

