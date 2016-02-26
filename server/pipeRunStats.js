'use strict';

/**
*	Pipe Run Stats implementation
*	@Author: David Taieb
*/

var moment = require('moment');
//var sdpLog = require('./logging/sdpLogger.js').getLogger('sdp_common');
var pipesSDK = require('simple-data-pipe-sdk');
var pipesDb = pipesSDK.pipesDb;
var sdpLog = pipesSDK.logging.getLogger('sdp_common');
var _ = require('lodash');
var bluemixHelperConfig = require('bluemix-helper-config');
var global = bluemixHelperConfig.global;

/**
 * PipeRunStats class
 * encapsulate the stats for a particular run
 */
function pipeRunStats(pipe, steps, callback){
	this.pipe = pipe;
	var logger = this.logger = global.getLogger('sdp_pipe_run');

	var debug_env_var = bluemixHelperConfig.configManager.get('DEBUG') || '';
	if((debug_env_var.match(/^[\s]*\*[\s]*$/i)) || (debug_env_var.match(/[\s,]+\*[\s,]*/i)) || (debug_env_var.match(/[\s]*sdp_pipe_run[\s]*/i))) {
		// enable lowest level of logging for data pipe runs if the DEBUG environment variable is set
		logger.level('trace');
	}
	else {
		logger.level('info');
	}

	// counts the number of pending run doc updates that have not yet been stored in the repository
	var runDocSavesInProgress = 0;

	sdpLog.info('Simple Data Pipe run log level for ' + pipe.connectorId + ' pipe ' + pipe.name + '(' + pipe._id + ') is ' + logger.level());

	var runDoc = this.runDoc = {
		type : 'run',
		connectorId : pipe.connectorId,
		startTime : moment(),
		pipeId: pipe._id,
		status: 'NOT_STARTED',
		error: null,
		tableStats : {},
		message:''
	};
	
	for ( var i = 0; i < steps.length; i++ ){
		//Assign a space for this step
		steps[i].stats = {
			label: steps[i].label,
			status: 'NOT_STARTED',
			error: ''
		};
		
		runDoc['step' + i] = steps[i].stats;
	}
	
	var save = this.save = function(callback, outerError){

		//sdpLog.debug('Saving run document for pipe ' + pipe._id);

		runDocSavesInProgress++;

		//Create a new run doc and associate it with this pipe
		pipesDb.saveRun( pipe, runDoc, function( err, runDocument ){

			runDocSavesInProgress--;

			if ( err ){
				sdpLog.error('Run information for pipe ' + pipe._id + ' could not be saved: ', err);
				return callback && callback( err );
			}
			//Replace with the latest doc from db
			runDoc = runDocument;
			broadcastRunEvent();
			return callback && callback( outerError );
		});
	};
	
	var broadcastRunEvent = this.broadcastRunEvent = function(event){
		global.emit('runEvent', global.currentRun && global.currentRun.runDoc );
	};
	
	//Initial save
	save( callback );
	
	//Public apis
	this.getId = function(){
		return runDoc._id;
	};
	
	this.getPipe = function(){
		return this.pipe;
	};

	
	this.setMessage = function( message ){
		runDoc.message = message || '';
		broadcastRunEvent();		
	};
	
	this.getTableStats = function(){
		return runDoc.tableStats;
	};
	
	this.addTableStats = function( stats ){
		if ( runDoc.tableStats.hasOwnProperty( stats.tableName )){
			//Merge the two objects
			_.assign( runDoc.tableStats[stats.tableName], stats );
		}else{
			runDoc.tableStats[stats.tableName] = stats;
		}
		
		//Save the document
		save();
	};	
	
	/**
	 * start: Called when a run is about to start
	 */
	this.start = function( callback ){

		sdpLog.info('Starting a new pipe run');
		
		//Set the current run to this
		if ( global.currentRun ){
			// i63
			var msg = 'A run is already in progress ' + global.currentRun._id;
			sdpLog.error( msg );
			return callback( msg );
		}
		global.currentRun = this;
		broadcastRunEvent();
		
		runDoc.startTime = moment();
		runDoc.status = 'RUNNING';
		
		//Add the run id to the pipe to signify that it is running
		if ( pipe.run !== runDoc._id ){
			pipesDb.upsert( pipe._id, function( storedPipe ){
				storedPipe.run = runDoc._id;
				pipe = storedPipe;
				return storedPipe;
			}, function( err ){
				if(err) {
					sdpLog.error('The pipe run document could not be updated: ', err );
					return callback( err );					
				}
				return callback();

			});
		}else{
			return callback();
		}
	};
	
	/**
	 * done: called when a run is completed
	 */
	this.done = function(err ){
		global.currentRun = null;
		if ( err ){
			sdpLog.error('The pipe run resulted in an error: ', err );
			if ( err.stack ){
				sdpLog.error( err.stack );
			}
			runDoc.status = 'ERROR';
			runDoc.message = '' + err;
		}else{
			runDoc.status = 'FINISHED';
			runDoc.message = 'Pipe run completed';	// steps might have produced warnings, hence a completed run is not always a successful run
		}
		
		runDoc.endTime = moment();
		runDoc.elapsedTime = moment.duration( runDoc.endTime.diff( runDoc.startTime ) ).humanize();
		
		//compute the number of records processed
		runDoc.numRecords = 0;
		_.forEach( runDoc.tableStats, function( value, key){
			if ( value && value.numRecords ){
				runDoc.numRecords += value.numRecords;
			}
		});
		
		sdpLog.info( runDoc.message );
		
		//Save the document
		save();
		
		//Remove the run from the pipe
		pipesDb.upsert( pipe._id, function( storedPipe ){
			if ( storedPipe && storedPipe.hasOwnProperty('run') ){
				delete storedPipe['run'];
			}
			return storedPipe;
		}, function( err ){
			if ( err ){
				sdpLog.error('Unable to remove reference to run in pipe %s. Error is %s ', pipe._id, err );
			}
			sdpLog.info({
				message: 'Pipe run completed',
				runDoc: runDoc
			});
			
			/*
			 * Wait until all run document updates have been saved before trying to attach the run log file.
			 * Maximum wait time is stopWaitCounter * sleepInterval (~10 minutes)
			 * 
			 */
			var stopWaitCounter = 60 * 10;  // wait is aborted if this counter reaches 0
			var sleepInterval = 1000;		// wake-up every sleepInterval in ms

			var updateAfterAllSavesComplete = 	
			    setInterval( function(){
			    						stopWaitCounter--;

			    						// runDocSavesInProgress counter is updated by the save() function, which updates the run document
										if(runDocSavesInProgress < 1)	{
											// all run document updates were saved; stop waiting
											clearInterval(updateAfterAllSavesComplete);	

											// attach run log file to run document in Cloudant repository
											pipesDb.attachLogFileToRun( logger.logPath, 
																		runDoc, 
																		function(err){
																			if ( err ){
																				sdpLog.error('Unable to attach log file %s to run document %s: %s', 
																					         logger.logPath, runDoc._id, err );
																			}
																			else {
																				sdpLog.info('Attached log file %s to run document %s.',
																			         		logger.logPath, runDoc._id);
																			}
																		});
										}
										else {
											// there are still run document updates pending
											if(stopWaitCounter < 1) {
												// stop waiting; no attempt will be made to attach the log file
												clearInterval(updateAfterAllSavesComplete);	
												// log error condition
												sdpLog.error('There are too many run document updates pending. The run log file %s will not be attached to the run document %s.', 
															 logger.logPath, runDoc._id);
											}
											else {
												// issue informational message every (sleepInterval * 5) ms
												if(stopWaitCounter % 5 === 0) {
													sdpLog.info('Waiting for remaining ' + runDocSavesInProgress + ' run doc updates to be saved.');	
												}
											}
										}
				}, sleepInterval); 


			/*    
				//Save the log file as an attachment to the run
				pipesDb.attachLogFileToRun( logger.logPath, runDoc, function(err){
					if ( err ){
						sdpLog.error('Unable to attach log file %s to run document %s : %s', logger.logPath, runDoc._id, err );
					}				
				});
			*/
		});
		
		broadcastRunEvent();
	};
}

module.exports = pipeRunStats;