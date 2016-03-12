'use strict';

/**
*	Endpoints for managing data pipes
*	@Author: David Taieb
*/

var pipesSDK = require('simple-data-pipe-sdk');
var pipesDb = pipesSDK.pipesDb;
var global = require('bluemix-helper-config').global;
var webSocket = require('ws');
var webSocketServer = webSocket.Server;
var _  = require('lodash');
var pipeRunner = require('./pipeRunner');
var connectorAPI = require('./connectorAPI');
var passportAPI = require('./passportAPI');
var nodeStatic = require('node-static');
var sdpLog = pipesSDK.logging.getGlobalLogger();
var util = require('util');

module.exports = function( app ){

	//Configure passport
	passportAPI.initEndPoints(app);
	
	//Private APIs
	var getPipe = function( pipeId, callback, noFilterForOutbound ){
		pipesDb.getPipe( pipeId, function( err, pipe ){
			if ( err ){
				return callback( err );
			}
			
			callback( null, pipe );			
		}.bind(this), noFilterForOutbound || false);
	}.bind(this);
	
	/**
	 * Get list of existing data pipes
	 */
	app.get('/pipes', function(req, res) {
		pipesDb.listPipes( function( err, pipes ){
			if ( err ){
				return global.jsonError( res, err );
			}
			return res.json( pipes );
		});
	});
	
	function validatePipePayload( pipe ){
		var requiredFields = ['name', 'connectorId'];
		if ( pipe.hasOwnProperty('new') ){
			delete pipe['new'];
		}else{
			//Ask the connector for required field
			var connector = connectorAPI.getConnector( pipe );
			if ( !connector ){
				throw new Error('Unable to get connector information for pipe');
			}
			
			if ( !!connector.getOption('useOAuth') ){
				requiredFields.push('clientId');
				requiredFields.push('clientSecret');
			}
			
			if ( connector.getOption('extraRequiredFields') ){
				var extraReq = connector.getOption('extraRequiredFields');
				if ( !_.isArray(extraReq )){
					extraReq = [extraReq];
				}
				_.forEach( extraReq, function(field){
					requiredFields.push(field);
				});
			}
		}
		
		requiredFields.forEach( function( field ){
			if ( !pipe.hasOwnProperty( field ) ){
				throw new Error('Missing field ' + field);
			}
		});
	}
	
	/**
	 * Create/Save a data pipe
	 */
	app.post('/pipes', function(req, res ){
		var pipe = req.body;
		try{
			validatePipePayload( pipe );
		}catch( e ){
			return global.jsonError( res, e );
		}
		pipesDb.savePipe( pipe, function( err, pipe ){
			if ( err ){
				return global.jsonError( res, err );
			}
			sdpLog.info('Data Pipe configuration ' + pipe.name + ' was saved.');
			res.json( pipe );
		});
	});
	
	/**
	 * Delete
	 */
	app.delete('/pipes/:id', function( req, res ){
		pipesDb.removePipe( req.params.id, function( err, pipe ){
			if ( err ){
				return global.jsonError( res, err );
			}
			passportAPI.removeStrategy(pipe._id);
			sdpLog.info('Data Pipe configuration ' + req.params.id + ' was deleted.');
			res.json( pipe );
		});
	});
	
	/**
	 * Returns the last 10 runs
	 */
	app.get('/runs', function( req, res ){
		pipesDb.run( function( err, db ){
			db.view( 'application', 'all_runs', 
					{startkey: [{}, req.params.pipeid], endKey:[0, req.params.pipeid],'include_docs':true, 'limit': 10, descending:true},
				function(err, data) {
					if ( err ){
						sdpLog.error('Pipe run list could not be retrieved: ', err);						
						//No runs yet, return empty array
						return res.json( [] );
					}
					return res.json( injectDbUrl(data.rows) );
				}
			);
		});
	});
	
	/**
	 * Returns the last 10 runs for given pipe
	 */
	app.get('/runs/:pipeid', function( req, res ){
		pipesDb.run( function( err, db ){
			db.view( 'application', 'all_runs_for_pipe', 
					{key: req.params.pipeid,'include_docs':true, 'limit': 10, descending:true},
				function(err, data) {
					if ( err ){
						sdpLog.error('Pipe run list could not be retrieved for pipe ' + req.params.pipeid + ': ', err);
						//No runs yet, return empty array
						return res.json( [] );
					}
					return res.json( injectDbUrl(data.rows) );
				}
			);
		});
	});
	
	/*
	 * @param rows - a set of pipe run documents
	 */
	var injectDbUrl = function(rows) {

		var baseUrl = null;
		var storageUrl = null; // Cloudant staging database URL 
		var logUrl = null;     // Data Pipe run log download URL

		if (pipesDb.storageDb && pipesDb.storageDb.config) {

			baseUrl = pipesDb.storageDb.config.url ? pipesDb.storageDb.config.url : null;
			
			if (baseUrl) {
				//strip credential from url (e.g., http://username:password@localhost)
				baseUrl = baseUrl.replace(/:\/\/\S+:\S+@/i,'://');

				storageUrl = baseUrl;

				// compose run log URL string (same format used by Couch and Cloudant)
				// $BASE_URL/$DATABASE/$DOCUMENT_ID/$ATTACHMENT_NAME
				logUrl = baseUrl + '/' + pipesDb.storageDb.config.db + '/'; // /$DATABASE/

				if (storageUrl.indexOf('cloudant.com') > -1) {
					//cloudant.com
					storageUrl += '/dashboard.html#/database/{dbname}/_all_docs';
				}
				else {
					//couchdb
					storageUrl += '/_utils/database.html?{dbname}';
				}
			}
		}
		
		if (storageUrl) {

			var docProperty = null;

			var updateRows = _.forEach(rows, function(row, index) {
				docProperty = row.doc || row;

				// add link to storage database to run document
				docProperty.dbUrl = storageUrl;

				// if a run log is attached to the run doc, add the logUrl property, which can be used by the UI to display a download link
				if((docProperty['_attachments']) && (docProperty['_attachments']['run.log'])) {
					docProperty.logUrl = logUrl + row.id + '/run.log' ;	// append $DOCUMENT_ID/$LOG_FILE_ATTACHMENT_NAME 					
				}
				
			});
			return updateRows;
		}
		else {
			return rows;
		}
	};
	
	/**
	 * Private API for running a pipe
	 * @param pipeId: id of the pipe to run
	 * @param callback(err, pipeRun)
	 */
	var runPipe = function( pipeId, callback ){
		getPipe( pipeId, function( err, pipe ){
			if ( err ){
				return callback(err);
			}

			sdpLog.info('Running data pipe ', pipe.name);
			var doRunInstance = function(){
				var pipeRunnerInstance = new pipeRunner( pipe );			
				pipeRunnerInstance.newRun( function( err, pipeRun ){
					if ( err ){
						sdpLog.error('Run for Data Pipe ' + pipe.name + ' could not be started: ', err);
						//Couldn't start the run
						return callback(err);
					}
					return callback( null, pipeRun );
				});
			};
			
			if ( pipe.run ){
				//Check if the run is finished, if so remove the run
				pipesDb.getRun( pipe.run, function( err, run ){
					// i63
					if ( err || run.status == 'FINISHED' || run.status == 'STOPPED' || run.status == 'ERROR'){
						sdpLog.info('Pipe has a reference to a run that has already completed. OK to proceed...');
						//Can't find the run or run in a final state; ok to run
						return doRunInstance();
					}
					//Can't create a new run while a run for this pipe is already in progress
					var message = 'A run is already in progress for pipe ' + pipe.name;
					sdpLog.error(message);
					return callback( message );
				});
			}else{
				// prevent more than one concurrent pipe run
				if(global.currentRun) {
					//Can't create a new run while a run for another pipe is already in progress.
					var message = 'A run is already in progress for another pipe.';
					sdpLog.error(message);
					return callback( message );
				}
				else {
					doRunInstance();
				}
			}
			
		}, true);
	};

	//Default FileServer pointing at the built-in template files
	var defaultFileServer = new nodeStatic.Server('./app/templates');
	app.get('/template/:pipeId/:tab', function( req, res ){
		//Get the connector for this pipeid
		connectorAPI.getConnectorForPipeId( req.params.pipeId, function( err, connector ){
			if ( err ){
				sdpLog.error('Could not find connector for ' + req.params.pipeId + ' : ', err);
				if (req.params.tab !== 'settings') {
					defaultFileServer.serveFile('connectorError.html', 200, {}, req, res);
					return;
//					return global.jsonError( res, err );
				}
			}
			
			if (req.params.tab === 'settings') {
				//allow 'settings' page to be served at all times
				defaultFileServer.serveFile('pipeSettings.html', 200, {}, req, res);
			}
			else {
				//The filename to look for (can come from the connector or the default location)
				var fileName = 'pipeDetails.' + req.params.tab + '.html';
				
				//Try the connectorsFileServer first
				connector.fileServer = connector.fileServer || new nodeStatic.Server( connector.path);
				connector.fileServer
					.serveFile( 'templates/' + fileName, 200, {}, req, res )
					.on('error',function(err){
						//console.log('Not able to serve from connectorsFileServer: ' + err );
						defaultFileServer.serveFile(fileName, 200, {}, req,res);
					 });
			}
		});
	});
	
	/**
	 * Start a new pipe run
	 * @param pipeId: id of the pipe to run
	 */
	app.post('/runs/:pipeId', function( req, res ){
		runPipe( req.params.pipeId, function( err, pipeRunDoc){
			if ( err ){
				return global.jsonError( res, err );
			}
			//Return a 202 accepted code to the client with information about the run
			return res.status( 202 ).json( pipeRunDoc.getId() );
		});
	});
	
	/**
	 * Connect to connector data source
	 */
	app.get('/connect/:id', function( req, res){
		getPipe( req.params.id, function( err, pipe ){
			if ( err ){
				return global.jsonError( res, err );
			}
			var connector = connectorAPI.getConnector( pipe );
			if ( !connector ){
				return global.jsonError('Unable to get Connector for ' + pipe.connectorId );
			}
			
			var passportStrategy = null;
			if (typeof connector.getPassportStrategy === 'function') {
				passportStrategy = connector.getPassportStrategy(pipe);
			}
			
			if (passportStrategy) {
				//use Passport to handle authentication
				passportAPI.addStrategy(pipe._id, passportStrategy);
				
				if (req.session) {
					req.session.state = {
						                  url: req.query.url
						                };
				}
				var passportAuthUrl = '/auth/passport/' + pipe._id;
				res.redirect(passportAuthUrl);
			}
			else {
				//connector will handle authentication
				connector.connectDataSource( req, res, pipe._id, req.query.url, function( err, results ){
					if ( err ){
						return global.jsonError( res, err );
					}
					return res.json( results );
				});
			}
		});
	});
	
	/**
	 * Common OAuth authentication processing endpoint.
	 * @param {Object} req - request, which must contain the following query parameters:
	 *                       code: OAuth code for data_pipe_id
	 *                       state: {pipe: data_pipe_id}
	 * @param {Object} res - response 
	 */
	app.get('/authCallback', function( req, res ){

		sdpLog.info('Starting OAuth callback processing.');

		// OAuth code
		var code = req.query.code || req.query.oauth_verifier;

		// state parameter
		var state = null;
		
		if (req.query.state) {
			state = JSON.parse(req.query.state);
		}
		else if (req.session && req.session.state) {
			state = JSON.parse(req.session.state);
		}

		// The Data Pipe ID should be included in the state parameter
		var pipeId = null;
		
		if (state) {
			pipeId = state.pipe;
		}
		
		if ( !pipeId ){
			sdpLog.error('No data pipe ID was included in OAuth callback parameter state.');
			sdpLog.info('FFDC query: ' + util.inspect(req.query,4));
			sdpLog.info('FFDC session: ' + util.inspect(req.session,4));
			return global.jsonError( res, 'No data pipe ID was included in OAuth callback parameter state.');
		}

		if(! code) {
			sdpLog.error('No code was found in OAuth callback request.');
			return global.jsonError( res, 'No code was found in OAuth callback request.');
		}
		
		// fetch data pipe configuration
		getPipe( pipeId, function( err, pipe ){
			if ( err ){
				sdpLog.error('Data pipe configuration for pipe ' + pipeId + ' could not be loaded: ' + err);
				return global.jsonError( res, 'Data pipe configuration for pipe ' + pipeId + ' could not be loaded: ' + err);
			}
			var connector = connectorAPI.getConnector( pipe );
			if ( !connector ){
				sdpLog.error('No suitable connector was found for data pipe ' + pipeId);
				return global.jsonError( res, 'No suitable connector was found for data pipe ' + pipeId);
			}
			
			var passportStrategy = null;
			if (typeof connector.getPassportStrategy === 'function') {
				passportStrategy = connector.getPassportStrategy(pipe);
			}
			
			if (passportStrategy) {
				// let Passport handle OAuth processing
				passportAPI.authCallback(req, res, function(err, pipe) {

					if(err) {
						sdpLog.error('Passport OAuth processing failed: ' + err);
						return global.jsonError( res, 'Passport OAuth processing failed: ' + err);
					}

					if(! pipe) {
						// this indicates a bug in the implementation: passportAPI.authCallback must return a data pipe config
						// unless an error was returned
						sdpLog.error('Passport OAuth processing failed: no data pipe configuration was returned');
						return global.jsonError( res, 'Passport OAuth processing failed: no data pipe configuration was returned');
					}

					//Save the data pipe configuration
					pipesDb.savePipe( pipe, function( err, data ){
						if ( err ){
							return global.jsonError( res, err );
						}

						res.redirect(state.url);
					});


				}); 
			}
			else {
				// let connector implementation handle OAuth processing
				connector.authCallback( code, pipeId, function( err, pipe ){
					if ( err ){
						return res.type('html').status(401).send('<html><body>' +
							'Authentication error: ' + err +
							'</body></html>');
					}
					
					//Save the data pipe configuration
					pipesDb.savePipe( pipe, function( err, data ){
						if ( err ){
							return global.jsonError( res, err );
						}

						res.redirect(state.url);
					});
				}, state);
			}

		}); // getPipe
	}); // app.get(/authCallback)
	
	//Catch all for uncaught exceptions
	process.on('uncaughtException', function( err ){
		//Something terribly wrong happen within the code, catch it here so we don't crash the app
		if ( global.currentRun ){
			// a pipe run was in progress; clean up
			sdpLog.error('Simple Data Pipe: Unexpected exception caught while processing data pipe ' + global.currentRun.runDoc.pipeId + ': ', err );
			// console.log(err.stack || 'No stack available');
			// the log gets saved in the run doc
			global.currentRun.done(err);
		}
		else {
			// no pipe run was in progress
			sdpLog.error('Simple Data Pipe: Unexpected exception caught. No pipe run was in progress: ' + err); 
			sdpLog.error(err.stack || 'No stack available');
		}	

		if(global.getLogger('sdp_pipe_run')) {
			sdpLog.info('Simple Data Pipe run log file is located in ' + global.getLogger('sdp_pipe_run').logPath);
		}

	});
	
	//Listen to scheduled event runs
	global.on('runScheduledEvent', function( pipeId){
		runPipe( pipeId, function( err, run ){
			if ( err ){
				sdpLog.error('Unable to execute a scheduled run for pipe ' + pipeId + ': ', err);
				return;
			}
			sdpLog.info('New Scheduled run started for pipe %s', pipeId);
		});
	});
	
	//Returns a function that configure the webSocket server to push notification about runs
	return function(server){
		var wss = new webSocketServer({
			server: server,
			path:'/runs'
		});
		
		global.on('runEvent', function( runDoc ){
			_.forEach( wss.clients, function( client ){
				if ( client.readyState === webSocket.OPEN){
					client.send( JSON.stringify( runDoc ) );
				}
			});
		});
		
		wss.on('connection', function(ws) {
			//Send response with current run
			ws.send( global.currentRun && JSON.stringify(global.currentRun.runDoc ));

			ws.on('close', function() {
				//console.log('Web Socket closed');
			});
		});
	};
};
