'use strict';

/**
*	Endpoints for managing data pipes connectors
*	@Author: David Taieb
*/

var fs = require('fs');
var bluemixHelperConfig = require('bluemix-helper-config');
var configManager = bluemixHelperConfig.configManager;
var vcapServices = bluemixHelperConfig.vcapServices;
var global = bluemixHelperConfig.global;

var pipesSDK = require('simple-data-pipe-sdk');
var connector = pipesSDK.connector;
var _ = require('lodash');
var path = require("path");
var pipesDb = pipesSDK.pipesDb;

function connectorAPI(){
	
	var connectors = [];
	
	/**
	 * loadConnector: private API for loading a connector from a specified path
	 */
	var loadConnector = function( connectorPath ){
		try{
			var connector = require( connectorPath );
			//Determine the path for the connector which is the parent directory of the module main js file
			connector.path = path.join( require.resolve( connectorPath ), "..");
			//Read any custom tab controller provided by the connector
			readCustomControllers( path.join(connector.path, "controllers"), connector );
			return connector;
		}catch(e){
			console.log("Invalid connector found at location %s. Make sure to export an object that inherit from connector object", connectorPath );
			console.log(e.stack);
			return null;
		}
	}
	
	/**
	 * readCustomController: private API for reading controllers provided by a custom connector
	 */
	var readCustomControllers = function( controllersPath, connector ){
		var customControllers = connector.getOption("customControllers") || {};
		if ( fs.existsSync(controllersPath) ){
			var files = fs.readdirSync( controllersPath);
			files = _.chain( files )
			.filter( function( file ){
				var ret = fs.lstatSync( path.join( controllersPath, file ) ).isFile() && _.endsWith(file, ".js");
				return ret;
			})
			.map(function(file){
				//Extract the tab name from the file name. convention is <<tab>.js
				var tabName = file.substring(0, file.length - ".js".length );
				customControllers[tabName] = fs.readFileSync(path.join( controllersPath, file ), "utf8" );
			})
			.value()
			
			connector.setOption("customControllers", customControllers);
		}
	}
	
	//load connectors with source installed within the main app
	var connectorPath = path.join( __dirname, "connectors" );
	fs.readdir( connectorPath, function(err, files){

		if (! err ){
			console.log('Embedded connector repository is present. Searching for connectors.');
			// Load pre-packaged connectors
			connectors = _.chain( files )
				.filter( function(file) {
											return fs.lstatSync(path.join(connectorPath, file)).isDirectory();
						})
				.map( function( file ){
										//Load the connector
										var connector = loadConnector( path.join(connectorPath, file) );
										if ( connector ){
											console.log("Loaded built-in connector", connector.getId());
										}
										return connector;
						})
				.filter( function( connector ){
										//One more pass to remove any invalid connector
										return connector != null;
						})
				.value();
		}

		//Load connectors installed as dependent node modules
		var npm = require("npm");
		npm.load({ parseable: true, depth:0 }, function (err, npm) {
			if (err) {
				return console.log("Unable to load npm programmatically: ", err);
			}
			npm.commands.ls([], true, function( err, data, lite){
				if (err) {
					return console.log("Error running npm ls: ", err);
				}

				//console.log("ls data: ", data)
				var declared_services = [];
				var service_dependencies_ok = true;
				_.forEach(data.dependencies, function(module, key ){

					if((module.hasOwnProperty("simple_data_pipe")) || (module.hasOwnProperty("pipes-connector-name"))) {
						// This module contains a Simple Data Pipe connector.
						
						if(! module.simple_data_pipe.name) {
							// Connectors without an id are not supported. Display error.
							console.log('Error: Module ' + module.name + ' does not define a connector name and will not be loaded.');
						}
						else {
								service_dependencies_ok = true;

								// Does this connector declare any services that must have been bound to this Simple Data Pipe application?	
								if((module.simple_data_pipe) && (module.simple_data_pipe.service_dependencies) && (module.simple_data_pipe.service_dependencies.length > 0)) {
									console.log('Debug: Connector ' + module.simple_data_pipe.name + ' in module ' + module.name + 
										        ' requires: ' + JSON.stringify(module.simple_data_pipe.service_dependencies));

									// verify for each service that an instance is bound to the application
									_.forEach(module.simple_data_pipe.service_dependencies, function(service, key){

										if((! service.service_name) || (! service.service_plan) || (! service.instance_name)) {
											// at least one mandatory property is missing
											console.log('Error: Connector ' + module.simple_data_pipe.name + ' in module ' + module.name + ' contains incomplete service declaration: ' + JSON.stringify(service));
											service_dependencies_ok = false;
										}
										else {
											// try to locate the required service among the bound services
											if(((! service.vcap_env_service_alias) && (! vcapServices.getService( service.instance_name))) ||
										   		(! vcapServices.getService( configManager.get(service.vcap_env_service_alias) || service.instance_name)))	{
													console.log('Error: Connector ' + module.simple_data_pipe.name + ' in module ' + module.name + ' requires service ' + JSON.stringify(service));
													service_dependencies_ok = false;
											}	
										}
									})
									
								}
						
								if(service_dependencies_ok) {
									// load connector
									var connector = loadConnector( module.path );
									if ( connector ){						
										console.log("Loaded npm dependency connector", connector.getId());
										connectors.push( connector );
									}
								}
								else {
									console.log('Error: Connector ' + module.simple_data_pipe.name + ' was not loaded due to missing service dependencies.');
								}							
						}
					}				
				})
			})
		});
	});
	
	//Public APIs
	/**
	 * InitEndPoints
	 * @param app: express app
	 */
	this.initEndPoints = function( app ){
		/**
		 * return list of registered connectors
		 */
		app.get('/connectors', function(req, res) {
			if ( !connectors ){
				return global.jsonError( res, "Unable to load registered connectors");
			}
			return res.json( connectors );
		});
	};
	
	/**
	 * getConnectorForPipeId: return the connector for the pipe identified by its id
	 * @param pipeId: id for the pipe
	 * @param callback(err, connector)
	 */
	this.getConnectorForPipeId = function( pipeId, callback ){
		pipesDb.getPipe( pipeId, function( err, pipe ){
			if ( err ){
				return callback(err);
			}
			var connector = this.getConnector( pipe );
			if ( !connector ){
				return callback( "Unable to get Connector for " + pipeId );
			}
			return callback( null, connector );
		}.bind(this));
	}
	
	/**
	 * getConnector: return the connector associated with a pipe
	 */
	this.getConnector = function(pipe){
		var connectorId = _.isString( pipe ) ? pipe : pipe.connectorId;
		return _.find( connectors, function( connector ){
			return connector.getId() === connectorId;
		});
	};
}

//Singleton
module.exports = new connectorAPI();