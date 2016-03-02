
'use strict';

var passport = require("passport");
var global = require('bluemix-helper-config').global;
var pipesSDK = require('simple-data-pipe-sdk');
var pipesDb = pipesSDK.pipesDb;
var connectorAPI = require("./connectorAPI");


var passportAPI = {
	
	addStrategy: function(name, strategy) {
		passport.use(name, strategy);
	},
	
	removeStrategy: function(name) {
		passport.unuse(name);
	},

	initEndPoints: function(app) {
		//callback during passport authentication
		app.get('/auth/passport/callback',
			function(req, res, next) {
				var pipeId = null;
				
				if (req.query.pipeId) {
					pipeId = req.query.pipeId;
				}
				else if (req.session && req.session.pipeId) {
					pipeId = req.session.pipeId;
				}
				
				if ( !pipeId ){
					return global.jsonError( res, 'No code or state specified in /auth/passport/callback request');
				}
				
				passport.authenticate(pipeId, { pipeId:pipeId }, function(err, user, info) {
					if (err) {
						return next(err);
					}
					else if (!user) {
						res.type("html").status(401).send("<html><body> Authentication error: " + err + " </body></html>");
					}
					else {
						pipesDb.getPipe( info.pipeId, function( err, pipe ) {
							if ( err ){
								return global.jsonError( res, err );
							}
							
							var connector = connectorAPI.getConnector( pipe );
							
							if ( !connector ){
								return global.jsonError( res, "Unable to find connector for " + pipeId)
							}
							
							connector.authCallback( info.oauth_access_token, info.pipeId, function( err, pipe ){
								if ( err ){
									return res.type("html").status(401).send("<html><body>" +
										"Authentication error: " + err +
										"</body></html>");
								}
								
								//Save the pipe
								pipesDb.savePipe( pipe, function( err, data ){
									if ( err ){
										return global.jsonError( res, err );
									}
									else if (req.query.url) {
										res.redirect(req.query.url);
									}
									else if (req.session.returnUrl) {
										res.redirect(req.session.returnUrl);
									}
								})
								
							}, info);
						});
					}
				})(req, res, next);
			}
		);

		//initiate passport authentication
		app.get('/auth/passport/:pipeid',
			function(req, res, next) {
				req.session.pipeId = req.params.pipeid;
				passport.authenticate(req.params.pipeid, { 
					pipeId:req.params.pipeid,
					callbackURL: global.getHostUrl() + "/auth/passport/callback"
				})(req, res, next);
			}
		);
		
		//nothing to be done - call callback
		passport.serializeUser(function(user, done) {
			done(null, user);
		});

		//nothing to be done - call callback
		passport.deserializeUser(function(obj, done) {
			done(null, obj);
		});
		
		app.use(passport.initialize());
		app.use(passport.session());
	}
};

module.exports = passportAPI;
