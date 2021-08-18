process.title = 'loadbalancer';
// Initialization 
// Config
const config = JSON.parse(Buffer.from(require('./config.js'), 'base64').toString());

// Settings
var broker = config.globalsettings.broker;
var mynodeid = config.mynodeid;
var logtopic = mynodeid+'/log';
var controltopic = mynodeid+'/control';
var datatopic = mynodeid+'/data';
var previousnode = config.previousnode;
var previousnodecontroltopic = previousnode+'/control';
var nameid = config.nameid;
var pipelinetopic = nameid+'/broadcast'
var messagequeuelimit = config.appsettings.MQMaxThreshold;
var upperthreshold = config.appsettings.MQMaxPerfThreshold;
var lowerthreshold = config.appsettings.MQMinPerfThreshold;
var queuesplicevalue = config.appsettings.queuesplicevalue;
var maxnominalClients = config.appsettings.maxnominalClients;
var kubectlproxy = config.kubeproxy.split(":");
var namespace = config.namespace;
var deployment;
var executiontimeout = config.appsettings.executiontimeout;
var scaleTimeout = config.appsettings.scaleTimeout;
var logmode = config.appsettings.logmode;

// Modules
const mqttmod = require('mqttmod');
const l = require('mqttlogger')(broker, logtopic, mqttmod, logmode);

// Variables
var readyresponse = '{"node":"'+mynodeid+'","name":"loadbalancer","request":"ready"}';
var terminatingresponse = '{"node":"'+mynodeid+'","name":"loadbalancer","request":"terminating"}';
var init = 0;
var halt = 1;
var appmodules = ['emitter','filter','loadbalancer','trilaterator','aggregator'];
var livemodules = [];
var messageQueue = [];
var clientQueue = [];
var clients = [];
var scaleTimestamp = 0
var scaleUpTrigger = 0;
var scaleDownTrigger = 0;
var clientQueueDelTrigger = 0;
var clientsDelTrigger = 0;
//var pointerA = 0;

// Functions
function filterRequests(payload){
	try {
		data = JSON.parse(payload);
    } catch (e) {
        l.error('Received not valid JSON.\r\n'+payload);
		return false;
    }
	var requestingNode = data.node;
	var requestingNodePid = data.pid;
	var requestingNodeName = data.name;
	if (requestingNode != mynodeid) {
		var checkNode = 0;
		switch(data.request) {
			case 'ready':
				if (livemodules.length < appmodules.length) {
					var alpha = -1;
					var beta = 0
					for(var i = 0; i < appmodules.length; i++){
						alpha = appmodules.indexOf(requestingNodeName);
						if (alpha > -1) {
							for(var ii = 0; ii < livemodules.length; ii++){
								if (livemodules[ii].name == requestingNodeName) {
									beta = 1;
								}
							}
						}
					}
					if (alpha > -1 && beta == 0) {
						if (requestingNodeName == 'trilaterator') {
							livemodules.push({"node":requestingNode,"pid":data.pid,"name":requestingNodeName});
							deployment = data.deployment;
							mqttmod.send(broker,requestingNode+'/'+data.pid+'/control',readyresponse);
						} else {
							livemodules.push({"node":requestingNode,"name":requestingNodeName});
							mqttmod.send(broker,requestingNode+'/control',readyresponse);
						}
						l.info('Node '+requestingNode+' reported that is ready');
						l.info('Informing the new nodes that local node is ready');
						console.log(livemodules);
					} 
					if (alpha > -1 && beta == 1) {
						l.info('A '+requestingNodeName+' node already exists');
					}
					if (alpha == -1) {
						l.info(requestingNodeName+' node is not valid');
					}
				}
				if (livemodules.length == appmodules.length) {
					if (init == 0 && halt == 1) {
						halt = 0;
						l.info('All modules ready');
					}
					if (init == 1 && halt == 1){
						halt = 2;
						l.info('All modules ready');
					}
					if (requestingNodeName == 'trilaterator' && init == 1 && halt == 0) {
						for(var i = 0; i < livemodules.length; i++){
								if (livemodules[i].name == requestingNodeName && livemodules[i].node == requestingNode && livemodules[i].pid != data.pid) {
									mqttmod.send(broker,requestingNode+'/'+data.pid+'/control',readyresponse);
								}	
						}
					}
				}
			break;
			case 'execute':
				if (init == 0 && halt == 0) {
					mqttmod.send(broker,previousnodecontroltopic,payload);
					init = 1;
					l.info('Starting application');
				} else if (init == 1 && halt == 2) {
					mqttmod.send(broker,previousnodecontroltopic,payload);
					halt = 0;
					l.info('Restarting application');
				} else {
					l.info('Not all modules are loaded');
				}
			break;
			case 'terminating':
				for(var i = 0;i < livemodules.length;i++){ 
					if (livemodules[i].name == requestingNodeName && livemodules[i].node == requestingNode) { 
						switch(requestingNodeName) {
							case 'trilaterator':
								if ( data.pid == livemodules[i].pid) {
									livemodules.splice(i,1);
								} else {
									
								}
							break;
							default:
								livemodules.splice(i,1);
						}
					}
				}
				if (livemodules.length < appmodules.length) {
					l.info('Node '+requestingNode+' reported that is terminating, halt application.');
					halt = 1;
				}
			break;
			case 'join':
				for(var i = 0; i < clients.length; i++){ 
					if (clients[i].node == requestingNode && clients[i].pid == requestingNodePid) {
						clientQueue.push({"node":requestingNode,"pid":requestingNodePid});
						checkNode = 1;
						findClient();
						//l.info('Node '+requestingNode+ ' with pid '+requestingNodePid+' is now available again');
					}
				}
				if (checkNode == 0) {
					l.info('Suddenly a new pod appears: '+requestingNode+'/'+requestingNodePid);
					clientQueue.push({"node":requestingNode,"pid":requestingNodePid});
					clients.push(data);
					scaleUpTrigger = 0;
				}
			break;
			case 'leaving':
				if (requestingNodePid) {
					l.info('Pod: '+requestingNode+'/'+requestingNodePid+' is leaving');
					for(var i = 0; i < clients.length; i++){ 
						if ( clients[i].node == requestingNode && clients[i].pid == requestingNodePid) { 
							clients.splice(i, 1); 
							l.info('Removed from clients array');
							clientsDelTrigger = 1;
						}   
					}
					for(var i = 0; i < clientQueue.length; i++){ 
						if ( clientQueue[i].node == requestingNode && clientQueue[i].pid == requestingNodePid) { 
							clientQueue.splice(i, 1);						
							l.info('Removed from clients queue');
							clientQueueDelTrigger = 1;
						}
						if (i == clientQueue.length && clientQueueDelTrigger == 0) {
							l.info('Not present in clients queue');
						}
					}
					if (clientsDelTrigger == 1) {
						scaleDownTrigger = 0;
						clientQueueDelTrigger = 0;
						clientsDelTrigger = 0;
						l.info('Client removed');
					}
				}
			break;
			default:
				l.info('Didn\'t receive a valid request');
		}
	}
}

function regulateConsumers(){

	var connectedClients = clients.length;
	var nominalClients = 2*connectedClients;
	var scalepods = 0;
	l.info('Connected clients: '+connectedClients);
	l.info('Nominal clients: '+nominalClients);
	l.info('Message queue length is: '+messageQueue.length);
	
	if (scaleUpTrigger == 1){
		l.info('A scale up is in progress, waiting for the new client to appear');
	}
	
	if (scaleDownTrigger == 1){
		l.info('A scale down is in progress, removing client');
	}
	
	if ((scaleUpTrigger == 1 || scaleDownTrigger == 1) && ((Date.now()-scaleTimestamp) > scaleTimeout)) {
		l.info('Last scaling attempt failed');
		l.info('Reseting scale triggers');
		scaleUpTrigger == 0;
		scaleDownTrigger == 0;
		l.info('Check next node just in case');
	}

	if (messageQueue.length>nominalClients*upperthreshold && nominalClients <= maxnominalClients && scaleUpTrigger == 0 && scaleDownTrigger == 0){

		scalepods = connectedClients + 1;
		scaleUpTrigger = 1;
		l.info('Scaling up');
		l.debug('Scalepods: '+scalepods);
	}

	if (messageQueue.length <= nominalClients*lowerthreshold && connectedClients > 1 && scaleUpTrigger == 0 && scaleDownTrigger == 0){
		scalepods = connectedClients - 1;
		scaleDownTrigger = 1;
		l.info('Scaling down');
	}
	
	if (scalepods > 0) {
		try{
			scaleTimestamp = Date.now();
			kubepatch(scalepods);
			l.debug('Connected clients: '+connectedClients);
			l.debug('Scaling to: '+scalepods);
			l.debug('Timestamp is: '+scaleTimestamp);
		}catch(err){
			l.debug('Scaling to '+scalepods+' failed, returning');
			scalepods = 0;
			if (scaleUpTrigger = 1) {
				scaleUpTrigger = 0;
			}
			if (scaleDownTrigger = 1) {
				scaleDownTrigger = 0;
			}			
		}
	} else {
		l.debug('Scalepods is: '+scalepods);
	}
}

function kubepatch(pods) {
	var http = require("http");	
	var options = {
	  "method": "PATCH",
	  "hostname": ""+kubectlproxy[0]+"",
	  "port": ""+kubectlproxy[1]+"",
	  "path": "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deployment+"",
	  "headers": {
		"content-type": "application/strategic-merge-patch+json"
	  }
	};
	var req = http.request(options, function (res) {
		var chunks = [];
		l.debug('Building request header');
		res.on("data", function (chunk) {
			chunks.push(chunk);
		});
		l.debug('Building data payload');
		res.on("end", function () {
			var body = Buffer.concat(chunks);
		});
	});
	l.debug('Sending now to kubectl http proxy');
	req.write('{"spec":{"replicas": '+pods+'}}');
	req.end();
}

function filterResults(payload) {
	if (halt == 0) {
		var results = JSON.parse(payload);
		//resultsCounter += results.length;
		//l.info('Adding '+results.length+' results to queue, queue now has '+resultsCounter+' items');
		l.info('Adding '+results.length+' results to queue, queue now has '+messageQueue.length+' items at: '+Date.now());
		//pointerA = pointerA + results.length;
		Array.prototype.push.apply(messageQueue,results);
	}
}

function sendData (results,client) {
	l.info('Sending payload of '+JSON.parse(results).length+' results to node '+client.node+' and to client with pid '+client.pid+' at: '+Date.now());
	nextnodedatatopic = client.node+'/'+client.pid+'/data';
	mqttmod.send(broker,nextnodedatatopic,JSON.stringify(results));
};

function findClient () {
	while (messageQueue.length > 0 && clientQueue.length > 0 && scaleDownTrigger == 0){
		var client = clientQueue.shift();
		var message = messageQueue.shift();
		//mqttmod.send(broker,nextnodedatatopic,JSON.stringify(message));
		sendData(message,client);
		message = null;
		client = null;
		//var message = getRow(sendData,client);
		//l.info('Sending payload to node '+client.node+' and to client with pid '+client.pid);
	}
}

function heapCheck () {
	var usage = '';
	const used = process.memoryUsage();
	//console.log(used);
	for (let key in used) {
		usage = usage.concat(`${key} ${Math.round(used[key] / 1024 / 1024 * 100) / 100} MB, `);
		if (key == 'external') {
			usage=usage.slice(0, -2);
			l.info('Heap usage: '+usage);
		}
		if (key == 'rss') {
			nowrss =  Math.round((used[key] / 1024 / 1024 * 100) / 100);
		}
	}
}

// Begin execution
livemodules.push({"node":mynodeid,"name":"loadbalancer"});

// Start recieving control MQTT messages
l.info('Started recieving control MQTT messages on '+controltopic);
mqttmod.receive(broker,controltopic,filterRequests);	

// Start recieving data MQTT messages
l.info('Started recieving data MQTT messages on '+datatopic);
mqttmod.receive(broker,datatopic,filterResults);

// Start recieving control MQTT messages
l.info('Started receiving control messages on '+pipelinetopic);
mqttmod.receive(broker,pipelinetopic,filterRequests);

// Inform all nodes that you are ready
mqttmod.send(broker,pipelinetopic,readyresponse);

// Main loop
var interval = setInterval(function(){
	if (halt == 0) {
		var nextnodedatatopic;
		heapCheck();
		l.info('Live clients before assigning new jobs: '+clientQueue.length);
		findClient();
		l.info('Live clients after assigning new jobs: '+clientQueue.length);
		//safeguard in case that noone receives the messages and they are stacked in memory
		if (messageQueue.length > messagequeuelimit){
			l.info('Queue is full, the length is:'+messageQueue.length);
			let oldmessages = messageQueue.splice(0,queuesplicevalue);
			l.info('Removed old messages, the new queue length is:'+messageQueue.length);
		}
		regulateConsumers();
	};
}, executiontimeout);

process.on('SIGTERM', function onSigterm () {
	l.info('Got SIGTERM');
	mqttmod.send(broker,pipelinetopic,terminatingresponse);
});