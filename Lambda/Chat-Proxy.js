/* strict parser. 
* With strict mode, you can not, for example, use undeclared variables.
*/
'use strict';


var AWS = require('aws-sdk'); // AWS SDK library

var dynamo = new AWS.DynamoDB();

var bucket = 'wsm-serverless-chat'; // Bucket name

/* exports object allows export functions for public use by other modules, 
* in this case, it is used by lambda execution framework.
* This function is designed to work in API Gateway's lambda proxy mode
* callback tells lambda that we are done and return the actual result
*/
exports.handler = function (event, context, callback) {

    const done = function (err, res) {
        // helper function with the same signature as callback itself
        callback(null, {
            statusCode: err ? '400' : '200',
            body: err ? JSON.stringify(err) : JSON.stringify(res),
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': 'http://wsm-serverless-chat.s3-website-us-west-1.amazonaws.com'  // 只允许s3 origin获得response的内容
            }
        });
    };
    // logging out information in log file for debugging
    console.log(context);

    var path = event.pathParameters.proxy;

    if (path == 'conversations' && event.httpMethod === 'GET') {
        dynamo.query({
            TableName: 'Chat-Conversations',
            IndexName: 'Username-ConversationID-index',
            Select: 'ALL_PROJECTED_ATTRIBUTES',
            KeyConditionExpression: 'Username = :username',
            ExpressionAttributeValues: {':username': {S: 'Student'}}
        }, function (err, data) {
            handleIdQuery(err, data, done, [], 'Student');
        });
    } else if (path.startsWith('conversations/')) {
        var id = path.substring('conversations/'.length);
        switch(event.httpMethod) {
            case 'GET':
                dynamo.query({
                    TableName: 'Chat-Messages',
                    ProjectionExpression: '#T, Sender, Message',
                    ExpressionAttributeNames: {'#T': 'Timestamp'},
                    KeyConditionExpression: 'ConversationID = :id',
                    ExpressionAttributeValues: {':id': {S: id}}
                }, function (err, data) {
                    loadMessages(err, data, id, [], done);
                });
                break;
            case 'POST':
                dynamo.putItem({
                    TableName: 'Chat-Messages',
                    Item: {
                        ConversationID: {S: id},
                        Timestamp: {
                            N: "" + new Date().getTime()
                        },
                        Message: {S: event.body},
                        Sender: {S: 'Student'}
                    }
                }, done);
                break;
            default:
                done('No cases hit');
                break;
        }
    } else {
        done('No cases hit');
    }
};

// loadMessages() construct json structure of a conversation
function loadMessages(err, data, id, messages, callback) {
    if (err === null) {
        data.Items.forEach(function (message) {
            messages.push({
                sender: message.Sender.S,
                time: Number(message.Timestamp.N), // DynamoDB returns timestamp as a string
                message: message.Message.S
            });
        });
        // Ensure that get all the messages including those not in the 1st page
        if (data.LastEvaluatedKey) {
            dynamo.query({
                TableName: 'Chat-Messages',
                ProjectionExpression: '#T, Sender, Message',
                KeyConditionExpression: 'ConversationID = :id', 
                ExpressionAttributeNames: {'#T': 'Timestamp'}, 
                ExpressionAttributeValues: {':id': {S: id}},
                ExclusiveStartKey: data.LastEvaluatedKey
            }, function (err, data) {
                loadMessages(err, data, id, messages, callback);
            });
        } else {
            loadConversationDetail(id, messages, callback);
        }
    } else {
        callback(err);
    }
}

function loadConversationDetail(id, messages, callback) {
    dynamo.query({
        TableName: 'Chat-Conversations',
        Select: 'ALL_ATTRIBUTES',
        KeyConditionExpression: 'ConversationID = :id',
        ExpressionAttributeValues: {':id': {S: id}}
    }, function (err, data) {
        if (err === null) {
            var participants = [];
            data.Items.forEach(function (item) {
                participants.push(item.Username.S);
            });

            callback(null, {
                id: id,
                participants: participants,
                // messages are sorted by timestamp, the last message is the newest message
                last: messages.length > 0 ? messages[messages.length-1].time : undefined,
                messages: messages
            });
        } else {
            callback(err);
        }
    });
}


function handleIdQuery(err, data, callback, ids, username) {
    console.log("Username query results: " + JSON.stringify(data));
    if (err === null) {
        data.Items.forEach(function (item) {
            ids.push(item.ConversationID.S);
        });

        if (data.LastEvaluatedKey) {
            dynamo.query({
                TableName: 'Chat-Conversations',
                IndexName: 'Username-ConversationID-index',
                Select: 'ALL_PROJECTED_ATTRIBUTES',
                KeyConditionExpression: 'Username = :username',
                ExpressionAttributeValues: {':username': {S: username}},
                ExclusiveStartKey: data.LastEvaluatedKey
            }, function (err, data) {
                handleIdQuery(err, data, callback, ids, username);
            });
        } else {
            loadDetails(ids, callback);
        }
    } else {
        callback(err);
    }
}

// 
function finished(convos) {
    for (var i = 0; i < convos.length; i++) {
        if (!convos[i].participants) {
            return false;
        }
    }
    return true;
}

function loadDetails(ids, callback) {
    console.log("Loading details");
    var convos = [];
    ids.forEach(function (id) {
        var convo = {id: id};
        convos.push(convo);
    });

    if(convos.length > 0) {
        convos.forEach(function (convo) {
            loadConvoLast(convo, convos, callback);
        });
    } else {
        callback(null, convos);
    }
}

function loadConvoLast(convo, convos, callback) {
    dynamo.query({
        TableName: 'Chat-Messages',
        ProjectionExpression: '#T',
        Limit: 1,  // Only return the most recent conversation
        ScanIndexForward: false,  // scan messages in indescending order
        KeyConditionExpression: 'ConversationID = :id',
        ExpressionAttributeNames: {'#T': 'Timestamp'},
        ExpressionAttributeValues: {':id': {S: convo.id}}
    }, function (err, data) {
        if (err === null) {
            if (data.Items.length === 1) {
                convo.last = Number(data.Items[0].Timestamp.N);
            }
            loadConvoParticipants(convo, convos, callback);
        } else {
            callback(err);
        }
    });
}

function loadConvoParticipants(convo, convos, callback) {
    dynamo.query({
        TableName: 'Chat-Conversations',
        Select: 'ALL_ATTRIBUTES',
        KeyConditionExpression: 'ConversationID = :id',
        ExpressionAttributeValues: {':id': {S: convo.id}}
    }, function (err, data) {
        if (err === null) {
            var participants = [];
            data.Items.forEach(function (item) {
                participants.push(item.Username.S);
            });
            convo.participants = participants;

            if (finished(convos)) {
                callback(null, convos);
            }
        } else {
            callback(err);
        }
    });
}