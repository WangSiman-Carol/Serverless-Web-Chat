'use strict';

const uuidV4 = require('uuid/v4');  // DynamoDB (NoSQL) can't increment id like relational db

var AWS = require('aws-sdk');

var dynamo = new AWS.DynamoDB();

exports.handler = function (event, context, callback) {
    var id = uuidV4();
    var users = event.users;
    users.push(event.cognitoUsername);
    var records = [];
    users.forEach(function(user) {
        records.push({
            PutRequest: {
                Item: {
                    ConversationID: {
                        S: id
                    },
                    Username: {
                        S: user
                    }
                }
            }
        });
    });

    dynamo.batchWriteItem({
        RequestItems: {
            'Chat-Conversations': records
        }
    }, function (err, data) {
        if(err === null) {
            callback(null, id);
        } else {
            callback(err);
        }
    });
};