'use strict';

const AWS = require('aws-sdk');
var parseString = require('xml2js').parseString;

const SQS = new AWS.SQS({apiVersion: '2012-11-05'});


const QUEUE_URL = "https://sqs.ap-northeast-1.amazonaws.com/098616358255/mws_notifications";
const FIFO_QUEUE_URL = "https://sqs.ap-northeast-1.amazonaws.com/098616358255/mws_notifications.fifo";
const FIFO_MESSAGE_GROUP_ID = "FIFO_MWS_NOTIFICATIONS_GROUP";


function sendMessagesToFIFO(event, context, callback){
    event.Records.map((message, index) => {
        parseString(message.body, (error, item) => {
            //console.log("parse Notification: ", item);
            const asin = item.Notification.NotificationPayload[0].AnyOfferChangedNotification[0].OfferChangeTrigger[0].ASIN[0];
            const rows = item.Notification.NotificationPayload;
            const offers = rows[0].AnyOfferChangedNotification[0].Offers[0].Offer;
            const seller_id = item.Notification.NotificationMetaData[0].SellerId[0];
            const notification = {asin, offers, seller_id};

            const params = {
                QueueUrl: FIFO_QUEUE_URL,
                MessageDeduplicationId:asin,
                MessageGroupId:FIFO_MESSAGE_GROUP_ID,
                MessageBody: JSON.stringify(notification),
            };

            //console.log("FIFO Send Message Error: ", notification);

            SQS.sendMessage(params, function(err, data) {
                if (err) {
                    console.log("FIFO Send Message Error: ", err);
                } else {
                    console.log("FIFO Send Message Success", data.MessageId);
                }
            });

            const delete_params = {
                QueueUrl: QUEUE_URL,
                ReceiptHandle: message.receiptHandle,
            };
            SQS.deleteMessage(delete_params, (err) => callback(err, message));
        });
    });
}

exports.handler = (event, context, callback) => {
    sendMessagesToFIFO(event, context, callback);
};