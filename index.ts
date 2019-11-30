import * as AWS from "aws-sdk";
import * as fs from "fs";
import * as path from "path";
import * as json2csv from "json2csv";
import Log from "./log";

const accessKeyId = process.env["AWS_KEYID"];
const secretAccessKey = process.env["AWS_SECRET"];
const region = process.env["AWS_REGION"];
const queueUrl = process.env["QUEUE_URL"];
const outDir = process.env["OUT_DIR"];
const fields = process.env["FIELDS"];
const delimiter = process.env["DELIMITER"] || ";";

Log.log("AWS_KEYID=" + accessKeyId);
Log.log("AWS_SECRET=" + (secretAccessKey ? "**********" : ""));
Log.log("AWS_REGION=" + region);
Log.log("QUEUE_URL=" + queueUrl);
Log.log("OUT_DIR=" + outDir);
Log.log("FIELDS=" + fields);
Log.log("DELIMITER=" + delimiter);
Log.log("-------------------");
if (
  !accessKeyId ||
  !secretAccessKey ||
  !region ||
  !queueUrl ||
  !outDir ||
  !fields
) {
  Log.error(
    "You must set environement variables : AWS_KEYID, AWS_SECRET, AWS_REGION, QUEUE_URL, OUT_DIR, FIELDS"
  );
  process.exit(1);
}

AWS.config.update({ region, accessKeyId, secretAccessKey });
const sqs = new AWS.SQS({ apiVersion: "2012-11-05" });

const parser = new json2csv.Parser({
  delimiter,
  fields: fields.split(","),
});

function processMessage(messages: AWS.SQS.ReceiveMessageResult) {
  if (messages.Messages) {
    messages.Messages.forEach(function(msg: AWS.SQS.Message) {
      console.log(msg);
      const filepath = msg.MessageAttributes["filepath"].StringValue;
      const filename = msg.MessageAttributes["filename"].StringValue;
      const body = JSON.parse(msg.Body);
      const destPath = path.join(outDir, filepath);
      const dest = path.join(destPath, filename);
      if (fs.existsSync(destPath)) {
        if (!fs.existsSync(dest)) {
          Log.log("Writing file to", dest);
          const csv = parser.parse(body);
          fs.writeFileSync(dest, csv);
          sqs
            .deleteMessage({
              QueueUrl: queueUrl,
              ReceiptHandle: msg.ReceiptHandle,
            })
            .promise()
            .then(function() {
              Log.log("SQS Message deleted");
              listenNext();
            })
            .catch(function(err) {
              Log.error(err, "SQS delete message failed");
              listenNext();
            });
          return;
        } else {
          sqs.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: msg.ReceiptHandle,
            VisibilityTimeout: 60,
          });
          Log.log("Output file already exists (no overwrite):", dest);
        }
      } else {
        Log.error("Output path not exists:", destPath);
      }
    });
  }
  listenNext();
}

function listenNext() {
  console.log("Listenning for messages");
  sqs
    .receiveMessage({
      QueueUrl: queueUrl,
      MessageAttributeNames: ["All"],
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 20,
      VisibilityTimeout: 30,
    })
    .promise()
    .then(processMessage)
    .catch(function(err) {
      Log.error(err);
      listenNext();
    });
}
listenNext();
