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
      const org = msg.MessageAttributes["org"].StringValue;
      const ref = msg.MessageAttributes["ref"].StringValue;
      const body = JSON.parse(msg.Body);
      const destPath = path.join(outDir, org);
      const dest = path.join(destPath, ref + ".csv");
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
          Log.error("Output file already exists (no overwrite):", dest);
        }
      } else {
        Log.error("Output path not exists:", destPath);
      }
    });
  }
  listenNext();
}

function listenNext() {
  sqs
    .receiveMessage({
      QueueUrl: queueUrl,
      MessageAttributeNames: ["All"],
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 20,
    })
    .promise()
    .then(processMessage)
    .catch(function(err) {
      Log.error(err);
      listenNext();
    });
}
listenNext();
