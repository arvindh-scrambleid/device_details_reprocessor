const AWS = require("aws-sdk");
const fs = require("fs");
const csv = require("csv-parser");

const ENV = process.env.ENV || "qa";
const TABLE_NAME = `${ENV}-user`;
const CONCURRENCY_LIMIT = parseInt(process.env.CONCURRENCY_LIMIT || "20");
const processedDevices = new Set();

const logger = {
  info: (msg) => console.log(`INFO: ${new Date().toISOString()} - ${msg}`),
  warn: (msg) => console.warn(`WARN: ${new Date().toISOString()} - ${msg}`),
  error: (msg, err) =>
    console.error(`ERROR: ${new Date().toISOString()} - ${msg}`, err || ""),
};

function getOsFromSourceApp(sourceApp) {
  sourceApp = sourceApp.toLowerCase();
  if (sourceApp.includes("mac")) return "Mac";
  if (sourceApp.includes("windows")) return "Windows";
  return null;
}

function isValidRow(row) {
  const { suid, zid, sourceApp } = row;
  return suid && zid && sourceApp;
}

function shouldUpdateItem(zid) {
  return !processedDevices.has(zid);
}

async function updateItemWithOs(docClient, suid, zid, osValue) {
  const newName = `Desktop Agent (${osValue})`;

  const updateParams = {
    TableName: TABLE_NAME,
    Key: {
      pk: suid,
      sk: `device#${zid}`,
    },
    UpdateExpression: "SET #name = :name, #os = :os",
    ExpressionAttributeNames: {
      "#name": "name",
      "#os": "os",
    },
    ExpressionAttributeValues: {
      ":name": newName,
      ":os": osValue.toLowerCase(),
    },
  };

  await docClient.update(updateParams).promise();
}

async function processRow(docClient, row) {
  const { suid, zid, sourceApp } = row;
  logger.info(`Processing suid=${suid}, zid=${zid}, sourceApp=${sourceApp}`);

  if (!shouldUpdateItem(zid)) {
    logger.info(
      `Skipping update for suid=${suid}, zid=${zid} as name already includes OS`
    );
    return { updated: false };
  }

  const osValue = getOsFromSourceApp(sourceApp);
  if (!osValue) {
    logger.info(`No OS detected for suid=${suid}, zid=${zid}`);
    return { updated: false };
  }

  await updateItemWithOs(docClient, suid, zid, osValue);
  processedDevices.add(zid);
  return { updated: true };
}

async function processCsvFile(filePath, docClient) {
  const input = fs.createReadStream(filePath).pipe(csv());

  let processedCount = 0;
  let updatedCount = 0;
  let batch = [];

  for await (const row of input) {
    processedCount++;
    if (!isValidRow(row)) {
      logger.warn(`Skipping record with missing data: ${JSON.stringify(row)}`);
      continue;
    }

    batch.push(processRow(docClient, row));

    if (batch.length >= CONCURRENCY_LIMIT) {
      const results = await Promise.allSettled(batch);
      results.forEach((res) => {
        if (res.status === "fulfilled" && res.value.updated) updatedCount++;
      });
      batch = [];
    }

    if (processedCount % 1000 === 0) {
      logger.info(`Processed ${processedCount} records`);
    }
  }

  // Flush remaining batch
  if (batch.length > 0) {
    const results = await Promise.allSettled(batch);
    results.forEach((res) => {
      if (res.status === "fulfilled" && res.value.updated) updatedCount++;
    });
  }

  logger.info(
    `CSV complete: Processed ${processedCount}, Updated ${updatedCount}`
  );
  return { updatedCount, processedCount };
}

async function main() {
  try {
    const start = Date.now();
    const docClient = new AWS.DynamoDB.DocumentClient();
    const csvFilePath = process.env.FILE_PATH || "./logs-insights-results.csv";

    if (!fs.existsSync(csvFilePath)) {
      logger.error(`CSV file not found: ${csvFilePath}`);
      return 1;
    }

    logger.info(
      `Starting process with table ${TABLE_NAME} and file ${csvFilePath}`
    );

    const { updatedCount, processedCount } = await processCsvFile(
      csvFilePath,
      docClient
    );

    logger.info("Process summary:");
    logger.info(`  - Total records processed from CSV: ${processedCount}`);
    logger.info(`  - Total records updated: ${updatedCount}`);
    logger.info(`Processed devices in set :: ${processedDevices.size}`);

    const end = Date.now() - start;
    logger.info(`Execution time: ${Math.round(end / 1000)} seconds`);
    return 0;
  } catch (error) {
    logger.error("Unhandled exception:", error);
    return 1;
  }
}

main()
  .then((exitCode) => process.exit(exitCode))
  .catch((error) => {
    logger.error("Fatal error:", error);
    process.exit(1);
  });
