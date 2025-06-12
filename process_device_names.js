const AWS = require("aws-sdk");
const fs = require("fs");
const csv = require("csv-parser");

// Global variables
const ENV = process.env.ENV || "dev";
const TABLE_NAME = `${ENV}-user`;

// Configure logging
const logger = {
  info: (message) =>
    console.log(`INFO: ${new Date().toISOString()} - ${message}`),
  warn: (message) =>
    console.warn(`WARN: ${new Date().toISOString()} - ${message}`),
  error: (message, error) =>
    console.error(
      `ERROR: ${new Date().toISOString()} - ${message}`,
      error || ""
    ),
};

/**
 * Scan DynamoDB table and count items with specific attributes
 * @param {AWS.DynamoDB.DocumentClient} docClient - DynamoDB document client
 * @returns {Promise<number>} - Total count of matching items
 */
async function scanDeviceCount(docClient) {
  let totalCount = 0;
  let lastKey = null;

  try {
    do {
      const params = {
        TableName: TABLE_NAME,
        FilterExpression: "recordType = :rt AND #t = :type AND #n = :name",
        ExpressionAttributeNames: {
          "#t": "type",
          "#n": "name",
        },
        ExpressionAttributeValues: {
          ":rt": "device",
          ":type": "desktop",
          ":name": "Desktop Agent",
        },
        Select: "COUNT",
        ExclusiveStartKey: lastKey,
      };

      if (!lastKey) {
        delete params.ExclusiveStartKey;
      }

      const response = await docClient.scan(params).promise();
      totalCount += response.Count;
      lastKey = response.LastEvaluatedKey;
    } while (lastKey);

    logger.info(`Found ${totalCount} device records matching criteria`);
    return totalCount;
  } catch (error) {
    logger.error("Error scanning table:", error);
    throw error;
  }
}

/**
 * Extract OS information from sourceApp field
 * @param {string} sourceApp - Source application string
 * @returns {string} - OS name
 */
function getOsFromSourceApp(sourceApp) {
  sourceApp = sourceApp.toLowerCase();
  if (sourceApp.includes("mac")) {
    return "Mac";
  } else if (sourceApp.includes("windows")) {
    return "Windows";
  } else {
    return null;
  }
}

/**
 * Process CSV file and update DynamoDB records
 * @param {string} filePath - Path to CSV file
 * @param {AWS.DynamoDB.DocumentClient} docClient - DynamoDB document client
 * @returns {Promise<Object>} - Counts of processed and updated records
 */
async function processCsvFile(filePath, docClient) {
  let updatedCount = 0;
  let processedCount = 0;

  const input = fs.createReadStream(filePath).pipe(csv());

  for await (const row of input) {
    processedCount++;

    if (!isValidRow(row)) {
      logger.warn(`Skipping record with missing data: ${JSON.stringify(row)}`);
      continue;
    }

    const { suid, zid, sourceApp } = row;

    logger.info(`Processing suid=${suid}, zid=${zid}, sourceApp=${sourceApp}`);

    try {
      const item = await getDynamoDbItem(docClient, suid, zid);

      if (!item) {
        logger.info(`No matching item for suid=${suid}, zid=${zid}`);
        continue;
      }

      if (shouldUpdateItem(item)) {
        const osValue = getOsFromSourceApp(sourceApp);
        if (!osValue) {
          logger.info(`No OS detected for suid=${suid}, zid=${zid}`);
          continue;
        }

        await updateItemWithOs(docClient, suid, zid, osValue);
        updatedCount++;
      } else {
        logger.info(
          `Skipping update for suid=${suid}, zid=${zid} as name already includes OS`
        );
      }
    } catch (err) {
      logger.error(`Error processing suid=${suid}, zid=${zid}`, err);
    }

    logProgressIfNeeded(processedCount);
  }

  logCompletion(processedCount, updatedCount);
  return { updatedCount, processedCount };
}

function isValidRow(row) {
  const { suid, zid, sourceApp } = row;
  return suid && zid && sourceApp;
}

async function getDynamoDbItem(docClient, suid, zid) {
  const getParams = {
    TableName: TABLE_NAME,
    Key: {
      pk: suid,
      sk: `device#${zid}`,
    },
    ProjectionExpression: "pk, sk, #name, recordType, #type, os",
    ExpressionAttributeNames: {
      "#name": "name",
      "#type": "type",
    },
  };

  const response = await docClient.get(getParams).promise();
  return response.Item;
}

function shouldUpdateItem(item) {
  return (
    item.name &&
    item.name.toLowerCase() === "desktop agent" &&
    !item.name.includes("Mac") &&
    !item.name.includes("Windows")
  );
}

async function updateItemWithOs(docClient, suid, zid, osValue) {
  const newName = `Desktop Agent (${osValue})`;

  logger.info(`Updating suid=${suid}, zid=${zid} with new name=${newName}`);

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
      ":os": osValue,
    },
  };

  await docClient.update(updateParams).promise();
}

function logProgressIfNeeded(processedCount) {
  if (processedCount % 1000 === 0) {
    logger.info(`Processed ${processedCount} records`);
  }
}

function logCompletion(processedCount, updatedCount) {
  logger.info(
    `CSV complete: Processed ${processedCount}, Updated ${updatedCount}`
  );
}

/**
 * Main function to orchestrate the process
 */
async function main() {
  try {
    // Initialize DynamoDB client
    const docClient = new AWS.DynamoDB.DocumentClient();

    // Get the file path from environment or use default in same folder
    const csvFilePath = process.env.FILE_PATH || "./logs-insights-results.csv";

    // Check if file exists
    if (!fs.existsSync(csvFilePath)) {
      logger.error(`CSV file not found: ${csvFilePath}`);
      return 1;
    }

    logger.info(
      `Starting process with table ${TABLE_NAME} and file ${csvFilePath}`
    );

    //Process CSV and update records
    const { updatedCount, processedCount } = await processCsvFile(
      csvFilePath,
      docClient
    );

    // Log summary
    logger.info("Process summary:");
    logger.info(`  - Total records processed from CSV: ${processedCount}`);
    logger.info(`  - Total records updated: ${updatedCount}`);

    return 0;
  } catch (error) {
    logger.error("Unhandled exception:", error);
    return 1;
  }
}

// Run the main function
main()
  .then((exitCode) => process.exit(exitCode))
  .catch((error) => {
    logger.error("Fatal error:", error);
    process.exit(1);
  });
