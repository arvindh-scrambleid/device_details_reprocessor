import {
  ListSecretsCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";

import { PutItemCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";

const env = "qa";

const tableName = `${env}-client-credential`;

const oauthClients = [];

const basicAuthClients = [];

const secretClient = new SecretsManagerClient({ region: "us-east-1" });

const dynamoClient = new DynamoDBClient({ region: "us-east-1" });

const scopesMapper = {
  scim: ["scim.read", "scim.write", "scim.delete"],
  "scim-readonly": ["scim.read"],
  audit: ["audit.read", "audit.write", "audit.delete"],
  ivr: ["ivr.read", "ivr.write", "ivr.delete"],
  ldap: ["ldap.read", "ldap.write", "ldap.delete"],
};

let totalSecrets = 0;

const fetchClients = async () => {
  let nextToken;

  do {
    const command = new ListSecretsCommand({ NextToken: nextToken });
    const response = await secretClient.send(command);

    let secrets = response.SecretList ?? [];

    for (let secret of secrets) {
      if (secret.Name.includes(env) && secret.Name.includes("/oauth/")) {
        oauthClients.push(secret.Name);
      }
      if (secret.Name.includes(env) && secret.Name.includes("BasicAuth")) {
        basicAuthClients.push(secret.Name);
      }
    }

    totalSecrets = totalSecrets + secrets.length;

    nextToken = response.NextToken;
  } while (nextToken);
};

let a = {
  client_id: {
    S: "dem-apiuser-audit-oauth",
  },
  authMechanism: {
    S: "client_credentials",
  },
  createDate: {
    S: "2025-06-18T05:40:17",
  },
  name: {
    S: "dem-apiuser-audit-oauth",
  },
  orgCode: {
    S: "dem",
  },
  scopes: {
    L: [
      {
        S: "audit.read",
      },
    ],
  },
  status: {
    S: "ACTIVE",
  },
};

let b = {
  client_id: {
    S: "dem-apiuser-scim-basic",
  },
  authMechanism: {
    S: "basic_auth",
  },
  createDate: {
    S: "2025-06-18T05:40:17",
  },
  name: {
    S: "dem-apiuser-scim-basic",
  },
  orgCode: {
    S: "dem",
  },
  scopes: {
    L: [
      {
        S: "scim.write",
      },
      {
        S: "scim.read",
      },
      {
        S: "scim.delete",
      },
    ],
  },
  status: {
    S: "ACTIVE",
  },
};

const services = {
  scim: "scim",
  audit: "audit",
  ivr: "ivr",
  ldap: "ldap",
};

/**
 *
 * dev/ten/dem/oauth/dem-apiuser-ldap
 * dev/ten/dem/auditLogBasicAuth
 */

let oauthProcessed = 0;
let basicAuthProcessed = 0;

const createClient = async (secretName, authType) => {
  if (secretName.split("/").length < 3) {
    console.log("unsupported client", secretName);
    return;
  }

  let clientId;

  let orgCode;

  let service;

  let auth;

  if (authType == "basic_auth") {
    let splitted = secretName.split("/");

    orgCode = splitted[2];

    auth = "basic";

    service = splitted[3].includes(services.scim)
      ? services.scim
      : services.audit;

    clientId = `${orgCode}-apiuser-${service}-basic`;
  } else {
    let splitted = secretName.split("/");

    orgCode = splitted[2];

    auth = "oauth";

    service = splitted[4].split("-")[2];

    splitted[4].includes("readonly") && (service += "-readonly");

    clientId = `${splitted[4]}-oauth`;
  }

  const scopes = scopesMapper[service];

  if (!Array.isArray(scopes)) {
    console.error(`Missing or invalid scopes for clientId: ${clientId}`);
    return;
  }

  let client = {
    client_id: clientId,
    authMechanism: authType,
    createDate: new Date().toISOString(),
    name: clientId,
    orgCode,
    scopes: scopes,
    status: "ACTIVE",
  };

  const command = new PutItemCommand({
    TableName: tableName,
    Item: marshall(client),
  });

  // await dynamoClient.send(command);

  console.log("client created", clientId);

  if (authType == "basic_auth") basicAuthProcessed++;
  else oauthProcessed++;
};

(async () => {
  try {
    await fetchClients();

    for (let name of oauthClients) {
      await createClient(name, "client_credentials");
    }

    for (let name of basicAuthClients) {
      await createClient(name, "basic_auth");
    }

    console.log("totalSecrets", totalSecrets);

    console.log("basicAuthSecrets", basicAuthClients.length);
    console.log("basicAuthProcessed", basicAuthProcessed);

    console.log("oauthSecrets", oauthClients.length);
    console.log("oauthProcessed", oauthProcessed);
  } catch (e) {
    console.error("Error", { message: e.message, stack: e.stack });
  }
})();
