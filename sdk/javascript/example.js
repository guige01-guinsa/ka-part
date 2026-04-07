import { ComplaintSDK } from "./client.js";

async function main() {
  const sdk = new ComplaintSDK({
    baseUrl: "https://ka-facility-os.onrender.com/api",
    apiKey: "sk-ka-REPLACE_ME",
  });

  const created = await sdk.createComplaint({
    building: "101",
    unit: "1203",
    channel: "전화",
    content: "엘리베이터가 멈췄어요",
  });

  const dashboard = await sdk.generateDailyReport();
  console.log({ created, dashboard });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
