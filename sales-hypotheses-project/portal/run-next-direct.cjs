process.env.NODE_ENV = "development";
process.env.NEXT_RUNTIME = "nodejs";

const port = Number(process.env.PORT || "3000");
const hostname = process.env.HOSTNAME || "127.0.0.1";

async function main() {
  const { startServer } = require("next/dist/server/lib/start-server");
  await startServer({
    dir: process.cwd(),
    port,
    isDev: true,
    hostname,
    allowRetry: false,
  });
}

main().catch((err) => {
  console.error("[next-direct] failed to start");
  console.error(err && err.stack ? err.stack : err);
  process.exit(1);
});
