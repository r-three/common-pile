// Simple wikitext parsing server, node parser.js --port [port]
//
// Can create multiple versions that listen on multiple ports behind a load
// balancer for multiprocessing.

// Simple webserver
const express = require("express");
// cli parsing
const { program } = require("commander");
const workerpool = require("workerpool");

// Convert the cli argument into an actual int.
function parseIntArg(value, prev) {
  const parsedValue = parseInt(value, 10);
  if (isNaN(parsedValue)) {
    throw new commander.InvalidArgumentError("Not an Int.")
  }
  return parsedValue;
}

// Parse CLI arguments
program
  .option("--port <int>", "port", parseIntArg, 3000)
  .option("--host", "host", "localhost")
  .option("--timeout <int>", "timeout (seconds)", parseIntArg, 120)
  .option("--maxworkers <int>", "max #workers in pool", parseIntArg, 1)
  .parse();
const args = program.opts(process.argv);

// TODO: make pool settings configurable
console.log(`Starting worker pool with at most ${args.maxworkers} workers.`)
const pool = workerpool.pool("./worker.js", {
  maxWorkers: args.maxworkers,
  emitStdStreams: false,
  workerThreadOpts: {
    resourceLimits: {
      maxOldGenerationSizeMb: 65536,
      maxYoungGenerationSizeMb: 32768,
    }}});

const app = express();

// TODO: How to set no size limit?
app.use(express.json({limit: "1000mb"}));
// This is an endpoint the load balancer and the runner script will hit to make
// sure the server is running. Sometime the main server and crash when multiple
// large document requests come in.
app.get("/health", async (req, res) => {
  res.status(200).send("");
})
// Endpoint to parse wikitext.
app.post("/", async (req, res) => {
  // Document comes as json {"wikitext": str, "id": str, "source": str}
  const data = req.body;
  console.log(`Parsing wikitext from document ${data['id']} of ${data['source']}`);

  // Pass this document to the worker pool. Using a worker pool allows us to
  // put a timeout on syncronous code (wtf_wikipedia) as the main server will
  // run async and kill the worker if it is taking too long.
  pool
    // Run the parsing function `wtf_parse` in the worker file `worker.js`
    .exec('wtf_parse', [data["wikitext"]])
    // If the worker doesn't return a result in this time, an error is thrown
    .timeout(args.timeout * 1000)
    // When the worker returns, this is run
    .then((response) => {
      // Log finish and return parsed text.
      console.log(`Finished parsing wikitext from document ${data['id']} of ${data['source']}`);
      res.json(response);
    })
    // If there was an error in the worker,
    .catch((err) => {
      console.log(err.message);
      // If this is a timeout error, set the status code.
      if (err.message.indexOf("timed out") != -1) {
        console.error(`Parsing wikitext from document ${data['id']} of ${data['source']} timed out.`)
        // This is technaially for the server to send the client when the client has
        // timed out, but there isn't a server side timeout code. 504 is for when the
        // server is a proxy, not just long running.
        res.status(408).json({ timeout: err.message });
      // Log other errors, these are generally from the worker running out of
      // memory
      } else {
        console.log(`~~~~~~~~~~ Error processing ${data['id']} of ${data['source']} ~~~~~~~~~~`);
        console.error(err);
        res.status(500).json({ error: err.message});
      }
    });

})
// Start the server.
app.listen(args.port, () => {
  console.log(`Server started on port=${args.port} with timeout=${args.timeout} seconds.`)
})
