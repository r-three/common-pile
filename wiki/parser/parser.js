// Simple wikitext parsing server, node parser.js --port [port]
//
// Can create multiple versions that listen on multiple ports behind a load
// balancer for multiprocessing.

// Simple webserver
const express = require("express");
// cli parsing
const { program } = require("commander");
const workerpool = require("workerpool");
const pool = workerpool.pool("./worker.js");


// Parse CLI arguments
program
  .option("--port <int>", "port", 3000)
  .option("--host", "host", "localhost")
  .option("--timeout <int>", "timeout (seconds)", 120)
  .parse();
const args = program.opts(process.argv);

const app = express();

app.use(express.json({limit: "1000mb"}));
app.post("/", async (req, res) => {
  const data = req.body;

  console.log(`Parsing wikitext from document ${data['id']} of ${data['source']}`);

  // var response = await pool.exec('wtf_parse', [data["wikitext"]]);
  pool
    .exec('wtf_parse', [data["wikitext"]])
    .timeout(args.timeout * 1000)
    .then((response) => {
      res.json(response);
    })
    .catch((err) => {
      console.log(err.message);
      if (err.message.indexOf("timed out") != -1) {
        console.error(`Parsing wikitext from document ${data['id']} of ${data['source']} timed out.`)
        res.status(408).json({ timeout: err.message });
      } else {
        console.error(err);
        res.status(500).json({ error: "Internal server error"});
      }
    });

})
app.listen(args.port)
