// Simple wikitext parsing server, node parser.js --port [port]
//
// Can create multiple versions that listen on multiple ports behind a load
// balancer for multiprocessing.

// Simple webserver
const http = require("http");
// Wikitext parsing
const wtf = require("wtf_wikipedia");
// Explored the wikitext -> latex conversion, doesn't help much, things like
// unicode pi are still unicode.
// wtf.extend(require("wtf-plugin-latex"))
// cli parsing
const { program } = require("commander");


const requestListener = (req, res) => {
  // Server endpoint:
  // Input: {"wikitext": str, "id": str, "source": str}
  //   id and source are just used for debugging.
  // Output: {"document": List[{"title": str, "text": str}]}

  // Read in request json
  var data = "";
  req.on("data", function (chunk) {
    data += chunk;
  });
  req.on("end", function () {
    data = JSON.parse(data);

    // Set response headers
    console.log(`Parsing wikitext from document ${data['id']} of ${data['source']}`);
    res.setHeader("Content-Type", "application/json");

    // parse wikitext with wtf_wikipedia
    var doc = wtf(data["wikitext"]);
    // convert the format into json, a list of sections (which have title + text)
    const response = {
      document: doc.sections().map(s => ({title: s.title(), text: s.text()})),
    };

    // Return response
    const json = JSON.stringify(response);
    res.writeHead(200);
    res.end(json);
  });

};


// Parse CLI arguments
program
  .option("--port <int>", "port", 3000)
  .option("--host", "host", "localhost")
  .parse();
const args = program.opts(process.argv);

// Setup Server
const server = http.createServer(requestListener);
// Start Server
server.listen(args.port, args.host, function(error) {
  if (!error)
    console.log(`Server is Listening at http://${args.host}:${args.port}`);
  else
    console.log("Error binding server to 3000");
});
