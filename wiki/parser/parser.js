const http = require("http");
const wtf = require("wtf_wikipedia");
const { program } = require("commander");


const requestListener = (req, res) => {
  // console.log("Incoming Request!");

  var data = "";
  req.on("data", function (chunk) {
    data += chunk;
  });
  req.on("end", function () {
    data = JSON.parse(data);
    console.log(`Parsing wikitext from document ${data['id']} of ${data['source']}`);

    res.setHeader("Content-Type", "application/json");

    const response = {
      text: wtf(data["wikitext"]).text(),
    }

    const json = JSON.stringify(response);
    res.writeHead(200);
    res.end(json);
  });

};


const server = http.createServer(requestListener);

program
  .option("--port <int>", "port", 3000)
  .option("--host", "host", "localhost")
  .parse();
const args = program.opts(process.argv);

server.listen(args.port, args.host, function(error) {
  if (!error)
    console.log(`Server is Listening at http://${args.host}:${args.port}`);
  else
    console.log("Error binding server to 3000");
});
