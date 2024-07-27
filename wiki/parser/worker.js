const workerpool = require("workerpool");
const wtf = require("wtf_wikipedia");

function wtf_parse(text) {
  var doc = wtf(text);

  const response = {
    document: doc.sections().map(s => ({title: s.title(), text: s.text()})),
  };
  return response;
}

workerpool.worker({
  wtf_parse,
});
