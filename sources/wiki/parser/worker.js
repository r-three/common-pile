// Actually run wtf_wikipedia parsing. This is done in a worker thread to allow
// for timeouts as it is sync code.

const workerpool = require("workerpool");
const wtf = require("wtf_wikipedia");

function wtf_parse(text){
  // If the input is empty, at least return one empty section. This might have
  // been better to have the client code deal with an empty list.
  if (!text) {
    return {document: [{title: "", text: ""}]}
  }

  // Parse with wtf_wikipedia
  var doc = wtf(text);

  // Convert to simple [{"title": str, "text": str}, ...] representation of
  // sections for the response
  const response = {
    document: doc.sections().map(s => ({title: s.title(), text: s.text()})),
  };
  return response;
}

workerpool.worker({
  wtf_parse,
});
