# Licensed Pile

Repo to hold code and track issues for the collection of permissively licensed data


## Tips

You can look at Dolma formatted data via commandline tools like so.

```
cat ${file}.jsonl.gz | gunzip | jq -s ${commmand}
```

`js -s` is used to process the input as jsonl (valid json per line) instead of expecting the whole input to be valid json.

Then you can use jq syntax to look for specific things, e.g.:

Look at the text for item 1115 `cat ${file}.jsonl.gz | gunzip | jq -s '.[1115].text'`

Look at the text for item with the id of 12 (note that position in file is not correlated with id) `cat ${file}.jsonl.gz | gunzip | jq -s '.[] | select(.id == "12").text'`

Note: You can also use `gunzip -c ${file}.jsonl.gz | jq -s ${command}` which is slightly faster (reduces the amount of  data flowwing through pipes) but if you forget the `-c` flag you end up uncompressing the file and deleting the compressed version, i.e. you need to run `gzip ${file}.jsonl` to fix it.

### Capped-parallelism in bash script
Sometimes we want to download/process multiple files in parallel up to a limited number of jobs in bash script.
Below is a example code snippet (used in [courtlistener/get_data.sh](courtlistener/get_data.sh)).
Note that `jobs -r` counts all jobs running in the current shell.

````
max_jobs = 8
for file in "${files[@]}"; do
    download_and_process "file" &

    # Limit the number of parallel jobs
    if (( $(jobs -r | wc -l) >= max_jobs )); then
        wait -n
    fi
done
````
## Development

We use git pre-commit hooks to format code and keep style consistent.

* Install the pre-commit library with `pip install pre-commit`.
* Install the pre-commit hooks with `pre-commit install` from the repository root.

Now when a `git commit` is run, the hooks will run. If one of the hooks reformats a file, the commit will be blocked. Then you need to inspect the changes and readd them with `git add`. Then you can re-run your commit command and the commit will actually be added.
