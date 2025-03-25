# WTF WIKIPEDIA parsing server

We use the dolma format and a server running `wtf_wikipedia` for wikitext parsing instead of they dumpster dip as we want to be able to parse wikitext even when it is not in the standard xml format.

## Starting the Parser Server

1. Install HAProxy `sudo apt install haproxy` You need at least version 2.2, we used 2.4
2. Install nvm and node
3. Install dependencies `npm install`
4. edit `haproxy.cfg` to include one `server ${name} 127.0.0.1:${port} check` line for each server you plan to run.
5. move/link `haproxy.cfg` to `/etc/haproxy/haproxy.cfg`
6. Restart haproxy (`systemctl restart haproxy` on systemd based systems)
7. Run `./start ${numserver}`. Should match the number of `server` lines in `haproxy`
8. Go to `localhost:8404/stats` to check that each server is seen by haproxy

## Why?

Each server uses a worker pool with `1` worker. This is because `wtf_wikipedia` is syncronous code, so we need to run it in a thread to be able to use timeouts to cancel execution for long running documents. This also helps in cases where the parsing causes an OoM error, this happens in the thread instead of the real server.

We then have multiple copies of the server behing the load balancer (which uses least connections scheduling), this allows for recovery in cases where the main server itself crashes.

### v8 garbage collection

v8, and therefore node, seem to have a pretty complex garbage collector and includes things like different heaps for persistant objects and "young" objects that are short-lived. Despite various efforts to set the sizes for these heaps (defaults to 64 and 32 GB in our code for each worker), I have found a lot of javascript OoM error, even though they seem to say that the heap is much smaller than the limits. This is set in the optinos for the constructor for the worker pool.

There were also cases where using a large worker pool and a single server, the main server can have OoM errors. This crashes the whole server and grinds the dolma conversion to a halt. Even with commandline arguments to set the size of the heap, this was still happening, again despite it seeming to not have much on the heap. When this happens, our load balancer stops routing traffic to this server and out start script brings a new version online. Once it is live it is added back to the pool.

These errors tend to happen on pages that have over 2 million characters.

## Settings

It seems to be fast to try to make sure that each server is currently working on 1 document and have already received a second document to be processed next. As the python code is syncronous, this means we need ~twice as many dolma processes as we have servers. Having extra python processes allows for the server to not have to wait for python string manipulataions.

On a Ryzen 9 7950X using 30 dolma processes and 16 servers, the whole system processes ~5.5k documents/second and takes ~4 hours and 15 mins to process wikipeadia, its talk pages, and the other mediawiki pages.
