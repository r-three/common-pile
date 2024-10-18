# Python PEP

The majority of PEPs should be placed in the public domain, as seen here https://peps.python.org/pep-0001/#pep-review-resolution:

> Copyright/license – Each new PEP must be placed under a dual license of public domain and CC0-1.0-Universal (see this PEP for an example).

However some are published under the Open Publication License, as seen here https://peps.python.org/pep-0009/

> Update your References and Copyright section.  Usually you'll place your PEP into the public domain, in which case just leave the "Copyright" section alone.  Alternatively, you can use the Open Publication License[3], but public domain is still strongly preferred.


## Collecting the Data

1. Clone the peps repository https://github.com/python/peps
2. run `python to_dolma.py --peps /path/to/cloned/repo`
3. Install pandoc
4. run `python preprocess.py`

### Alternative Approaches

An alternative to Pandoc would be to use `docutils` and `rst2txt` as they have some python specific features (like converting the ``:pep:`00NN``` to `PEP NN`). However, the formatting in the `rst2txt` writer was so slow that things never finished.

``` python
def clean_rst(text):
    from docutils.core import publish_string
    import rst2txt
    try:
        return publish_string(source=text, writer=rst2txt.Writer()).decode("utf-8")
    except:
        logger = logs.get_logger()
        logger.error("Failed to parse rst", exc_info=True)
        return text
```
