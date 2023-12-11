# Project Gutenberg
##

A collection of public domain books from www.gutenberg.org.

# Data Download

1. Download Gutenberg metadata `./get-metadata.sh`
2. Build the Public Domain index `python build-index.py`
3. Add PG19 Special cases to the book index `python add-to-book-index.py 1546 378`
4. Download the books `python get-books.py --skip 51155`
5. Get PG19 Special cases `python get-pg19-books.py 28520 30360 57479 57486 38200 3189 26568 51155 38718`
6. Convert Books to the Dolma format. `python to-dolma.py`
7. Preprocess the books `python preprocess-books.py`

Raw text will live in `./data/project-gutenberg/raw` and processed books in `./data/project-gutenberg/v0`

## Data Stats

| # Books | # Tokens |
|--------:|---------:|
|         |          |

## Special Cases:

### Missing from PG

We can get a copy from PG19

* `28520` _Forbidden Fruit_, published in 1905
* `30360` _My Secret Life_, published in 1888
* `57479` _A Secret of the Sea, VOL II_ (VOL I is `57672`)
* `57486` _A Secret of the Sea, VOL III_

These will be added to the index later.

### Lacking PG Metadata, but it is in PG19

We can get a copy from PG19

* `38200` _Like Another Helen_, published 1900
* ~~`57983` _Mother, Nurse and Infant_~~ included as id `57979`

These will be added to the index later.

### Missing plaintext version on PG

We can get a copy from PG19

* `3189` _Sketches New and Old_ by Mark Twain.
* `26568` _Seventh Annual Report of the Bureau of Ethnology_ by John Wesley Powell
*  `51155` _Complete Dictonary of Synonyms and Antonyms_ by Samuel Fallows
* `38718` _Lawrence Clavering_ by A. E. W. Mason

These are in the index, but there content will be from PG19

```python
python get-pg19-books.py 28520 30360 57479 57486 38200 3189 26568 51155 38718
```

### PG Metadata says it is Copyrighted, but it is in PG19

* `1546` _Sonnets of Sundry Notes_ by William Shakespeare, originally published in The Passionate Pilgrim (1599). So it has moved into Public Domain.
* `378` _The White Knight: Tirant Lo Blanc_, originally published in 1490. So it has moved into Public Domain.

```python
python add-to-book-index.py 1546 378
```

## Possibly Missing

* Books marked as Copyrighted, but are published before 1923, i.e., they are older than 100 years and have passed into public domain.
* Books that lack a plaintext version.
* Two of the books missing from PG but included in PG19 are erotic books, similar books may be hidden by default?
