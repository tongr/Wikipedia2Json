# A Wikipedia Plain Text Extractor with Link Annotations
This project is a refactored version of the following github project:
   https://github.com/jodaiber/Annotated-WikiExtractor
which is, in turn, is a simple wrapper around the Wikipedia Extractor by [Medialab](http://medialab.di.unipi.it/wiki/Wikipedia_Extractor). It generates a JSON object for each article. The JSON object contains the id, title and plain text of the article, as well as annotations of article links in the text.

Some of the functionality of the base project is discarded in this project.

# Output

## JSON of a single article

	{"url": "http://en.wikipedia.org/wiki/Anarchism", 
	 "text": "Anarchism.\nAnarchism is a political philosophy which considers the state 
		undesirable, unnecessary and harmful, and instead promotes a stateless society, or 
		anarchy. It seeks to diminish ...",
	 "id": 12,
	 "title": "Anarchism"
	 "annotations": [
		{"offset": 26, "uri": "Political_philosophy", "surface_form": "political philosophy"},
		{"offset": 67, "uri": "State_(polity)", "surface_form": "state"},
		{"offset": 156, "uri": "Anarchy", "surface_form": "anarchy"},
		...
	]}


## Annotations

Annotations are stored in an ordered list. A single annotation has the following form:

	{"offset": 1156, "uri": "Socialist", "surface_form": "socialist"}
	
* `offset`: start positon of the string
* `uri`: Wikipedia/DBPedia article name
* `surface_form`: the label of the link in the text (what part of the text was linked)

# Usage

The extractor can be run from the Terminal.

## Bash

As this is only an extention of the orgininal WikiExtractor, the usage is more or less the same.

	$ python annotated_wikiextractor.py --help
	Annotated Wikipedia Extractor:
	Extracts and cleans text from Wikipedia database dump and stores output in a
	number of files of similar size in a given directory. Each file contains
	several documents in JSON format (one document per line) with additional
	annotations for the links in the article.

	Usage:
	  annotated_wikiextractor.py [options]

	Options:
	  -k, --keep-anchors    : do not drop annotations for anchor links (e.g. Anarchism#gender)
	  -c, --compress        : compress output files using bzip2 algorithm
	  -b ..., --bytes=...   : put specified bytes per output file (500K by default)
	  -o ..., --output=...  : place output files in specified directory (current
	                          directory by default)
	  --help                : display this help and exit
	  --usage               : display script usage
	  -w ..., --workers=... : use specified number of workers
	  -p ..., --prefix=...  : use specified url prefix (default is 'http://en.wikipedia.org/wiki/')

To convert the whole Wikipedia Dump to plain text, use the following command:

	bzip2 -dc enwiki-20110115-pages-articles.xml.bz2 | python wikiextractor.py -o extracted/

If you want the output files to be compressed, use the -c option:

	bzip2 -dc enwiki-20110115-pages-articles.xml.bz2 | python wikiextractor.py -co extracted/

