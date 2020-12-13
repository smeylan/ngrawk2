# ngrawk2
codebase for building n-gram language models and computing lexical and sublexical surprisal (information content) from large-scale UTF-8 sources, e.g., Google Books, Google 1T, OPUS.

# Requirements

Assumes Python 2.7

# Running 
Options for each dataset are determined by a `.ctrl` file, a JSON file which is passed to main to specify parameters for each corpus:

```python main.py --ctrl filtered.ctrl```

The control file has two top-level nodes. `corpora` include settings for each dataset (presented in a list); `defaults` are applied 
to all corpora unless they are specified (i.e., any fields specified at the corpus level override the defaults). 

Fields that can be defined as a default or a corpus include: 

`analysisname`: name of this specific analysis   
`overwrite`: should intermediate files at each stage be overwritten? specify 0 or more of `cleaning`, `analyze`   
`order`: order of the highest order n-grams to track, e.g. 3 for trigrams   
`target`: (dev only) position of the target word in the n-gram. For example, target=2 combined with order=3 would track center-embedded trigrams
`inputdir`: base directory of the input files. Assumes that data is organized by corpus and language under this directory    
`faststoragedir`: where to store yielded `zs` files, lexical and sublexical surprisal estimates (should be on an SSD)    
`slowstoragedir`: where to store cleaned files (only read once, so okay if it is on a hard drive)   

`modelcount`: number of word types to use to build the sublexical model    
`modellist`: list of word types to build a (type-weighted) sublexical model    
`smoothing`: smoothing method to use when building a sublexical surprisal model    

`filterlist`: name of the file to use to filter the dataset   
`retrievallist`: set of word types to retrieve lexical surprisal estimates for    
`retrievalcount`: number of word types to retrieve from the retrieval list   
`dictionary_filter`: one of `lowerInDictionary`, `upperInDictionary`, `none`; used to remove items from `retrievallist` before taking `retrievalcount` items   

`collapseyears`: Google Books 2012 only — set to 0 to retain years, set to collapse years    
`token_corpus`: Path to a text file to make a token-weighted sublexical surprisal model   
`token_filter`: Path to a text file to filter the token_corpus text   
`sublex_measures`: List of sublexical measures   

Corpus entries must have the following fields  
`corpus`: name of the corpus, eg GoogleBooks2012 or BNC   
`language`: language name of the corpus   
`country_code`: ISO country code for the language (used to select the appropriate Aspell dictionary and capitalization scheme)   

