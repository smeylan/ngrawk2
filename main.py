#!/usr/bin/python
# -*- coding: utf-8 -*-

import ngrawk2
import json
import pdb
import argparse

dependencies = ['ngram','ngram-count','zs','gzrecover']
[ngrawk2.ngrawk_utils.checkForBinary(d) for d in dependencies]


def augment_args(language, defaults):
	rdf = defaults.copy()
	for key, value in language.iteritems():
		rdf[key] = value
	return(rdf)	

if __name__ == "__main__":
	argparser = argparse.ArgumentParser()
	argparser.add_argument("--ctrl", type=str, help="name of .ctrl json")
	args = argparser.parse_args()

	print('Loading control .json')
	with open(args.ctrl) as ctrl_json:
		ctrl = json.load(ctrl_json)    

	# propagate the defaults; language-specific items override defaults
	ctrl = [augment_args(x, ctrl['defaults']) for x in ctrl['corpora']]

for corpus in ctrl:
	if 'download' in corpus["overwrite"]:
		raise notImplementedError
		#this has only been implemented for GB12 and 1T
		#ngrawk.downloadCorpus(corpus) 
	if 'validate' in corpus["overwrite"]:
		raise notImplementedError
		#this has only been implemented for GB12 and 1T
		#ngrawk.validateCorpus(corpus)
	if 'analyze' in corpus["overwrite"]:
		ngrawk2.analyzeCorpus(corpus)