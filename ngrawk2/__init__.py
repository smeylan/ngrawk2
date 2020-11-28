import os
import subprocess
import re
import codecs
import pandas
import json
import aspell
import numpy as np

import plaintext.processing
import google.download
import google.cleaning
import google.processing
import chaos
import surprisal
import ngrawk2.ngrawk_utils

def analyzeCorpus(corpusSpecification):
	'''Conducts the analysis on a given dataset (corpus + language).'''	
	corpus = corpusSpecification['corpus'] 
	language = corpusSpecification['language'] 
	n = corpusSpecification['order'] 
	print('Processing '+corpus+':'+language)

	
	lexSurpDir, sublexSurpDir, correlationsDir, processedDir = ngrawk_utils.makeDirectoryStructure(corpusSpecification['faststoragedir'], corpusSpecification['slowstoragedir'], corpusSpecification['analysisname'], corpusSpecification['corpus'], corpusSpecification['language'], int(corpusSpecification['order']))	
	
	if 'modellist' not in corpusSpecification:
		corpusSpecification['modellist'] = os.path.join(lexSurpDir,'unigram_list.txt')
	if 'retrievallist' not in corpusSpecification:
		corpusSpecification['retrievallist'] = os.path.join(lexSurpDir,'unigram_list.txt')

	#write model metadata in the form of a json of the corpusSpecification to the faststoragedir
	with open(os.path.join(corpusSpecification['faststoragedir'],
		corpusSpecification['analysisname'], corpusSpecification['corpus'], corpusSpecification['language'], 'model_details.json'), 'w') as outfile:
		json.dump(corpusSpecification, outfile)
	# read formatted version with cat model_details.json | python -m json.tool	

	if (corpus == 'GoogleBooks2012'):
		if (language in ('eng-all', 'spa-all', 'fre-all','ger-all','rus-all','test','heb-all','ita-all')):					
			print('Checking if input files exist...')						

			print('Building language models...')
			# get backwards-indexed model of highest order (n)
			backwardsNmodel = google.processing.getGoogleBooksLanguageModel(corpusSpecification, int(n), direction='backwards', filetype='gz', colnames=['ngram','year','count','context_count'], sum_columns=None)
			# get forwards-indexed model of order n-1 (text file  built as a consequence)
			forwardsNminus1model = google.processing.getGoogleBooksLanguageModel(corpusSpecification, int(n)-1, direction='forwards', filetype='gz', colnames=['ngram','year','count','context_count'], sum_columns=['count'])				

			processedDir = lexSurpDir.replace('00_lexicalSurprisal','3-processed')
			for temp_file_to_delete in os.listdir(processedDir):
				os.remove(os.path.join(processedDir, temp_file_to_delete))
		else:
			raise NotImplementedError		
	elif(corpus == 'Google1T'):
		if (language in ('SPANISH','FRENCH','DUTCH','GERMAN','SWEDISH','CZECH','ROMANIAN','POLISH','PORTUGUESE','ITALIAN')):
			backwardsNmodel = google.processing.getGoogleBooksLanguageModel(corpusSpecification, int(n), direction="backwards", filetype='bz2', colnames=['ngram','count'], sum_columns=None)
			forwardsNminus1model = google.processing.getGoogleBooksLanguageModel(corpusSpecification, int(n)-1, direction="forwards", filetype='bz2', colnames=['ngram','count'], sum_columns=['count'])
		elif language in ('ENGLISH'):
			backwardsNmodel = google.processing.getGoogleBooksLanguageModel(corpusSpecification, int(n), direction='backwards',  filetype='gz', colnames=['ngram','count'], sum_columns=None)
			forwardsNminus1model = google.processing.getGoogleBooksLanguageModel(corpusSpecification, int(n)-1, direction='forwards', filetype='gz', colnames=['ngram','count'], sum_columns=['count'])

		processedDir = lexSurpDir.replace('00_lexicalSurprisal','3-processed')
		for temp_file_to_delete in os.listdir(processedDir):
			os.remove(os.path.join(processedDir, temp_file_to_delete))

	elif(corpus == 'GoogleBooks2009'):
		if (language in ('eng-all')):					
			print('Checking if input files exist...')			
			
			print('Building language models...')
			# get backwards-indexed model of highest order (n)
			backwardsNmodel = google.processing.getGoogleBooksLanguageModel(corpusSpecification, int(n), direction='backwards',  filetype='csv.zip', colnames=['ngram', 'count'], sum_columns=None)
			# get forwards-indexed model of order n-1 (text file  built as a consequence)
			forwardsNminus1model = google.processing.getGoogleBooksLanguageModel(corpusSpecification, int(n)-1, direction='forwards', filetype='csv.zip', colnames = ['ngram', 'count'], sum_columns=['count'])				

			processedDir = lexSurpDir.replace('00_lexicalSurprisal','3-processed')
			for temp_file_to_delete in os.listdir(processedDir):
				os.remove(os.path.join(processedDir, temp_file_to_delete))
		else:
			raise NotImplementedError		


	elif(corpus in ('BNC', 'OPUS')):
		if (language in ('en', 'ru', 'es','fr','de','he','eng-half','eng','cs','pt', 'pl','ro','it','sv','nl','en-permuted')):

			print('Checking if input files exist...')
			#!!! check if file extant; if not, then download

			#!!! does buildZSfromPlaintext.py preserve unicode?	
			print('Building language models')
			# get backwards-indexed model of highest order (n)
			backwardsNmodel = plaintext.processing.getPlaintextLanguageModel(corpusSpecification, n, direction='backwards', cleaningFunction='cleanLine_BNC')
			# get forwards-indexed model of order n-1 (text file  built as a consequence)
			forwardsNminus1model = plaintext.processing.getPlaintextLanguageModel(corpusSpecification, int(n)-1, direction='forwards', cleaningFunction='cleanLine_BNC')
			#get unigrams to be able to take top N words in the analysis
			
		else:
			raise NotImplementedError	
	else:
		pdb.set_trace()
		raise NotImplementedError		
	
	#to use most frequent words from Google for the sublexical surprisal model
	forwardBigramPath = os.path.join(lexSurpDir, '2gram-forwards-collapsed.txt')
	unigramCountFilePath = os.path.join(lexSurpDir, 'unigram_list.txt')

	ngrawk2.ngrawk_utils.marginalizeNgramFile(forwardBigramPath, unigramCountFilePath, 1, 'numeric', colnames=['ngram','count'],sum_columns=['count']) 	

	#to use OPUS for the sublexical surprisal model:
	#unigramCountFilePath = corpusSpecification['wordlist']	

	print('Getting mean lexical surprisal estimates for types in the langauge...')
	forwardsNminus1txt = os.path.join(lexSurpDir,str(int(n)-1)+'gram-forwards-collapsed.txt')
	retrieval_file = os.path.join(os.path.dirname(forwardsNminus1txt), corpusSpecification['retrievallist'])
	
	lexfile = os.path.join(lexSurpDir, 'meanSurprisal.csv')	
	surprisal.getMeanSurprisal(backwardsNmodel, forwardsNminus1txt, corpusSpecification['filterlist'],retrieval_file, 0,lexfile, corpusSpecification['country_code'], corpusSpecification['retrievalcount'], corpusSpecification['dictionary_filter'])	

	sublexFilePath = os.path.join(sublexSurpDir, str(corpusSpecification['modelcount'])+'_sublex.csv')

	for sublex_measure in corpusSpecification['sublex_measures']:

		if 'token_corpus' not in corpusSpecification:
			token_corpus = None
		else:	
			token_corpus = corpusSpecification['token_corpus']

		if 'token_filter' not in corpusSpecification:
			token_filter = None
		else:	
			token_filter = corpusSpecification['token_filter']


		if sublex_measure == 'ipa':
			print('Getting sublexical surprisal estimates for types in the language, using IPA...')
		elif sublex_measure == 'token_ipa':
			print('Getting sublexical surprisal estimates for tokens in the language, using IPA...')			
		elif sublex_measure == 'ortho':	
			print('Getting sublexical surprisal estimates for types in the language, using orthography...')					
		elif sublex_measure == 'character':	
			print('Getting sublexical surprisal estimate for types in the language, building it over the characters')
		elif sublex_measure == 'token_character':
			print('Getting sublexical surprisal estimates for tokens in the language, using characters...')
		else:
			raise ValueError('Sublexical measure not implmented. Use ipa, ortho, or character')	

		model_file = os.path.join(os.path.dirname(forwardsNminus1txt), corpusSpecification['modellist'])

		surprisal.addSublexicalSurprisals(retrieval_file, corpusSpecification['retrievalcount'], model_file, corpusSpecification['modelcount'], sublexFilePath, sublex_measure, corpusSpecification['country_code'], corpusSpecification['dictionary_filter'], corpusSpecification['smoothing'], corpusSpecification['token_corpus'], corpusSpecification['token_filter'])

	#import pdb
	#pdb.set_trace()	
