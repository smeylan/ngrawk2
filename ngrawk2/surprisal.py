#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import zs
import numpy as np
from joblib import delayed, Parallel
import multiprocessing
import json
import subprocess
import io
import glob
import pandas
import srilm
import time
import aspell
import espeak

import ngrawk2.ngrawk_utils

def trainLexicalZSmodel(inputfile, outputfile, metadata, codec):
	'''Take the cleaned and sorted file and put it into ZS file'''		
	print('Building the language model...')
	zs_command = 'zs make \''+json.dumps(metadata)+'\' --codec='+codec+' '+inputfile + ' ' + outputfile 
	subprocess.call(zs_command, shell=True)	

	
def getMeanSurprisal(backwards_zs_path, forwards_txt_path, filter_file, retrieval_file, cutoff, outputfile, language, retrieval_count, dictionary_filter):		
	start_time = time.time()
	'''producing mean surprisal estimates given a backwards n-gram language model and a forwards text file (to be read into a hash table) for order n-1. Produces mean information content (mean log probability, weighted by the frequency of each context)'''	

	# if os.path.exists(outputfile):
	# 	print('Using the existing lexical surprisal estimate')			
	# else:		
	print('Loading the backwards ZS file for order n...')
	backward_zs = zs.ZS(backwards_zs_path, parallelism=0)

	print('Loading the forwards hash table for order n-1...')
	
	bigrams = {}
	f = io.open(forwards_txt_path, encoding='utf-8')
	for line in f:
		lineElements = line.split('\t')
		if len(lineElements) > 1:			
			key = lineElements[0]+u' ' 						
			val = int(lineElements[1])
			bigrams[key] = val
		else:
			import pdb
			pdb.set_trace()	

	#!!! be careful with loading text vs. csv files	
	print('Loading retrieval file...')	#use the column 'word', sort desceding by frequency or count
	retrievalDF = ngrawk2.ngrawk_utils.readCSVorTxt(retrieval_file)		
	if 'word' not in retrievalDF.columns:
		raise ValueError('Retrieval file must have "word" column')
	if ('count' in retrievalDF.columns):
		retrievalDF = retrievalDF.rename(columns={'count': 'retrieval_count'})
	elif ('frequency' in filterDF.columns):
		retrievalDF = retrievalDF.rename(columns={'frequency': 'retrieval_count'})
	else:
		raise ValueError('Filter file must contain "count" or "frequency" column')	

	# retrievalDF['nchar'] = [len(x) for x in retrievalDF.word]
	# retrievalDF['is_numeric'] = [np.char.isnumeric(x) for x in retrievalDF.word]	
	# retrievalDF = retrievalDF.loc[(retrievalDF.nchar >= 3) & ~retrievalDF.is_numeric]
	# import pdb
	# pdb.set_trace()


	print('Loading filter file...')	#use the column word	
	if filter_file is not None and filter_file != "None" and filter_file != "none":
		filterDF = ngrawk2.ngrawk_utils.readCSVorTxt(filter_file)

		if 'word' not in filterDF.columns:
			raise ValueError('Retrieval file must have "word" column')
		if ('count' in filterDF.columns):
			filterDF = filterDF.rename(columns={'count': 'filter_count'})
		elif ('frequency' in filterDF.columns):
			filterDF = filterDF.rename(columns={'frequency': 'filter_count'})
		else:
			raise ValueError('Filter file must contain "count" or "frequency" column')	

		merged = retrievalDF.merge(filterDF, left_on='word', right_on='word')
	else: 
		merged = retrievalDF		

	merged = merged.sort_values(by='retrieval_count', ascending=False)
	
	# Dictionary-based filtering
	if dictionary_filter is not None and dictionary_filter != "None" and dictionary_filter != "none":
		merged = filterByDictionary(merged, dictionary_filter, language)
	
	#take the top n items after dictionary-based exclusion
	wordsToRetrieve = merged['word'].head(retrieval_count).tolist()

	print('Retrieving lexical surprisal estimates...')
	surprisalEstimates = [get_mean_surp(bigrams, backward_zs, w, cutoff) for w in wordsToRetrieve]

	df = pandas.DataFrame(surprisalEstimates)
	df.columns = ['word','mean_surprisal_weighted','mean_surprisal_unweighted','frequency','numContexts','retrievalTime']
	df.to_csv(outputfile, index=False, encoding='utf-8')	
	print('Done! Completed file is at '+outputfile+'; elapsed time is '+str(round(time.time()-start_time /  60., 5))+' minutes')			

def get_mean_surp(bigrams_dict,zs_file_backward, word, cutoff):	
	start_time = time.time()	
	total_freq = 0
	surprisal_total = 0
	num_context = 0
	unweightedSurprisal = 0	
	searchTerm = word+u" " #need a trailing space
	print 'Retrieving context probabilities for '+searchTerm	
	for record in zs_file_backward.search(prefix=searchTerm.encode('utf-8')):
		r_split = record.decode("utf-8").split(u"\t")
		ngram = r_split[0].split(u' ')
		#print r_split[0]
		count = int(r_split[1])
		if count >= cutoff:
			total_freq += count
			context =u" ".join(ngram[1:][::-1])+u' '
			num_context += 1
			if context in bigrams_dict:
				total_context_freq = bigrams_dict[context]
				cond_prob = -1 * np.log(count / float(total_context_freq))
				#print cond_prob
				surprisal_total += (count * cond_prob) #this is weighted by the frequency of this context
				unweightedSurprisal +=  cond_prob #this is not
			else:
				print('Missing context: '+ context) 
				#pdb.set_trace()
				#there should not be any missing values			
		else:
			continue	
	stop_time = time.time()
	st = None if total_freq == 0 else surprisal_total / float(total_freq)
	uwst = None if num_context == 0 else unweightedSurprisal / float(num_context)
	return (word, st, uwst, total_freq, num_context, (stop_time-start_time))

def filterByDictionary(merged, dictionary_filter, language):		
	if dictionary_filter is None :	
		print('Not limiting words to a spelling dictionary')
		pass
	elif dictionary_filter in ('lowerInDictionary', 'inDictionary'):			
		aspellLang = language

		if aspellLang == 'pt':
			aspellLang = 'pt-BR'

		speller = aspell.Speller(('lang',aspellLang),('encoding','utf-8'))

		merged['aspell_lower'] = [speller.check(x.lower().encode('utf-8')) == 1 for x in merged['word']]

		if dictionary_filter == 'lowerInDictionary':
			if aspellLang == 'de':
				raise ValueError('German must use inDictionary filter setting because all nouns are capitalized')
			print('Limiting to words with lower-case in spelling dictionary')
			#German nouns are capitalized, so need to check upper case
			merged = merged[merged['aspell_lower']] #only take the upper case one
		elif dictionary_filter == 'inDictionary':			
			print('Limiting to words with lower-case or upper-case in spelling dictionary')			
			merged['aspell_upper'] = [speller.check(x.title().encode('utf-8')) == 1 for x in merged['word']]
			#this should be checking if speller.check has x.upper
			merged = merged[merged['aspell_upper'] | merged['aspell_lower']]	
	else:
		raise ValueError('Dictionary specification not recognized. Choose None, "lowerInDictioanry" or "inDictionary"')	
	return(merged)		

def addSublexicalSurprisals(retrieval_file, retrieval_count, model_file,model_count, output_file, column, language, dictionary_filter, smoothing, token_corpus=None, token_filter=None, overwriteTokens=False):
	'''get the probability of each word's letter sequence using the set of words in the language
		#retrieval_file : set of types to compute sublexical surprisal
		# retrieval_count: number of types to retrieve
		# model_file: file to build the sublexical measure
		# model_count: number of words to include in the sublexical measure
		# output_file: file to which the data should be added
		# column: name of the output column
		# language: name of the language
		# dictionary_filter: filter to in-dictionary types?
		# smoothing: choice of smoothing type
		# token_corpus: corpus to build the token-weighted estimate
		# token_filter:
		# overwrite tokens:
		#fifth is the country code, which is used in the call to espeak and/or aspell
	''' 	
	print('Retrieving sublexical surprisal estimates...')

	# Load the model DF
	modelDF = ngrawk2.ngrawk_utils.readCSVorTxt(model_file)
	if ('count' in modelDF.columns):
		modelDF = modelDF.rename(columns={'count': 'retrieval_count'})
	elif ('frequency' in lex.columns):
		modelDF = modelDF.rename(columns={'frequency': 'retrieval_count'})
	else:
		raise ValueError('Model file must contain "count" or "frequency" column')	
	modelDF = modelDF.sort_values(by='retrieval_count', ascending=False)
	
	# filter for the dataset used to compute sublexical measures
	if dictionary_filter is not None and dictionary_filter != "None" and dictionary_filter != "none":
		modelDF = filterByDictionary(modelDF, dictionary_filter, language)

	modelDF = modelDF.head(model_count)

	# Load the retrieval DF
	retrievalDF = ngrawk2.ngrawk_utils.readCSVorTxt(retrieval_file)
	if ('count' in retrievalDF.columns):
		retrievalDF = retrievalDF.rename(columns={'count': 'retrieval_count'})
	elif ('frequency' in lex.columns):
		retrievalDF = retrievalDF.rename(columns={'frequency': 'retrieval_count'})
	else:
		raise ValueError('Retrieval file must contain "count" or "frequency" column')	
	retrievalDF = retrievalDF.sort_values(by='retrieval_count', ascending=False)
	if dictionary_filter is not None and dictionary_filter != "None" and dictionary_filter != "none":
		retrievalDF = filterByDictionary(retrievalDF, dictionary_filter, language)
	retrievalDF = retrievalDF.head(retrieval_count)

	sublexLMfileDir = os.path.join(os.path.dirname(output_file), column)
	if not os.path.exists(sublexLMfileDir):
		os.makedirs(sublexLMfileDir)

	if column == 'character':				
		retrieval_pm = retrievalDF
		model_pm = modelDF

		retrieval_pm['character'] = [list(x) for x in retrieval_pm['word']]
		model_pm['character'] = [list(x) for x in model_pm['word']]


		LM = trainSublexicalSurprisalModel(model_pm, column, order=5, smoothing=smoothing, smoothOrder=[3,4,5], interpolate=True, sublexlmfiledir = sublexLMfileDir)	
		retrieval_pm[column+'_ss_array']   = [getSublexicalSurprisal(transcription, LM, 5, 'letters', returnSum=False) for transcription in list(retrieval_pm[column])]	

	elif column == "token_character":	
		retrieval_pm = retrievalDF
		retrieval_pm['character'] = [list(x) for x in retrieval_pm['word']]
		retrieval_pm['token_character'] = retrieval_pm['character']

		char_path = os.path.join(sublexLMfileDir, 'token_character.txt')
		if os.path.exists(char_path) and not overwriteTokens:			
			print('Using previously generated token files')
			char_file = char_path

		else:
			print('Generating token files')				

			# should write both character and IPA model from letterize
			char_file = letterize2(
				inputfile = token_corpus,
				outputfile =  char_path,
				filterfile = token_filter,				
				splitwords = True,
				espeak_lang = None,
				phonebreak = '" "',
				par = True,
				espeak_model = None)

		#then train models with these as the input
		char_LM = trainTokenModel(char_file, order=5, outputfile=os.path.join(sublexLMfileDir,'token_character.LM'))
			
		retrieval_pm[column+'_ss_array'] = [getSublexicalSurprisal(transcription, char_LM, 5, 'letters', returnSum=False) for transcription in list(retrieval_pm['character'])]
			#note that the queries are from the character column


	elif column in ('ipa','token_ipa'):			
		
		#get the IPA representation from espeak
		if language == u'en':
			espeak_lang = u'en-US'
		elif language == u'he':
			print 'No Hebrew support for Espeak, returning None for IPA'
			return None
		else:
			espeak_lang = language	
		
		

		print('Retrieving IPA for all words in sample...')
		n = multiprocessing.cpu_count()			
		retrieval_pronunciations = Parallel(n_jobs=n)(delayed(espeak.espeak)(*i) for i in [(espeak_lang, x) for x in retrievalDF['word']])

		retrieval_pdf = pandas.DataFrame(retrieval_pronunciations) #this has a column "ipa"				
		retrieval_pm = retrievalDF.merge(retrieval_pdf, left_on="word", right_on="word")		

		#exclude items where pronunctiation is more than twice as long as the number of characters. This filters out many abbreviations  
		retrieval_pm['nSounds'] = [len(x) for x in retrieval_pm['ipa']]	
		retrieval_pm['suspect'] = retrieval_pm.apply(lambda x: (x['nSounds']/2.) > len(x['word']), axis=1)
		retrieval_pm = retrieval_pm.ix[~retrieval_pm['suspect']]

		if column  == 'ipa':
			print('Building a type-weighted model')
			print('Retrieving IPA for words in model...')


			espeak_results = Parallel(n_jobs=n)(delayed(espeak.espeak)(*i) for i in [(espeak_lang, x) for x in modelDF['word']]) 		
			#vocab['espeak'] = [x['ipa'] for x in espeak_results]
			#model_pronunciations = [espeak.espeak(espeak_lang,x) for x in modelDF['word']]		
			model_pdf = pandas.DataFrame(espeak_results)
			model_pm = modelDF.merge(model_pdf, left_on="word", right_on="word")	

			
			LM = trainSublexicalSurprisalModel(model_pm, column, order=5, smoothing=smoothing, smoothOrder=[3,4,5], interpolate=True, sublexlmfiledir=sublexLMfileDir)	
			retrieval_pm[column+'_ss_array']   = [getSublexicalSurprisal(transcription, LM, 5, 'letters', returnSum=False) for transcription in list(retrieval_pm[column])]

		elif column == 'token_ipa':
			
			# input paths:
			# token_corpus : plaintext corpus from which we want token frequencies
			# token_filter : wordlist for limiting the token corpus

			# output paths
			# sublexLMfileDir

			phone_path = os.path.join(sublexLMfileDir, 'token_ipa.txt')
			if os.path.exists(phone_path) and not overwriteTokens:				
				print('Using previously generated token files')
				phone_file = phone_path

			else:
				print('Generating token files')	

				# should write both character and IPA model from letterize
				phone_file = letterize2(
					inputfile = token_corpus,
					outputfile =  phone_path,
					filterfile = token_filter,					
					splitwords = True,
					espeak_lang = espeak_lang,
					phonebreak = '" "',
					par = True,
					espeak_model = None)

			#then train models with these as the input
			phone_LM = trainTokenModel(phone_file, order=5, outputfile=os.path.join(sublexLMfileDir,'token_ipa.LM'))

			retrieval_pm['token_ipa'] = retrieval_pm['ipa']
			retrieval_pm[column+'_ss_array']   = [getSublexicalSurprisal(transcription, phone_LM, 5, 'letters', returnSum=False) for transcription in list(retrieval_pm['ipa'])]
			#note that the queries are from ipa rather than token_ipa
	
	elif column == 'ortho':
		retrieval_pm = retrievalDF
		retrieval_pm['ortho'] = [list(x) for x in retrieval_pm['word']]
		retrieval_pm[column+'_ss_array'] = [[1]*len(x) for x in retrieval_pm['ortho']]

		#use pm['word']
	elif column == 'sampa':		
		raise ValueError('Out of date procedure for obtaining SAMPA model')	
		retrieval_pm =  retrievalDF
		if not 'sampa' in retrieval_pm.columns:
			print 'Must have SAMPA column to compute sublexical model for SAMPA'
			return None #can't compute SAMPA on the fly
		retrieval_pm['sampa'] = [x.split(' ') for x in retrieval_pm['sampa']]
		LM = trainSublexicalSurprisalModel(modelDF, column, order=5, smoothing=smoothing, smoothOrder=[3,4,5], interpolate=True, sublexlmfiledir= sublexLMfileDir)	
		retrieval_pm[column+'_ss_array']   = [getSublexicalSurprisal(transcription, LM, 5, 'letters', returnSum=False) for transcription in list(retrieval_pm[column])]
	else:
		raise ValueError('Acceptable column types are sampa, character, and ortho')	
	
	
	retrieval_pm[column+'_ss'] = [sum(x) if x is not None else 0 for x in retrieval_pm[column+'_ss_array']]	
	retrieval_pm[column+'_n'] = [len(x) if x is not None else 0 for x in retrieval_pm[column+'_ss_array']]
	
	#add the new results to the output_file and write it out
	if os.path.exists(output_file):
		aug = pandas.read_csv(output_file, encoding='utf-8').dropna()	
		if column in aug.columns:
			#columns already exist in the file, so we want to overwrite it
			retrieval_pm[['word', column, column+'_ss_array', column+'_ss', column+'_n']].to_csv(output_file, index=False, encoding='utf-8')			
		else:				
			aug.merge(retrieval_pm[['word', column, column+'_ss_array', column+'_ss', column+'_n']], left_on="word", right_on="word").to_csv(output_file, index=False, encoding='utf-8')
	else: 
		retrieval_pm[['word', column, column+'_ss_array', column+'_ss', column+'_n']].to_csv(output_file, index=False, encoding='utf-8')			
	print('Done!')	

def trainTokenModel(inputfile, order, outputfile):
		''' Train an n-gram language model using a token inventory 

			inputfile: path to a file with one word per line, separated by a space
			order: integer representing the highest order encoded in the language model
			
			outputfile: path to the resulting language model
		'''	
		commandString = 'ngram-count -text '+inputfile+' -order ' + str(order) + ' -lm ' + outputfile

		subprocess.call(commandString, shell=True)
		
		# load the language model and return it
		lm = srilm.LM(outputfile, lower=True)
		return(lm)	

def trainSublexicalSurprisalModel(wordlist_DF, column, order, smoothing, smoothOrder, interpolate, sublexlmfiledir):	
	''' Train an n-gram language model using a list of types 

		wordList_DF: a pandas data DataFrame
		column: the name of the pandas data frame to use 
		order: integer representing the highest order encoded in the language model
		smoothing: Smoothing technique: 'wb' or 'kn'
		smoothOrder: list of integers, indicating which orders to smooth
		interpolate: boolean, indicating whether to use interpolation or not
		sublexlmfiledir: where should the type file and the language model be stored?

	'''

	# ensure that ngram-count is on the path. shouldn't need to do this from the command line	
	#os.environ['PATH'] = os.environ['PATH']+':'+srilmPath
	#generate the relevant filenames
	typeFile = os.path.join(sublexlmfiledir, 'typeFile.txt')
	modelFile = os.path.join(sublexlmfiledir, 'types.LM')

	# write the type inventory to the outfile
	outfile = io.open(typeFile, 'w',encoding='utf-8')
	sentences=[u' '.join(transcription) for transcription in wordlist_DF[column]] 
	outfile.write('\n'.join(sentences))
	outfile.close()

	# train a model with smoothing on the outfile
	if smoothing is not None:
		discounting = ' '.join([''.join(['-', smoothing,'discount', str(x)]) for x in smoothOrder])
		commandString = 'ngram-count -text '+typeFile+' -order ' + str(order) + ' ' + discounting + (' -interpolate' if interpolate else '') + ' -lm ' + modelFile
	else:	
		commandString = 'ngram-count -text '+typeFile+' -order ' + str(order) + ' -lm ' + modelFile

	subprocess.call(commandString, shell=True)

	# load the language model and return it
	lm = srilm.LM(modelFile, lower=True)
	return(lm)

def getSublexicalSurprisal(targetWord, model, order, method, returnSum):		

	''' Get the sublexical surprisal for a word
		targetWord: type for which surprisal is calculated
		model: pysrilm LM object
		order: specify n of n-gram model. e.g. 1 for unigrams
		method: get probability of sounds or letters. 
				if sounds, input must be a list of phones
		returnSum: if true, return sum of surprisal values
				otherwise, return a list of surprisal values		
	'''
	print  'Getting sublexical surprisal: '+''.join(targetWord).encode('utf-8')	
	if (method == 'sounds'):		
		#throw an error if the variable word is not already a list
		word = ['<s>'] + targetWord + ['</s>'] #append an end symbol
		infoContent = list()	
		raise NotImplementedError	
	elif (method == 'letters'):		
		# if type(targetWord) is not str:
		# 	pdb.set_trace()		

		if(len(targetWord) == 0):
			return(None)
			#proceed to the next one
		else:
			word = ['<s>'] + targetWord + ['</s>'] 
			infoContent = list()

	for phoneIndex in range(len(word)):
		if(phoneIndex - order < 0):
			i = 0 #always want the start to be positive 
		else:
			i = phoneIndex - order + 1 					
		target=word[phoneIndex].encode('utf-8') 				 		
		preceding=[x.encode('utf-8') for x in word[i:phoneIndex][::-1]] #pySRILM wants the text in reverse 		
		phonProb = model.logprob_strings(target,preceding)
		#print('Target: '+target,': preceding: '+' '.join(preceding)+'; prob:'+num2str(10**phonProb,5))
		infoContent.append(-1*phonProb)

	infoContent = infoContent[1:-1] #remove the beginning and end marker									
	if (all ([ x is not None for x in infoContent])):
		if returnSum:
			return(sum(infoContent))
		else:
			return(infoContent)	
	else:
		return(None)
