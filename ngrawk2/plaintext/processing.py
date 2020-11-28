#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
import sys
import codecs
import pandas
import multiprocessing
import unicodedata
import joblib
import time
import espeak
from joblib import delayed, Parallel
import numpy as np

import ngrawk2.ngrawk_utils 
import ngrawk2.surprisal

def getPlaintextLanguageModel(corpusSpecification, n, direction, cleaningFunction):	
	'''This metafunction produces a ZS language model from a large plaintext document using the program "ngrams" from the AutoCorpus Debian package to count the n-gram frequencies for a specified order (n). Example use: for producing a ZS file from the BNC.'''
	startTime = time.time()
	lexSurpDir = os.path.join(corpusSpecification['faststoragedir'], corpusSpecification['analysisname'],corpusSpecification['corpus'],corpusSpecification['language'],'00_lexicalSurprisal')	

	zs_metadata = corpusSpecification
	zs_metadata["zs_n"] = n
	zs_metadata["direction"] = direction
	print zs_metadata
	
	tbl = dict.fromkeys(i for i in xrange(sys.maxunicode)
	if unicodedata.category(unichr(i)).startswith('P'))
	
	inputfile = os.path.join(corpusSpecification['inputdir'],corpusSpecification['corpus'],corpusSpecification['language'],corpusSpecification['filename'])
	cleanedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-cleaned.txt')
	countedFile= os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-counted.txt')
	countMovedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-countMoved.txt')
	sortedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-sorted.txt')
	collapsedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-collapsed.txt')
	zsFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'.zs')	

	cleanTextFile(inputfile, cleanedFile, cleaningFunction)
	countNgrams(cleanedFile, countedFile, n)
	ngrawk2.ngrawk_utils.rearrangeNgramFile(countedFile, countMovedFile, direction)
	ngrawk2.ngrawk_utils.sortNgramFile(countMovedFile, sortedFile)
	os.system ("cp "+sortedFile+" "+collapsedFile) #this just copies it, so filenames are equivalent to the google procedure
	ngrawk2.surprisal.trainLexicalZSmodel(collapsedFile, zsFile, zs_metadata, codec="none")

	print('Done! Completed file is at '+zsFile+'; elapsed time is '+str(round(time.time()-startTime, 5))+' seconds') 
	return(zsFile)

def cleanTextFile(inputfile, outputfile, cleaningFunction):
	'''Cleans a plaintext file line by line with the function specified in cleaningFunction'''
	print('Cleaning the plaintext file...')

	tbl = dict.fromkeys(i for i in xrange(sys.maxunicode)
                      if unicodedata.category(unichr(i)).startswith('P'))
	tbl.pop(ord(u"'")) #remove apostrophe from the list of punctuation

	def cleanLine_BNC(l):
		return remove_punctuation(l.lower(), tbl)
	cleanLineOptions = {'cleanLine_BNC': cleanLine_BNC}


	iff = codecs.open(inputfile, 'r', encoding='utf-8')
	off = codecs.open(outputfile, 'w', encoding='utf-8')	

	for line in iff:		
		off.write(cleanLineOptions[cleaningFunction](line))

	iff.close()
	off.close()

def countNgrams(inputfile, outputfile, n):
	'''Produces an ngram count for a text file using the ngrams command from Autocorpus'''
	print('Counting the ngrams...')
	ngramsCommand = 'cat '+inputfile+' | /usr/bin/ngrams -n '+str(n)+' > '+outputfile
	subprocess.call(ngramsCommand, shell=True)	

def permuteTextFile(inputfile, outputfile):
	'''creates a permuted ordering of a text'''	
	print('Permuting the plaintext file '+ inputfile)

	print('Retrieving the uniphone and space counts...')
	charcounts = {}	
	lineLengths = []
	spaces = 0 # keep spaces separate

	iff = codecs.open(inputfile, 'r', encoding='utf-8')	
	for line in iff:		
		letters = list(line)
		lineLengths.append(len(letters))
		for letter in letters:
			if letter == u' ':
				spaces += 1
			elif letter == u'\n':
				pass #don't do anything with an end of line character	
			elif letter in charcounts:
				charcounts[letter] += 1

			else:
				charcounts[letter] = 1	

	print('Generating first and last letter of every sentence...')
	letters = np.array(charcounts.keys())			
	letterIndices = numpy.array(range(len(letters)), dtype =np.uint8)
	counts = np.array(charcounts.values(), dtype =np.float)			
	probs = counts/np.sum(counts)

	
	bookends = []
	for line in range(len(lineLengths)):
		line_bookends = np.random.choice(letterIndices,size=2,p=probs)	#this returns a number			
		bookends.append(line_bookends)

		for j in line_bookends:
			counts[j] -= 1.			
		probs = counts/np.sum(counts) # recompute the probs	

	print('Generating the center of every sentence...')
	# now add back the spaces and draw from indices 1: n-1, including space; 
	# retreive the first and last letter from bookends	
	letters_withSpace = np.array(charcounts.keys() + [u' '])				
	spaceIndex = len(letters_withSpace)-1
	letterIndices_withSpace = range(len(letters_withSpace))		
	counts_withSpaces = np.hstack([counts, np.float(spaces)])			
	probs_withSpaces = counts_withSpaces/np.sum(counts_withSpaces)

	off = codecs.open(outputfile, 'w', encoding='utf-8')	
	for lineIndex in range(len(lineLengths)):
		lineLength = lineLengths[lineIndex]
				
		middle_content = []
		lastLetter = -1
		for index in range(lineLength-2): #omitting the first and the last letter
			newLetter = np.random.choice(letterIndices_withSpace, p=probs_withSpaces)
			redrawCount = 0
			while newLetter == spaceIndex and lastLetter == spaceIndex:
				#print('drawing a double space')				
				redrawCount += 1
				if redrawCount > 100:
					print(counts_withSpaces)
					break
				newLetter = np.random.choice(letterIndices_withSpace, p=probs_withSpaces)

			middle_content.append(newLetter)	
			lastLetter = newLetter
			counts_withSpaces[newLetter] -=1.			
			# update the probablities	
			probs_withSpaces = counts_withSpaces/np.sum(counts_withSpaces)		


		stringToWrite = ''.join([letters_withSpace[x] for x in ([bookends[lineIndex][0]]+middle_content +[bookends[lineIndex][1]])]) 

		off.write(stringToWrite+u'\n')		

	iff.close()
	off.close()
	print('Finished permuting plaintext file.')	

def letterize2(inputfile, outputfile, filterfile, splitwords, espeak_lang, phonebreak, par, espeak_model):
	'''take textfile, split by words, and output a list of letters or phones (phones if espeak_lang is not None) separated by phonebreak. This can then be used as input to SRILM for various models'''

	if espeak_lang == 'None': # make sure None is not treated as a string
		espeak_lang = None

	if (espeak_lang is not None) and (espeak_model is None):
		print('building an espeak model...')
		
		# vocab is produced in the OPUS stack, e.g. OPUS/main.py 	
		vocab = pandas.read_table(filterfile, skiprows=2, header=None, encoding='utf-8') 
		vocab.columns = ['word','count']
		#vocab['espeak'] = [espeak.espeak(espeak_lang,x)['ipa'] for x in vocab['word']]

		print('Retrieving espeak transcriptions...')
		n = multiprocessing.cpu_count()
		espeak_results = Parallel(n_jobs=n)(delayed(espeak.espeak)(*i) for i in [(espeak_lang, x) for x in vocab['word']]) 		
		vocab['espeak'] = [x['ipa'] for x in espeak_results]

		espeak_model = dict(zip(vocab['word'], vocab['espeak']))
		print('Espeak model ready!')		

	if par:
		print 'Calling parallelized version of letterize'
		arguments = {
			'inputfile':inputfile,
			'outputfile':outputfile,
			'filterfile': filterfile,			
			'splitwords':splitwords,
			'espeak_lang': espeak_lang,
			'phonebreak':phonebreak,
			'espeak_model': espeak_model}
		return(embpar(letterize2, arguments))
	else:	
		print 'Executing single-thread version of letterize'
				
		input_f = codecs.open(inputfile, 'r', encoding='utf-8')		
		output_f = codecs.open(outputfile, 'w', encoding='utf-8')		

		phonebreak = phonebreak[1:-1] #get rid of quotes


		for c,l in enumerate(input_f):
			if l == '\n':
				pass
			else:				
				if splitwords: #output each word on a separate line		
					words = l.replace('\n','').lower().split(' ')					
					for word in words:
						if espeak_lang is None:
							output_f.write(phonebreak.join(list(word))+'\n')
						else:
							if word in espeak_model: 
								output_f.write(phonebreak.join(espeak_model[word])+'\n')
				else: #all on a single line
					if espeak_lang is None:
						output_f.write(phonebreak.join(list(l.replace(' ','')))+'\n')		
					else:
						words = l.replace('\n','').lower().split(' ')
						translatedWords = []
						for word in words:							
							if word in espeak_model: #check if that token is cached already
								translatedWords.append(phonebreak.join(espeak_model[word]))	
							output_f.write(u' '.join(translatedWords)+'\n')	

		input_f.close()		
		output_f.close()
		return(outputfile)			

def filterByWordList(inputfile, outputfile, loweronly, vocabfile,n, par):
	'''take a textfile, split by words, and check if each word is in the provided vocabfile'''
	if(par):
		print 'Calling parallelized version of filterByWordList'
		arguments = {'inputfile':inputfile,'outputfile': outputfile, 'loweronly':loweronly, 'vocabfile':vocabfile, 'n':n}
		embpar(filterByWordList, arguments)
	else:		
		print 'Executing single-thread version of filterByWordList'
		def filterWords(l, loweronly, vocab):		
			if loweronly:
				words = l.replace('\n','').split(' ')
				return(u' '.join([word for word in words if word in vocab]))
			else: 	
				words = l.replace('\n','').lower().split(' ')
				return(u' '.join([word for word in words if word in vocab]))

		vocab = set(pandas.read_table(vocabfile, encoding='utf-8', sep='\t')['word'][0:n])

		iff = codecs.open(inputfile, 'r')
		off = codecs.open(outputfile, 'w', encoding='utf-8')	

		bufsize = 100000	
		lineStore = []

		for c,l in enumerate(iff):
			lineStore.append(l)
			if c % bufsize == 0:#refresh the buffer	
				rows = [filterWords(l,loweronly,vocab) for l in lineStore]
				off.write('\n'.join(rows)+'\n')
				lineStore =[] 
				print 'Processed '+os.path.basename(inputfile)+' through line '+ str(c)
		rows = [filterWords(l,loweronly,vocab) for l in lineStore]	
		off.write('\n'.join(rows)+'\n')

		iff.close()		
		off.close()
		return(outputfile)		

def par_filterByWordList(idc):
	filterByWordList(idc['inputfile'], idc['outputfile'], idc['loweronly'], idc['vocabfile'],idc['n'], par=False)

def par_letterize2(idc):
	return(letterize2(idc['inputfile'], idc['outputfile'], idc['filterfile'], idc['splitwords'], idc['espeak_lang'], idc['phonebreak'], False, idc['espeak_model']))		

functionMappings = {
	'filterByWordList' : par_filterByWordList,	
	'letterize2' : par_letterize2
}	

def embpar(functionName, arguments):
	#dict wrappers for functions that can be called with embpar

	def file_len(fname):
	    with open(fname) as f:
	        for i, l in enumerate(f):
	            pass
	    return i + 1

	def split_seq(numItems, numRanges):
		newseq = []
		splitsize = 1.0/numRanges*numItems
		for i in range(numRanges):
			newseq.append((int(round(i*splitsize)),int(round((i+1)*splitsize))))
		return newseq

	def splitfile(inputfile, n):
		'''divide inputfile into n approximately equal-sized parts.'''			
		fileLength = file_len(arguments['inputfile'])
		lineRanges = split_seq(fileLength, n)
		rangeStarts = set([x[0] for x in lineRanges])	

		iff = codecs.open(arguments['inputfile'], 'r',encoding='utf-8')
		filenames = []
		
		for c,l in enumerate(iff):			
			if c in rangeStarts: #switch the output file
				filename = arguments['inputfile']+'-'+str(c)
				filenames.append(filename)
				off = codecs.open(filename, 'w', encoding='utf-8')	
			off.write(l)	
		off.close()	
		return(filenames)

	n = multiprocessing.cpu_count()	
	print 'Splitting file: '+arguments['inputfile']
	subfiles = splitfile(file_len(arguments['inputfile']), n)	
	#subfiles = glob.glob(os.path.join(os.path.dirname(arguments['inputfile']),'*.txt-*'))

	#get the string of the function name
	if functionName.__name__ is 'filterByWordList':
		#build the inputs
		print 'Building inputs for parallelization'
		inputs = [{'inputfile':subfiles[x],
					'outputfile':subfiles[x]+'_out',
					'loweronly': arguments['loweronly'],
					'vocabfile': arguments['vocabfile'],
					'n': arguments['n']} for x in range(0,n)]	
	elif functionName.__name__ in ('letterize','letterize2'):				
		inputs = [{'inputfile':subfiles[x],
					'outputfile':subfiles[x]+'_out',
					'filterfile': arguments['filterfile'],
					'splitwords':arguments['splitwords'],
					'espeak_lang':arguments['espeak_lang'],
					'phonebreak':arguments['phonebreak'],
					'espeak_model':arguments['espeak_model'],
					} for x in range(0,n)]

	print 'Starting parallelized execution...'					
	resultfiles = Parallel(n_jobs=n)(delayed(functionMappings[functionName.__name__])(i) for i in inputs)  
	#!!! resultfiles above is not giving back the appropriate filenames
	#resultfiles = glob.glob(os.path.join(os.path.dirname(arguments['inputfile']),'*_out'))

	print('Combining files from parallelization...')
	combineFiles(os.path.dirname(arguments['inputfile']), '*_out', arguments['outputfile'])	
	print('Deleting temporary files from parallelization...')

	[os.remove(file) for file in subfiles]	
	[os.remove(file) for file in resultfiles]
	return(arguments['outputfile'])

def remove_punctuation(text, tbl):
	'''remove punctuation from UTF8 strings given a character table'''
	return text.translate(tbl)
