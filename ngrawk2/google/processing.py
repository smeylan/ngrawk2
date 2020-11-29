#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import io
import time

import ngrawk2.ngrawk_utils 
import ngrawk2.surprisal
import ngrawk2.google.cleaning
import warnings


def getGoogleBooksLanguageModel(corpusSpecification, n, direction, filetype, colnames, sum_columns):
	'''Metafunction to create a ZS language model from Google Ngram counts. Does a linear cleaning, merges the file into a single document, sorts it, collapses identical prefixes, and builds the ZS file.'''
	startTime = time.time()
	lexSurpDir = os.path.join(corpusSpecification['faststoragedir'], corpusSpecification['analysisname'],corpusSpecification['corpus'],corpusSpecification['language'],'00_lexicalSurprisal')


	# if not corpusSpecification['collapseyears']: #keeping dates is too large to keep the intermediate files on the ssd			
	# 	intermediateFileDir = os.path.join(corpusSpecification['slowstoragedir'],corpusSpecification['corpus'],corpusSpecification['language'])
	# else:
	intermediateFileDir	= lexSurpDir
	
	zs_metadata = {  #!!! should this be the corpusSpecification dictionary
		"corpus": corpusSpecification['corpus'],
		"language": corpusSpecification['language'],
		"n": n,
		"direction": direction
	}
	print zs_metadata
	
	zsFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'.zs')
	if not os.path.exists(zsFile):
		print('Checking if there are appropriate cleaned text files to create lower-order language model...')
		if corpusSpecification['collapseyears']:
			tryHigher = ngrawk2.ngrawk_utils.deriveFromHigherOrderModel(intermediateFileDir, n, direction, colnames=['ngram','count'], sum_columns=['count'])
		else:	
			tryHigher = ngrawk2.ngrawk_utils.deriveFromHigherOrderModel(intermediateFileDir, n, direction, colnames=['year','ngram','count'], sum_columns=['count'])

		if tryHigher is not None:
			print('Derived model from higher order model, results are at '+str(tryHigher))
			collapsedfile = tryHigher
		else:
			print('No higher-order or reversible models found. Cleaning the input files... If n > 3 and the language is English, this is a good time to grab a coffee, this will take a few hours.')

			#find only lines without POS tags and make them lowercase
			inputdir = os.path.join(corpusSpecification['inputdir'],corpusSpecification['corpus'],corpusSpecification['language'],str(n))
			outputdir = os.path.join(corpusSpecification['slowstoragedir'],corpusSpecification['analysisname'], corpusSpecification['corpus'],corpusSpecification['language'],str(n)+'-processed')	
		
			combinedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-combined.txt')	
			if corpusSpecification['collapseyears']:				
				cleanFileProp = ngrawk2.ngrawk_utils.checkForMissingFiles(inputdir, '*.'+filetype, outputdir, '*.yc')	
				if cleanFileProp < .5: #!!!
					ngrawk2.google.cleaning.cleanGoogleDirectory0(inputdir,outputdir,corpusSpecification['collapseyears'], n, colnames)
					cleanFileProp = ngrawk2.ngrawk_utils.checkForMissingFiles(inputdir, '*.'+ filetype, outputdir, '*.yc')	
					if cleanFileProp < .9:
						raise ValueError('No cleaned files!')


			else:					
				cleanFileProp = ngrawk2.ngrawk_utils.checkForMissingFiles(inputdir, '*.'+filetype, outputdir, '*.output')
				if cleanFileProp < .5: #!!!
					print('Begin cleaning...')
					ngrawk2.google.cleaning.cleanGoogleDirectory0(inputdir,outputdir,corpusSpecification['collapseyears'], n, colnames)
					ngrawk2.ngrawk_utils.checkForMissingFiles(inputdir, '*.'+filetype, outputdir, '*.output')
					
			if corpusSpecification['collapseyears']:				
				ngrawk2.ngrawk_utils.combineFiles(outputdir, '*.yc', combinedfile)	
			else:	
				ngrawk2.ngrawk_utils.combineFiles(outputdir, '*.output', combinedfile)
						
			#reorder the columns if specified, e.g. to get center-embedded trigrams	
			if corpusSpecification['target'] != corpusSpecification['order']:				
				reorderedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-reordered.txt')
				reorderGoogleFile(combinedfile, reorderedfile, int(corpusSpecification['target']))
				fileToReverse = reorderedfile				
			else:
				fileToReverse = combinedfile

			#reverse if specified
			if direction == 'backwards':
				reversedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-reversed.txt')
				if corpusSpecification['collapseyears']:
					ngrawk2.ngrawk_utils.reverseGoogleFile(fileToReverse, reversedfile, ['ngram','count'])
				else: 
					ngrawk2.ngrawk_utils.reverseGoogleFile(fileToReverse, reversedfile, ['year','ngram','count'])
				fileToSort = reversedfile
			elif direction == 'forwards':	
				fileToSort = reorderedfile
			
			#sort it	
			sortedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-sorted.txt')
			ngrawk2.ngrawk_utils.sortNgramFile(fileToSort, sortedfile)		

			#collapse after the sorting: this deals with different POS treatments 
			collapsedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-collapsed.txt')
			if corpusSpecification['collapseyears']:
				ngrawk2.google.cleaning.collapseGoogleNgrams(sortedfile, collapsedfile, id_columns=['ngram'], sum_columns=['count'], colnames=['ngram','count'])
			else:
				ngrawk2.google.cleaning.collapseGoogleNgrams(sortedfile, collapsedfile, id_columns=['year','ngram'], sum_columns=['count'], colnames = ['year','ngram','count'])
				#os.system('cp '+sortedfile+' '+collapsedfile)	
								
		#build the language model
		zsFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'.zs')	
		ngrawk2.surprisal.trainLexicalZSmodel(collapsedfile, zsFile, zs_metadata, codec="none")

		print('Done! Completed file is at '+zsFile+'; elapsed time is '+str(round(time.time()-startTime, 5))+' seconds') 
	
	else:
		print('ZS file already exists at '+zsFile) 
	return(zsFile)


def reorderGoogleFile(inputfile, outputfile, index):
	'''Reorder the columns in a Google-formatted ngram file to put the word at targetWordIndex as the last item. This supports the reordering of columns so that the context is the preceding + following word, for example.'''
	if index < 1:
		raise ValueError('targetWordIndex should be indexed from 1 (like the order argument)')

	#indexing from 1, the target word index can either be 1, the length of the array, or length of array +1/2 (for the center embedded trigram. The function should return an error if there are an even number of items and the target word is not the first or last
	print('Reordering existing model')		
	iff = io.open(inputfile, 'r', encoding='utf-8')
	firstLine = ''
	while firstLine == '\n' or firstLine == '':
		firstLine = iff.readline()
	numWords = len(firstLine.split('\t')[0].split(' '))	
	if not index in (1, numWords, (numWords+1.)/2.):
		raise ValueError('targetWordIndex needs to be the first, last, or center item')
	iff.close()
	
	iff = io.open(inputfile, 'r', encoding='utf-8')			
	off = io.open(outputfile, 'w', encoding='utf-8')			
	for l in iff:
		strArray = l.split('\t')		
		if len(strArray) > 0: #this cleans any empty lines that are produced by the cleaning process
			ngram = strArray[0].split(' ')
			if len(ngram) > 0 and ngram != [u'\n']: #only retain proper ngrams	
				targetWord = [ngram[index-1]]
				context = ngram
				del context[index-1]
				strArray[0] = ' '.join(context+targetWord)
				off.write('\t'.join(strArray))
	iff.close()
	off.close()
	print('Done!')	
