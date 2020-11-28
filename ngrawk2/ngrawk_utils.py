#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import subprocess
import glob
import pandas
import tempfile
import re
import aspell
import numpy as np
import json
import warnings

def combineFiles(inputdir, pattern, outputfile):
	'''combines a set of text files in directory with filenames terminating with pattern into a single file; a wrapper for GNU cat'''
	print('Combining the cleaned files...')	
	catCommand = 'cat '+os.path.join(inputdir,pattern)+' > '+outputfile
	subprocess.call(catCommand, shell=True)
	print('Done!')

def sortNgramFile(inputfile, outputfile, num_threads=24):
	'''sorts an ngram file; basically a wrapper for GNU sort'''
	print('Sorting the combined file...')	
	sortCommand = 'env LC_ALL=C sort --compress-program=lzop '+inputfile+'  -o '+outputfile+' --parallel='+str(num_threads)
	subprocess.call(sortCommand, shell=True)
	print('Done!')	

def readCSVorTxt(filename): 
	'''load a CSV or a TXT file without complaining'''
	file_path, file_extension = os.path.splitext(filename)
	if(file_extension=='.txt'):
		df = pandas.read_table(filename, encoding='utf-8', keep_default_na=False, na_values=[]).dropna()
	elif(file_extension=='.csv'):
		df = pandas.read_csv(filename, encoding='utf-8', keep_default_na=False, na_values=[]).dropna()
	else:
		import pdb
		pdb.set_trace()	
	return(df)		

def rearrangeNgramFile(inputfile, outputfile, direction):	
	print('Rearranging the ngrams...')	
	iff = io.open(inputfile, 'r', encoding='utf-8')
	off = io.open(outputfile, 'w', encoding='utf-8')	
	for l in iff:		
		l = l.replace('\n','')
		strArray = l.split('\t')
		if (len(strArray) == 1):
			continue
		else:	
			count = strArray.pop(0)	
			ngram = strArray[0].split(' ')
			if direction == 'backwards':
				ngram.reverse()
			strArray = [' '.join(ngram)]
			strArray.append(count) #move the count to the end, reverse ngram				
			off.write('\t'.join(strArray)+'\n')
	iff.close()
	off.close()


def marginalizeNgramFile(inputfile, outputfile, n, sorttype, colnames, sum_columns):

	'''collapse counts from inputfile for sequences of length n'''
	print('Marignalizing over counts from higher-order ngram file to produce counts of '+str(n)+'-grams')	
	iff = io.open(inputfile, 'r', encoding='utf-8')
	
	tf_path = os.path.join(os.path.dirname(inputfile),next(tempfile._get_candidate_names()))
	tf = open(tf_path, 'w')
	tf = io.open(tf_path, 'w', encoding='utf-8')	

	firstLine ='\n' #handle any lines that are blank at the beginning of the text
	while firstLine == '\n' or firstLine == '':
		firstLine = iff.readline()

	line = firstLine.replace('\n','').split('\t')
	ncols = len(line)

	prev_line_dict = {}
	for i in range(len(colnames)):		
		prev_line_dict[colnames[i]] = line[i]

	prev_line_dict['ngram']	= ' '.join(prev_line_dict['ngram'].split(' ')[0:n])

	alpha_output_cols = colnames[:]
	numeric_output_cols = colnames[:]
	numeric_output_cols.remove('ngram')
	numeric_output_cols = ['ngram'] + numeric_output_cols

	print('Collapsing counts...')
	for l in iff:
		line = l.replace('\n','').split('\t')
		
		if len(line) != ncols:
			print 'Mismatch in line length and ncols, line was '+l
			import pdb
			pdb.set_trace()
			continue
		
		new_line_dict = {}
		for i in range(len(colnames)):		
			new_line_dict[colnames[i]] = line[i]

		new_line_dict['ngram'] = ' '.join(new_line_dict['ngram'].split(' ')[0:n])			

		if prev_line_dict['ngram'] != new_line_dict['ngram']:
			if (sorttype == 'numeric'):
				tf.write('\t'.join([prev_line_dict[x] for x in numeric_output_cols])+'\n')
			elif (sorttype == 'alphabetic'):
				tf.write('\t'.join([prev_line_dict[x] for x in alpha_output_cols])+'\n')
		
			prev_line_dict = new_line_dict.copy()
		else:
			#if it is the same ngram, add it to the aggregate count
			for key in sum_columns:
				prev_line_dict[key] = str(int(prev_line_dict[key]) + int(new_line_dict[key]))

			
	#obligate write of final cached value at the end 
	if sorttype == 'alphabetic':               
		tf.write('\t'.join([prev_line_dict[x] for x in numeric_output_cols])+'\n')
	else:
		tf.write('\t'.join([prev_line_dict[x] for x in alpha_output_cols])+'\n')

	iff.close()
	tf.close()

	print('Sorting new counts...')
	#then run sort on the output file
	if (sorttype == 'numeric'):
		os.system("sort -n -r "+tf_path+' > '+outputfile) # sorted by descending frequency
		addCommand = "sed -i '1s/^/word\\tcount\\n/' " # add 
		#addCommand = "sed -i '1s/^/count\\tword\\n/' " # add labels, do this post hoc so we can sort the file		
		os.system(addCommand + outputfile)
		##df = pandas.read_table(outputfile, sep='\t', encoding='utf-8')
		#df.to_csv(outputfile, encoding='utf-8') #overwrite the file

	elif (sorttype == 'alphabetic'):
		os.system("env LC_ALL=C sort "+tf_path+' > '+outputfile) # sorted alphabetically, suitable for putting into a ZS file         
	os.remove(tf_path)	

	print('Done!')	

def checkForMissingFiles(directory1, pattern1, directory2, pattern2):
	'''check which files from directory1 are not in directory2'''

	raw_files = glob.glob(os.path.join(directory1,pattern1))
	raw_filenames = [os.path.splitext(os.path.basename(x))[0] for x in raw_files]
	if len(raw_filenames) == 0:
		raise ValueError('No files matching search terms found in first directory')	
	print('Directory 1 contains '+str(len(raw_filenames)) + ' files')
	processed_files = glob.glob(os.path.join(directory2,pattern2))
	processed_filenames = [os.path.splitext(os.path.basename(x))[0] for x in processed_files]
	
	if len(raw_filenames) != len(processed_filenames):
		print('Differing number of raw and processed files')

		missing = []
		[missing.append(file) for file in raw_filenames if file not in processed_filenames]
		warnings.warn(('Missing files'))
		print(missing)		
	else:
		print('Same number of raw and processed files')	

	return (len(processed_filenames) /  (len(raw_filenames) * 1.))	

def checkForBinary(command):
	test = os.popen("which "+command).read()
	if test != '':
		print(command +' found at '+test)
	else:
		raise ValueError('binary for '+command +' not found')	

def cleanString(string): 
		return(''.join(e for e in string if e.isalpha() or e in ("'") or e.isspace()))				


def makeDirectoryStructure(faststoragedir, slowstoragedir, analysisname, corpus, language, n):		
	print('Creating fast storage directory at '+os.path.join(faststoragedir, analysisname, corpus, language)+'...')	

	corpusLanguagePath = os.path.join(faststoragedir, analysisname, corpus, language)				
	lexSurpDir = os.path.join(faststoragedir, analysisname,corpus,language,'00_lexicalSurprisal')
	sublexSurpDir = os.path.join(faststoragedir, analysisname,corpus,language,'01_sublexicalSurprisal')
	correlationsDir = os.path.join(faststoragedir, analysisname, corpus,language,'02_correlations')

	if not os.path.exists(corpusLanguagePath):
		os.makedirs(corpusLanguagePath)
	if not os.path.exists(lexSurpDir):
		os.makedirs(lexSurpDir)	
	if not os.path.exists(sublexSurpDir):
		os.makedirs(sublexSurpDir)
	if not os.path.exists(correlationsDir):
		os.makedirs(correlationsDir)
	print('Fast directories created!')

	processedDir = os.path.join(slowstoragedir, analysisname, corpus, language)
	print('Creating slow storage directory at '+processedDir+'...')	
	if not os.path.exists(processedDir):
		os.makedirs(processedDir)

	#create directories for all n, n-1, 1	
	ordersToMake = [n, n-1, 1]
	for i in ordersToMake:
		pathToMake = os.path.join(processedDir, str(i)+'-processed')
		if not os.path.exists(pathToMake):
			os.makedirs(pathToMake)

	return lexSurpDir, sublexSurpDir, correlationsDir, processedDir


def cleanUnigramCountFile(inputfile, outputfile, n, language, filterByDictionary):	
	'''filter the unigram count file, and reduce the number of items in it'''	

	df = pandas.read_table(inputfile, encoding='utf-8')	
	df.columns = ['word','count']
	#take some multiple of items to run the filters on
	
	#discard purely numeric items
	df_nonnumeric = df[[type(x) is unicode for x in df['word']]]	

	#discard the <s> string
	df_clean = df_nonnumeric[[x != u'</s>' for x in df_nonnumeric['word']]]

	#delete apostrophes, numbers
	df_clean['word'] = [re.sub(u"’|'|\d",'',x) for x in df_clean['word']]

	#check for any empty strings
	df_clean = df_clean[[x != '' and x is not  None for x in df_clean['word']]]		
	
	df_clean['word'] = [cleanString(x) for x in df_clean['word']] 

	#check whether the upper and lower case is in the dictionary
	aspellLang = language
	if aspellLang == 'pt':
		aspellLang = 'pt-BR'
	speller = aspell.Speller(('lang',aspellLang),('encoding','utf-8'))
	df_clean['aspell_upper'] = [speller.check(x.lower().encode('utf-8')) == 1 for x in df_clean['word']]
	df_clean['aspell_lower'] = [speller.check(x.title().encode('utf-8')) == 1 for x in df_clean['word']]
	
	#Convert anything that can be lower case to lower case
	df_clean['word'][df_clean['aspell_lower']] = [x.lower() for x in df_clean['word'][df_clean['aspell_lower']]]

	if filterByDictionary:
		#check the rejected words
		#df_clean.ix[~df_clean['aspell']]	
		if language == 'de':
			#German nouns are capitalized
			df_clean = df_clean.ix[np.logical_or(df_clean['aspell_lower'],df_clean['aspell_upper'])]		
		else:
			df_clean = df_clean.ix[df_clean['aspell_lower']]		


	to_write = df_clean.drop(['aspell_lower','aspell_upper'], axis=1)
	to_write['word'] = [x.lower() for x in to_write['word']]
	to_write.to_csv(outputfile, sep='\t', index=False, header=False, encoding='utf-8')
	print('Wrote to file: '+outputfile)		


def deriveFromHigherOrderModel(intermediatefiledir, n, direction, colnames, sum_columns):
	'''Search for a pre-computed model from which the desired counts can be derived either through reversing or marginalization'''
	if direction == 'forwards':
		oppositeDirection = 'backwards'
	elif direction == 'backwards':
		oppositeDirection = 'forwards'		
	
	#first look for a model in the same direction that is larger than the desired n

	availableModels = glob.glob(os.path.join(intermediatefiledir,'*'+direction+'-collapsed.txt'))
	modelOrders = [os.path.basename(x)[0] for x in availableModels if x > int(n)]
	if len(modelOrders) > 0:
		print 'Higher order model of same direction found; will marginalize counts...'
		NtoUse = min(modelOrders)
		inputfile = os.path.join(intermediatefiledir,str(NtoUse)+'gram-'+direction+'-collapsed.txt')
		outputfile = os.path.join(intermediatefiledir,str(n)+'gram-'+direction+'-collapsed.txt')
		#!!! sort before marginalization! may be okay
		marginalizeNgramFile(inputfile, outputfile, n, 'alphabetic', colnames, sum_columns)
		return(outputfile)
	else: #no models in the same direction, may need to reverse one
		availableModels = glob.glob(os.path.join(intermediatefiledir,'*'+oppositeDirection+'-collapsed.txt'))	 #look for ones of the opposite direction		
		modelOrders = [int(os.path.basename(x)[0]) for x in availableModels if x > int(n)]

		if len(modelOrders) > 0: # if there is at least one higher-order opposite-direction model
			print 'Higher order model of different direction found; will reverse, sort, and marginalize'
			NtoUse = min(modelOrders)						

			#reverse it-- the higher order model MUST be reversed before marginalization, or some low frequency trigrams are lost
			startingModel = os.path.join(intermediatefiledir,str(NtoUse)+'gram-'+oppositeDirection+'-collapsed.txt')
			desiredDirectionStartingFile = os.path.join(intermediatefiledir,str(NtoUse)+'gram-'+direction+'-combined.txt')
			reverseGoogleFile(startingModel, desiredDirectionStartingFile, colnames)

			#sort it
			sortedFile = os.path.join(intermediatefiledir,str(n)+'gram-'+direction+'-sorted.txt')
			sortNgramFile(desiredDirectionStartingFile, sortedFile)

			#marginalize it							
			marginalizedfile = os.path.join(intermediatefiledir,str(n)+'gram-'+direction+'-marginalized.txt')
			marginalizeNgramFile(sortedFile,marginalizedfile, n, 'alphabetic', colnames, sum_columns)
						
			collapsedFile = os.path.join(intermediatefiledir,str(n)+'gram-'+direction+'-collapsed.txt')

			os.system('cp '+marginalizedfile+' '+collapsedFile)
			return(collapsedFile)
		else:
			print 'No appropriate models found, proceeding to cleaning the source trigrams.'
			return(None)

def reverseGoogleFile(inputfile, outputfile, colnames):
	'''Reverse the order of the ngram in a Google-formatted ngram file. Note that this is a different procedure than rearranging the ngram files that are output by AutoCorpus'''
	print('Reversing existing model')		
	iff = io.open(inputfile, 'r', encoding='utf-8')
	off = io.open(outputfile, 'w', encoding='utf-8')		
	contents = {}
	for l in iff:
		line = l.split('\t')	
		if len(line) == len(colnames): #this removes any empty lines that are produced by the cleaning process
			
			for i in range(len(colnames)):
				contents[colnames[i]] = line[i]
				
			# reverse the ngram	
			contents['ngram'] = ' '.join(contents['ngram'].split(' ')[::-1])
			off.write('\t'.join([contents[x] for x in colnames]))
	iff.close()
	off.close()
	print('Done!')			