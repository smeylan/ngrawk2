#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing
import os
import glob
import time
import io
import math
from subprocess import call
import pdb
import copy
from Queue import Empty

class cgWorker0(multiprocessing.Process):
    '''single-thread worker for parallelized cleanGoogle function'''  
    def __init__(self,queue,myList):
        super(cgWorker0, self).__init__()
        self.queue = queue
        self.myList = myList
        
    def run(self):    	
        for job in iter(self.queue.get, None): # Call until the sentinel None is returned
        	try:
        		cleanGoogleFile(job['inputfile'], job['outputfile'], job['collapseyears'],job['filetype'], job['order'], job['colnames'])        
        	except ValueError:
        		print 'Problems encountered in cleaning '+job['inputfile']
			self.myList.append(job['inputfile'])

def cgWorker3(arg_dict):
	return(cleanGoogleFile(**arg_dict))	
	
def cleanGoogleDirectory0(inputdir, outputdir, collapseyears, order, colnames, numThreads=20):
	'''Parallelized, load-balanced execution of cleanGoogle, starting with the largest files'''
	start_time =  time.time()

	# Put the manager in charge of how the processes access the list
	mgr = multiprocessing.Manager()
	myList = mgr.list() 
    
	# FIFO Queue for multiprocessing
	q = multiprocessing.Queue()
    
	# Start and keep track of processes
	procs = []
	for i in range(numThreads):
		p = cgWorker0( q,myList )
		procs.append(p)
		p.start()
	              
	files = glob.glob(os.path.join(inputdir,'*.gz')) + glob.glob(os.path.join(inputdir,'*.zip')) 
	if len(files) > 0:
		print('File type is gz')	
		filetype = 'gz'
	else:
		files = glob.glob(os.path.join(inputdir,'*.bz2'))
		if len(files) > 0:
			print('File type is bz2')	
			filetype = 'bz2'	
		else:
			raise ValueError('No files found')		
		
	filesizes = [(x, os.stat(x).st_size) for x in files]
	filesizes.sort(key=lambda tup: tup[1], reverse=True)
	
	extension = '.yc' if collapseyears else '.output'
	# Add data, in the form of a dictionary to the queue for our processeses to grab    
	[q.put({"inputfile": file[0], "outputfile": os.path.join(outputdir, os.path.splitext(os.path.basename(file[0]))[0]+extension),"collapseyears": collapseyears, 'filetype':filetype, 'order':order, 'colnames':colnames}) for file in filesizes] 
      
	#append none to kill the workers with poison pills		
	for i in range(numThreads):
		q.put(None) #24 sentinels to kill 24 workers
        
	# Ensure all processes have finished and will be terminated by the OS
	for p in procs:
		p.join()     
        
	for item in myList:
		print(item)

	print('Done! processed '+str(len(myList))+' files; elapsed time is '+str(round(time.time()-start_time /  60., 5))+' minutes') 	


def cleanGoogleDirectory3(inputdir, outputdir, collapseyears, order, colnames, numThreads=24):
	'''Parallelized, load-balanced execution of cleanGoogle, starting with the largest files'''
	# https://stackoverflow.com/questions/20887555/dead-simple-example-of-using-multiprocessing-queue-pool-and-locking
	start_time =  time.time()

	# Put the manager in charge of how the processes access the list
	pool = multiprocessing.Pool(numThreads)

	# FIFO Queue for multiprocessing	              
	files = glob.glob(os.path.join(inputdir,'*.gz')) + glob.glob(os.path.join(inputdir,'*.zip')) 
	if len(files) > 0:
		print('File type is gz')	
		filetype = 'gz'
	else:
		files = glob.glob(os.path.join(inputdir,'*.bz2'))
		if len(files) > 0:
			print('File type is bz2')	
			filetype = 'bz2'	
		else:
			raise ValueError('No files found')		
		
	filesizes = [(x, os.stat(x).st_size) for x in files]
	filesizes.sort(key=lambda tup: tup[1], reverse=True)
	
	extension = '.yc' if collapseyears else '.output'
	
	# Add data, in the form of a dictionary to the queue for our processeses to grab    	
	jobs = [{"inputfile": file[0], "outputfile": os.path.join(outputdir, os.path.splitext(os.path.basename(file[0]))[0]+extension),"collapseyears": collapseyears, 'filetype':filetype, 'order':order, 'colnames':colnames} for file in filesizes] 	

	results = pool.map(cgWorker3, jobs)
	

	print('Finished cleaning Google input files')

def cleanGoogleFile(inputfile, outputfile, collapseyears, filetype, order, colnames, fixPunc=True):
	'''Clean google trigram file. This is a highly streamlined version of process google that finds only non POS-tagged lines, with no punctuation, and makes them lowercase, using grep to find lines without punctuation (including _, which excludes lines with POS tags) and perl to lowercase the string, while maintaining the unicode encoding. If collapseyears is true, combine the year counts into a single record using collapseGoogleNgrams'''
	tempfile0 = outputfile+'_temp0'
	tempfile1 = outputfile+'_temp1'
	tempfile2 = outputfile+'_temp2'

	print('Clean Google File arguments')
	print(inputfile)
	print(outputfile)
	print(collapseyears)
	print(filetype)
	print(order)
	print(colnames)
	format_unzipper = {'gz':'zcat', 'csv.zip':'zcat','bz2':'bzcat'}

	cleanGoogleCommand = format_unzipper[filetype]+" "+inputfile+" | LC_ALL=C grep -v '[]_,.!\"#$%&()*+-/:;<>=@^{|}~[]' | tr A-Z a-z > "+tempfile0		
	call(cleanGoogleCommand, shell=True)

	if collapseyears:
		if fixPunc:		
			fixPunctuation(tempfile0, tempfile1, order, colnames, None, promoteYear=False)
			#os.remove(tempfile0)
			if os.stat(tempfile1).st_size > 0 :					
				collapseGoogleNgrams(tempfile1, outputfile, id_columns=['ngram'],sum_columns=['count'], colnames=colnames)
			else:
				#return(None)
				print('Temp file has no content; safe to remove.')
			#os.remove(tempfile1)
		else:
			if os.stat(tempfile).st_size > 0 :	
				collapseGoogleNgrams(tempfile, outputfile, id_columns=['ngram'],sum_columns=['count'], colnames=colnames) 	
			else:
				#return(None)
				print('Temp file has no content; safe to remove.')	
	else:	
		if fixPunc:
			fixPunctuation(tempfile0, tempfile1, order, colnames, dateTo25, promoteYear=True)			
			
			collapseGoogleNgrams(tempfile1, outputfile, id_columns=['year','ngram'], sum_columns=['count'], colnames=colnames)

			#os.remove(tempfile0)
		else:
			raise ValueError('Must choose an option that reduces temporal resolution, else sorting will take a very long time')	
			#os.system('mv '+tempfile0+' '+outputfile)			


	return(outputfile)	

def collapseGoogleNgrams(inputfile, outputfile, id_columns, sum_columns, colnames):	
	'''aggregate sum columns according to id_columns from a google-formatted ngram file'''	

	bufsize = 1000000
	print('Collapsing...')	
	iff = io.open(inputfile, 'r', encoding='utf-8')
	off = io.open(outputfile, 'w', encoding='utf-8')		
	firstLine ='\n' #handle any lines that are blank at the beginning of the text
	#need to confirm that there is anything in the file
	counter = 0
	while firstLine == u'\n' or firstLine == '':
		counter += 1
		firstLine = iff.readline()
		if counter > 100:
			iff.close()
			off.close()
			print('Nothing in the input file for cleaning')	
			return(None)

	line = firstLine.replace('\n','').split(u'\t')
	ncols = len(line)

	prev_line_dict = {}
	for i in range(len(colnames)):		
		prev_line_dict[colnames[i]] = line[i]
				
	rows =[]
	output_cols = id_columns + sum_columns

	for c,l in enumerate(iff):
		line = l.replace('\n','').split(u'\t')		
		if len(line) != ncols:
			print 'Mismatch in line length and ncols, line was '+l
			continue
		
		new_line_dict = {}
		for i in range(len(colnames)):		
			new_line_dict[colnames[i]] = line[i]

		if any([new_line_dict[id_column] != prev_line_dict[id_column] for id_column in id_columns]):
				
			#mismatch in an id column (ngram or year), so add to buffer
			rows.append(u'\t'.join([prev_line_dict[x] for x in output_cols]))

			#after appending row to the buffer, reset the storage with the current values
			prev_line_dict = new_line_dict
			continue
		
		else:
			for key in sum_columns:
				prev_line_dict[key] = str(int(prev_line_dict[key]) + int(new_line_dict[key]))

		if c % bufsize == 0:	
			print('Writing to file...')
			off.write(u'\n'.join(rows)+'\n')
			rows =[] 
	
	rows.append(u'\t'.join([prev_line_dict[x] for x in output_cols])) # catch the last record			

	off.write('\n'.join(rows)+'\n')	#catch any records since the last buffered write						 	
	iff.close()
	off.close()
	print('Finished collapsing years, output in file '+str(outputfile))	

def dateToDecade(year) :
	return(str(int(math.floor(int(year) / 10) * 10)))

def dateTo25(year): 
	return(str(int(math.floor(int(year) / 25) * 25)))

def fixPunctuation(inputfile, outputfile, order, colnames, downsampleDateFunction, promoteYear):
	'''remove symbols except apostrophes and replace right quotation mark with apostrophe'''
	bufsize = 10000000
	print('Fixing the punctuation, inputfile in file '+str(inputfile))
	iff = io.open(inputfile, 'r', encoding='utf-8')
	off = io.open(outputfile, 'w', encoding='utf-8')	
	firstLine =u'\n' #handle any lines that are blank at the beginning of the text
	#need to confirm that there is anything in the file
	counter = 0
	while firstLine == u'\n' or firstLine == '':
		counter += 1
		firstLine = iff.readline()
		if counter > 100:
			iff.close()
			off.close()
			print('Nothing in the input file for cleaning')	
			return(None)
	
	rows =[]
	line = firstLine.replace(u'\n',u'').split(u'\t')
	ncols = len(line)
		
	if len(colnames) != len(line):
		raise ValueError('Column labels do not match column names')
	
	contents = {}
	for i in range(len(colnames)):		
		contents[colnames[i]] = line[i]

	ngram_split = [x for x in contents['ngram'].replace(u'’',u"'").split(' ') if x != "'"]	
	contents['ngram'] = ' '.join(ngram_split)

	if downsampleDateFunction is not None:
		contents['year'] = downsampleDateFunction(contents['year'])	

	if promoteYear:		
		output_colnames = list(colnames)
		output_colnames.remove('year') 
		output_colnames = ['year'] + output_colnames
	else:
		output_colnames = copy.deepcopy(colnames)

	if len(ngram_split) == order:	
		rows.append(u'\t'.join([contents[x] for x in output_colnames]))

		
	for c,l in enumerate(iff):
		line = l.replace(u'\n','').split(u'\t')
		if len(line) != ncols:			
			print 'Mismatch in line length and ncols, line was '+' '.join(line)
			continue
		
		if len(output_colnames) != len(line):
			raise ValueError('Column labels do not match column names')
	
		contents = {}
		for i in range(len(colnames)):
			contents[colnames[i]] = line[i]

		if downsampleDateFunction is not None:
			contents['year'] = downsampleDateFunction(contents['year'])		

		ngram_split = [x for x in contents['ngram'].replace(u'’',u"'").split(u' ') if x not  in (u"'",u"''",u'"',u'""',u"?")]	
		contents['ngram'] = u' '.join(ngram_split)

		if len(ngram_split) == order:	
			rows.append(u'\t'.join([contents[x] for x in output_colnames]))
						
		if c % bufsize == 0:#refresh the buffer	
			off.write(u'\n'.join(rows)+u'\n')
			rows =[] 
		
	off.write(u'\n'.join(rows)+u'\n')	#catch any records since the last buffered write						 	
	iff.close()
	off.close()
	print('Finished fixing the punctuation, output in file '+str(outputfile))	
