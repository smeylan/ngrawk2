def downloadCorpus(language, order, inputdir, release):
	import httplib2
	from bs4 import BeautifulSoup, SoupStrainer	
	import urllib
	from datetime import datetime

	language_input = language
	corpora_dir = inputdir
	old_cwd = os.getcwd()
	os.chdir(corpora_dir)
	start_time = datetime.now()
	http = httplib2.Http()
	status, response = http.request('http://storage.googleapis.com/books/ngrams/books/datasetsv2.html')
	for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
		if link.has_key('href'):
			url = link['href']
			# IF we match what we want:
			if re.search(order+"gram.+"+release, url):
				# Decode this
				m = re.search(r"googlebooks-([\w\-]+)-(\d+)gram.+"+release,url)
				language, n = m.groups(None)
				# Only download some language
				#set(["eng-us-all", "fre-all", "ger-all", "heb-all", "ita-all", "rus-all", "spa-all", "chi-sim" ])
				if language != language_input: continue
				filename = re.split(r"/", url)[-1] # last item on filename split
				# Make the directory if it does not exist
				if not os.path.exists(language):       os.mkdir(language)
				if not os.path.exists(language+"/"+n): os.mkdir(language+"/"+n)
				if not os.path.exists(language+"/"+n+"/"+filename):
					print "# Downloading %s to %s" % (url, language+"/"+n+"/"+filename)
					urllib.urlretrieve(url, language+"/"+n+"/"+filename )
				else:
					print('File already exists')	
				print "opening url:", url
				site = urllib.urlopen(url)
				meta = site.info()
				print "Content-Length:", meta.getheaders("Content-Length")[0]
				if(os.path.getsize(language+"/"+n+"/"+filename)!= int(meta.getheaders("Content-Length")[0])):
					print("error: "+filename)
				sys.stdout.flush()
	os.chdir(old_cwd)
	print("It took " + str(datetime.now() - start_time))

def downloadCorpusWrapper(corpusSpecification):
	downloadCorpus(corpusSpecification['language'], corpusSpecification['order'],corpusSpecification['inputdir'])

def validateCorpus(corpusSpecification):
	import httplib2
	from bs4 import BeautifulSoup, SoupStrainer	
	import urllib
	from datetime import datetime

	language_input = corpusSpecification['language']
	order = corpusSpecification['order']
	corpora_dir = os.path.join(corpusSpecification['inputdir'], corpusSpecification['corpus'])
	old_cwd = os.getcwd()
	os.chdir(corpora_dir)
	start_time = datetime.now()
	http = httplib2.Http()
	status, response = http.request('http://storage.googleapis.com/books/ngrams/books/datasetsv2.html')
	for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
		if link.has_key('href'):
			url = link['href']
			# IF we match what we want:
			if re.search(order+"gram.+20120701", url):
				# Decode this
				m = re.search(r"googlebooks-([\w\-]+)-(\d+)gram.+",url)
				language, n = m.groups(None)
				# Only download some language
				#set(["eng-us-all", "fre-all", "ger-all", "heb-all", "ita-all", "rus-all", "spa-all", "chi-sim" ])
				if language != language_input: continue
				filename = re.split(r"/", url)[-1] # last item on filename split
				# Make the directory if it does not exist
				if not os.path.exists(language): print("no directory for " + language)
				if not os.path.exists(language+"/"+n): print("no directory for "+language+"/"+n)
				if not os.path.exists(language+"/"+n+"/"+filename): 
					print("no file: "+language+"/"+n+"/"+filename)
					print "# Downloading %s to %s" % (url, language+"/"+n+"/"+filename)
					#urllib.urlretrieve(url, language+"/"+n+"/"+filename )
				site = urllib.urlopen(url)
				meta = site.info()
				print(meta.getheaders("Content-Length")[0])
				if(os.path.getsize(language+"/"+n+"/"+filename)!= int(meta.getheaders("Content-Length")[0])):
					print("error(wrong file size): "+filename)
					print "# Downloading %s to %s" % (url, language+"/"+n+"/"+filename)
					#urllib.urlretrieve(url, language+"/"+n+"/"+filename )
					#gunzip -t # 
					#pigz#
					#gzrecover #
				sys.stdout.flush()
	os.chdir(old_cwd)