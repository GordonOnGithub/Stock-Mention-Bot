import base64
import requests
import json
import re
import time
import csv
import os
from twython import Twython

twitterAppKey = os.environ.get('twitterAppKey', '')
twitterAppSecret = os.environ.get('twitterAppSecret', '')
twitterOAuthToken = os.environ.get('twitterOAuthToken', '')
twitterOAuthTokenSecret = os.environ.get('twitterOAuthTokenSecret', '')

appId = os.environ.get('appId', '')
appSecret = os.environ.get('appSecret', '')
username = os.environ.get('username', '')
password = os.environ.get('password', '')
auth = requests.auth.HTTPBasicAuth(appId, appSecret)

auth_base_url = 'https://www.reddit.com/'
api_base_url= 'https://oauth.reddit.com'
userAgent = 'Subreddits Trends by GordonTheDeveloper'
subReddits = ['wallstreetbets','StockMarket','stocks','investing','options']

stock_list_endpoint = 'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey='
alphaVantageApiKey =  os.environ.get('alphaVantageApiKey', '')

postType = 'top'
postLimit = 100
mentionCountThreshold = 10
upvoteRatioThreshold = 0.6
scoreThreshold = 10
commentScoreThreshold = 1
commentSearchDepth = 6
debug = False

def reddit_trends_analysis(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    #print(pubsub_message)
    
    commonEnglishWords = []
    symbolMentionMap = {}
    symbolsSet = set()

    def addSymbol(symbol):
        symbolsSet.add(symbol.upper())

    def readCommonWordsFromFile(fileName):
        words = []
        with open(fileName+'.csv') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for row in spamreader:
                for word in row:
                    words.append(word)
        return words

    def extractStockSymbolFromCSV(fileName):
        symbols =set()
        with open(fileName+'.csv') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for row in spamreader:
                if len(row) > 0:
                    symbol = row[0]
                    symbol = symbol.split('-')[0]
                    symbols.add(symbol)
        #print(symbols)
        return symbols

    def getStockList(apiKey):
        symbols =set()   
        url = stock_list_endpoint + apiKey
        try:
            response = requests.get(url)
            if response.status_code == 200:
                spamreader = csv.reader(response.text.splitlines(), delimiter=',')
                
                skippedFirstRow = False
                for row in spamreader:
                    if not skippedFirstRow:
                        skippedFirstRow = True
                        #skip the first row because it is the column titles
                        continue
                        
                    if len(row) > 0:
                        symbol = row[0]
                        symbol = symbol.split('-')[0]
                        symbols.add(symbol)
            else:
                print(response.text)    
        except Exception as e:
            print(str(e))
        
        print('no. of symbols from Alpha Vantage:' + str(len(symbols)) + '\n')

        return symbols

    def filterWordSetForStockSearch(unfilteredArray):
        filteredWordSet = set()
        if len(unfilteredArray) < 2:
            return filteredWordSet #ignore content with one word only
        
        for word in unfilteredArray:
            trimmedWord = word.replace('$','')#trim $ symbol
            
            if len(trimmedWord) > 5 or len(word) < 2:
                continue #ignore long words that cannot be stock symbol, only single letter with $ sign is considered a stock symbol, e.g. $U
            if any(chr.isdigit() for chr in trimmedWord):
                continue #ignore numbers
            if "'" in trimmedWord:
                continue
            if trimmedWord.lower() != trimmedWord and trimmedWord.upper() != trimmedWord:
                continue #only check words that all upper or lower case
            if trimmedWord in filteredWordSet:
                continue
            if word[0] != '$' and trimmedWord in commonEnglishWords:
                continue
            filteredWordSet.add(trimmedWord.upper())
        return filteredWordSet
            

    def convertContentToWordArray(content):
        wordArray = re.findall(r"[\w'$]+", content)
        return wordArray

    def checkMentionsFromContentSet(author, contentSet, keywordsSet):
        if len(contentSet) == 0:
            return
        
        #print( author +": " + ascii(' / '.join(contentSet)))
        
        for word in keywordsSet:
            if word in contentSet:
                if word not in symbolMentionMap:
                    symbolMentionMap[word] = set()
                mentionAuthorSet = symbolMentionMap[word]
                mentionAuthorSet.add(author)

    def getAccessToken(username, password):
        try:
            response = requests.post(auth_base_url + 'api/v1/access_token',
                            data={'grant_type': 'password', 'username': username, 'password': password},
                            headers={'User-Agent' : userAgent},
                    auth=auth)
            if response.status_code == 200:
                responseJsonObject = response.json()
                return responseJsonObject['token_type'] + " " + responseJsonObject['access_token']
            print('response.status_code: ' + str(response.status_code))
            return None
        except Exception as e:
            print(str(e))
            return None
        
    def fixJsonString(brokenJsonString):
        fixedJsonString = brokenJsonString.replace(":\"\"",":\" \"")
        fixedJsonString = fixedJsonString.replace("\\\"","\\\" ")
        fixedJsonString = fixedJsonString.replace("\"\"","")
        fixedJsonString = fixedJsonString.replace(": ,", ":\" \",")
        return fixedJsonString

    def parseComment(commentData, level):
        if 'body' not in commentData:
            return
        commentBody = commentData['body']
        author = commentData['author']
        score = commentData['score']
        if author == 'AutoModerator':
            return
        if score >= commentScoreThreshold:
            filteredWords = filterWordSetForStockSearch(convertContentToWordArray(commentBody))
            checkMentionsFromContentSet(author,filteredWords,symbolsSet)
        
        if debug:
            indent = ">"
            i = 0
            while i < level:
                indent += ">"
                i+=1
            print(indent + '('+ author + ')' + ascii(' / '.join(filteredWords)))

        if 'replies' not in commentData:
            return
        replies = commentData['replies']
        if isinstance(replies, str) :
            return
        repliesData = replies['data']
        subCommentsArray = repliesData['children']
        for subComment in subCommentsArray:
            subCommentData =  subComment['data']
            parseComment(subCommentData, level + 1)

    def getComments(permalink, token, depth, sort):
        payload = {'showmedia':False, 'depth':depth, 'sort':sort}
        url = api_base_url + permalink
        if debug:
            print(url)
        try:
            response = requests.get(url,
                            params=payload,
                            headers={'Authorization' : token, 'User-Agent' : userAgent})
            if response.status_code == 200:
                jsonString = fixJsonString(response.text)

                responseJsonObject = json.loads(jsonString)
                for listing in responseJsonObject:
                    commentsArray = listing['data']['children']
                    for comment in commentsArray:
                        commentData =  comment['data']
                        parseComment(commentData, 0)
        
            else:
                print(response.text)
        except Exception as e:
            print(str(e))
            
    def getCount(data):
        return data['count']

    def getSubRedditPost(subRedditName,token,type, limit):
        payload = {'g':'GLOBAL','limit':limit}
        if type == 'top':
            payload['t'] = 'day'
        
        url = api_base_url + '/r/' + subRedditName + '/' + type
        try:
            response = requests.get(url,
                            params=payload,
                            headers={'Authorization' : token, 'User-Agent' : userAgent})
            if response.status_code == 200:
                jsonString = fixJsonString(response.text)
                
                responseJsonObject = json.loads(jsonString)
                postsArray = responseJsonObject['data']['children']
                call = True
                for post in postsArray:
                    data = post['data']
                    title = data['title']
                    content = data['selftext']
                    author = data['author']
                    upvoteRatio = data['upvote_ratio']
                    score = data['score']
                    if author == 'AutoModerator':
                        continue
                    if upvoteRatio < upvoteRatioThreshold or score < scoreThreshold:
                        continue

                    filteredWords = filterWordSetForStockSearch(convertContentToWordArray(title + " " + content))
                    checkMentionsFromContentSet(author,filteredWords,symbolsSet)
                    if debug:
                        print( '* ' + '('+ author + ')' + ascii(' / '.join(filteredWords)))
                    postUrl = data['permalink']
                    getComments(postUrl, token, commentSearchDepth, 'top')
                    time.sleep(1)
                    
                
            else:
                print(response.text)    
        except Exception as e:
            print(str(e))

    def saveResults(minimum):
        result = []
        for key in symbolMentionMap.keys():
            mentionSet = symbolMentionMap[key]
            result.append({'symbol':key, 'count':len(mentionSet)})
        
        result.sort(key=getCount)
        result.reverse()
        secondsSinceEpoch = round(time.time())

        with open(str(secondsSinceEpoch)+'.csv', mode='w', newline='') as resultFile:
            resultWriter = csv.writer(resultFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for data in result:
                count = data['count']
                if count > minimum:
                    resultWriter.writerow([data['symbol'], str(count)])
                    print(data['symbol'] + " : " + str(count))
                else:
                    break

    def printResults(minimum):
        result = []
        for key in symbolMentionMap.keys():
            mentionSet = symbolMentionMap[key]
            result.append({'symbol':key, 'count':len(mentionSet)})
        
        result.sort(key=getCount)
        result.reverse()

        for data in result:
            count = data['count']
            if count > minimum:
                print(data['symbol'] + " : " + str(count))
                time.sleep(0.1)
            else:
                break

    def tweetResults(numberOfSymbols):
        result = []
        for key in symbolMentionMap.keys():
            mentionSet = symbolMentionMap[key]
            result.append({'symbol':key, 'count':len(mentionSet)})
            
        if len(result) >= numberOfSymbols:
            result.sort(key=getCount)
            result.reverse()

            tweet = str(numberOfSymbols) + ' most mentioned US stocks on Reddit in past 24 hours:\n'
            v = 1
            for data in result:
                count = data['count']
                print(data['symbol'] + " : " + str(count))
                tweet += str(v) + '. $' + data['symbol'] + " : " + str(count) + '\n'
                v += 1
                if v > numberOfSymbols:
                    break
            tweet += "#stock #trading #usstock #stockmarket"
            postTweet(tweet)

    def postTweet(content):
        if len(content) <= 256:
            twitter = Twython(twitterAppKey, twitterAppSecret, twitterOAuthToken, twitterOAuthTokenSecret)
            twitter.update_status(status=content)
        else:
            print('content is too long to tweet: ' + str(len(content)))


    #execution begin 
    beginTimestamp = time.time()            
    print('Stock trend analysis begin: ' + str(beginTimestamp))
    commonEnglishWords = readCommonWordsFromFile('commonWords')
   
    symbolsSet = getStockList(alphaVantageApiKey)
    if len(symbolsSet) == 0:
        print('Warning: failed to get updated stock list, fallback to older version\n')
        symbolsSet = extractStockSymbolFromCSV('listing_status')

    token = getAccessToken(username, password)
    if token != None:
        for sub in subReddits:
            print('retriving ' + postType + ' posts data from r/' + sub + ', limit: ' + str(postLimit))
            getSubRedditPost(sub, token, postType, postLimit)
        
        #printResults(mentionCountThreshold)
        tweetResults(10)
    else:
        print('failed to get token')  
    endTimestamp = time.time()            
    print('Stock trend analysis completed: ' + str(endTimestamp) + ', time taken: ' + str(endTimestamp - beginTimestamp))
