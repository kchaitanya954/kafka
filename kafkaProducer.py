from kafka import KafkaProducer
from apiclient.discovery import build
from datetime import datetime, timedelta
import time
import json

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

#Set up YouTube credentials
DEVELOPER_KEY = 'key'
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,developerKey=DEVELOPER_KEY)

# -------------Build YouTube Search------------#
def youtubeSearch(query, max_results=50, order="date"):

 items = []


 search_response = youtube.search().list(
   q=query,
   type="video",
   pageToken=None,
   order=order,
   part="id,snippet",
   maxResults=max_results).execute()


 items = search_response['items']



 return items

def storeResults(response):
 # create variables to store your values
 viewCount = []
 likeCount = []

 for search_result in response:
  if search_result["id"]["kind"] == "youtube#video":

   # then collect stats on each video using videoId
   stats = youtube.videos().list(
    part='statistics, snippet',
    id=search_result['id']['videoId']).execute()

   # Not every video has likes/dislikes enabled so they won't appear in JSON response
   try:
    viewCount.append(int(stats['items'][0]['statistics']['viewCount']))
   except:

    # Appends "Not Available" to keep dictionary values aligned
    viewCount.append(None)

   try:
    likeCount.append(stats['items'][0]['statistics']['likeCount'])
   except:

    # Appends "Not Available" to keep dictionary values aligned
    likeCount.append(None)



 # Break out of for-loop and if statement and store lists of values in dictionary
 youtube_dict = { 'viewCount': sum(viewCount), 'likeCount': likeCount}

 return str(sum(viewCount))

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

q='IPL'
#Run YouTube Search
while True:
 response = youtubeSearch(q)
 results =storeResults(response)
 producer.send('testPK', str.encode(results))
 time.sleep(10)
