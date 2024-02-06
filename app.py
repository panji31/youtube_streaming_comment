# API client library
import googleapiclient.discovery
import time

# API information INIT
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "XXXXXXXXXXXXXXXXXXXX"
videoLiveID = "BBzdSRt-zSQ"
maxResultsStart = 10

#GET Chat ID
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = DEVELOPER_KEY)
r = youtube.videos().list(
    part='liveStreamingDetails,snippet',
    id=videoLiveID,
    fields='items(liveStreamingDetails(activeLiveChatId),snippet(title,liveBroadcastContent))'
).execute()
chatID = r['items'][0]['liveStreamingDetails']['activeLiveChatId']


# get 10 start live chat 
response = youtube.liveChatMessages().list(
    liveChatId=chatID,
    part="snippet,authorDetails",
    maxResults = maxResultsStart,
    fields="nextPageToken,items(snippet(publishedAt,displayMessage),authorDetails(channelId,displayName))"
).execute()

# # Dictionary where all the messages will be stored. When the retrieval ends, this object will
# # be transformed into a csv file.

data = {
    'authorChannelId':[],
    'authorChannelName':[],
    'messagePublishDate':[],
    'messageContent':[]
}

# # Storing first batch of messages
for item in response['items']:
    data['authorChannelId'].append(item['authorDetails']['channelId'])
    data['authorChannelName'].append(item['authorDetails']['displayName'])
    data['messagePublishDate'].append(item['snippet']['publishedAt'])
    data['messageContent'].append(item['snippet']['displayMessage'])



# # Permanent loop to retrieve messages
while True:
    # In this point is common to lose the whole progress if the quota is exceeded or if the program 
    # execution stops due our intervention or if the live video ends. This try/except block prevent
    # this scenarios and store all the gathered data
    try:

        # Next Token to retrieve non-duplicated messages is obtained from the previous request
        nextToken = response['nextPageToken']

        # Storing each meassage retrieved in the previous request and, if "verbose = '1'", also they 
        # will be printed
        for item in response['items']:
            data['authorChannelId'].append(item['authorDetails']['channelId'])
            data['authorChannelName'].append(item['authorDetails']['displayName'])
            data['messagePublishDate'].append(item['snippet']['publishedAt'])
            data['messageContent'].append(item['snippet']['displayMessage'])

            
            print(f"{item['snippet']['publishedAt']} [{item['authorDetails']['displayName']}]: " + \
                    f"{item['snippet']['displayMessage']}")
            print("--------------------------------\n")
        
        # There is a mandatory wait time between requests, half second approx. But since we can retrieve
        # up to 2000 messages in just one query, then we will wait 10 seconds between requests 
        # in order to save some quota usage
        time.sleep(10)

        response = youtube.liveChatMessages().list(
            liveChatId=chatID,
            part="snippet,authorDetails",
            maxResults = 10,
            fields='nextPageToken, items(snippet(publishedAt, displayMessage),' + \
                    'authorDetails(channelId, displayName))',
            pageToken=nextToken
        ).execute()
    except:
        end = time.time()

        f = open(f"youtube_live_chat.csv", 'w')
        f.write(",".join(list(data.keys())))
        f.write("\n")

        i = 0

        print(data)

        while i < len(data['authorChannelId']):
            f.write(
                f"{data['authorChannelId'][i]},"+
                f"{data['authorChannelName'][i]},"+
                f"{data['messagePublishDate'][i]},"+
                f"{data['messageContent'][i]}\n"
            )
            i += 1

        break