import googleapiclient.discovery
import pymongo
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import time
from streamlit_option_menu import option_menu
import numpy as np
from googleapiclient.errors import HttpError
from pprint import pprint

api_key ='************************'
youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=api_key)

# This function is to Scrape the channel details with respect to channel id
def channel_details(channel_id):
  try:
      request_channel = youtube.channels().list(
              part="snippet,contentDetails,statistics",
              id= channel_id
          )
      response_channel = request_channel.execute()
      request_playlist = youtube.playlistItems().list(
            part="contentDetails",
            maxResults=50,
            playlistId= response_channel['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        )
      videoIdList = []
      while request_playlist:
          response_playlist = request_playlist.execute()
          for item in response_playlist['items']:
            videoIdList.append(item['contentDetails']['videoId'])
          request_playlist = youtube.playlistItems().list_next(request_playlist, response_playlist)
      videoRecords = [youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videoid
        ).execute() for videoid in videoIdList]
      commentRecords=[]
      for id in videoIdList:
        try:
            request_comments = youtube.commentThreads().list(
            part="snippet,replies",
            maxResults=10,
            videoId= id)
            comment_response = request_comments.execute()
            commentRecords.append(comment_response)
        except HttpError as e:
            error_detail = e.content.decode('utf-8')
            if 'commentsDisabled' in error_detail:
                print(f"Comments are disabled for video with ID: {id}")
            else:
                raise e
      channelData = {
        'Channel': 
        {
        'title': response_channel['items'][0]['snippet']['title'],
        'channel_id': response_channel['items'][0]['id'],
        'playlistId': response_channel['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'description': response_channel['items'][0]['snippet']['description'],
        'started': response_channel['items'][0]['snippet']['publishedAt'],
        'thumbnail': response_channel['items'][0]['snippet']['thumbnails']['medium']['url'],
        'subscriberCount': response_channel['items'][0]['statistics']['subscriberCount'],
        'videocount': response_channel['items'][0]['statistics']['videoCount'],
        'viewcount': response_channel['items'][0]['statistics']['viewCount'] } ,

        'Videos' : [
          {
            'videoid': video['items'][0]['id'],
            'videoname': video['items'][0]['snippet']['title'],
            'channeltitle': video['items'][0]['snippet']['channelTitle'],
            'title': video['items'][0]['snippet']['title'],
            'channel_id': video['items'][0]['snippet']['channelId'],
            'description': video['items'][0]['snippet']['description'],
            'tags': video['items'][0]['snippet'].get('tags', []),
            'publishedAt': video['items'][0]['snippet']['publishedAt'],
            'viewCount': video['items'][0]['statistics'].get('viewCount', 0),
            'likeCount': video['items'][0]['statistics'].get('likeCount', 0),
            'favoriteCount': video['items'][0]['statistics'].get('favoriteCount', 0),
            'commentCount': video['items'][0]['statistics'].get('commentCount', 0),
            'duration': video['items'][0]['contentDetails']['duration'],
            'definition': video['items'][0]['contentDetails']['definition'],
            'caption': video['items'][0]['contentDetails']['caption']
          } for video in videoRecords ],

        'comments': [
          {
            'commentid': item['id'],  
            'videoid': item['snippet']['topLevelComment']['snippet']['videoId'],
            'channel_id': item['snippet']['topLevelComment']['snippet']['channelId'],
            'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'published_date': item['snippet']['topLevelComment']['snippet']['publishedAt'],
            'text': item['snippet']['topLevelComment']['snippet'].get('textDisplay', ''),
            'likeCount': item['snippet']['topLevelComment']['snippet'].get('likeCount', 0),
            'replyCount': item['snippet'].get('totalReplyCount', 0)
        } for comment in commentRecords if comment.get('items') for item in comment['items'] ]
          }
      return channelData
  except Exception as e:
    print(f"An error occurred in channel detail extraction: {e}")
    return None
  
# Creating the StreamLit Page Configuration  
st.set_page_config(
    page_title="Youtube Data Harvesting",
    page_icon="▶️",
    layout="wide",
    initial_sidebar_state="expanded", )

# Creating the Sidebar
with st.sidebar:
    selected = option_menu("Main Menu", ["Home","To Add Channel Details","Frequently Asked Questions"], 
                icons=['house', 'plus', 'question mark'], menu_icon="cast", default_index=0)
    if selected == "Frequently Asked Questions":
        option = st.selectbox(
        'Please select a Question Number',
        ('Question 1', 'Question 2', 'Question 3','Question 4','Question 5','Question 6','Question 7','Question 8','Question 9','Question 10'),
        index=None )

# Creating the Home menu
if selected == 'Home':
    st.title(":red[YouTube] Data Harvesting and Warehousing 📂")
    st.subheader('',divider='red')
    st.write("""**This Project aims to develop a user-friendly Streamlit application that utilizes the Google API 
            to extract information on a YouTube channel, stores it in a MongoDB database, migrates it 
            to a SQL data warehouse, and enables users to search for channel details and join tables 
            to view data in the Streamlit app.**""")
    st.subheader(":red[Skills Takeaway From This Project]")
    st.text("> Python scripting,")
    st.text("> Data Collection,")
    st.text("> MongoDB,")
    st.text("> Streamlit,")
    st.text("> API integration,")
    st.text("> Data Management using MongoDB (Atlas), &")
    st.text("> SQL")
    st.subheader('',divider='red') 

# To Add Channel Details Menu
elif selected == 'To Add Channel Details':
    st.subheader(':red[Please enter the Channel ID]')
    channel_id = st.text_input(" ")
    if st.button('SCRAP & ADD') and channel_id:
        
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://localhost:27017")
        db = client["youtube_dataHarvest"]
        collection = db["channel_details"]
        try:
              # Checking if the channel id already exists in Mongo DB
              check_id = collection.find_one({"Channel.channel_id": channel_id})
              if check_id  is None:
                channelData = channel_details(channel_id)
                result = collection.insert_one(channelData)
                print("Inserted channel data with ID:", result.inserted_id)
                data = list(collection.find())

                # Creating the respective dataframes for Channel,Videos and Comments
                cdf = pd.DataFrame([channel['Channel'] for channel in data ] )
                vdf = pd.DataFrame([video for i in data for video in i['Videos'] ])
                cmdf = pd.DataFrame([comment for i in data for comment in i['comments']])
                cdf = cdf.astype({'subscriberCount': np.int64, 'videocount': np.int64, 'viewcount': np.int64})
                vdf = vdf.astype({'viewCount': np.int64, 'likeCount': np.int64, 'favoriteCount': np.int64, 'commentCount': np.int64})
                cmdf = cmdf.astype({'likeCount': np.int64, 'replyCount': np.int64})
                vdf = vdf.drop(columns=['tags'])
                vdf['publishedAt'] = pd.to_datetime(vdf['publishedAt'], format='%Y-%m-%dT%H:%M:%SZ', utc=True)
                vdf['duration'] = pd.to_timedelta(vdf['duration'])
                vdf['duration'] = vdf['duration'].astype(str)
                vdf['duration'] = [i[-1] for i in (vdf['duration'].str.split())]
                cmdf['published_date'] = pd.to_datetime(cmdf['published_date'], format='%Y-%m-%dT%H:%M:%SZ', utc=True)

                # Creating an Engine to connect to MySql Database using SQlAlchemy
                engine = create_engine('mysql+mysqlconnector://root:*********@localhost/youtubeData')

                # Convert DataFrame to MySQL table
                cdf.to_sql("channel_details", engine, index=False, if_exists='replace')
                vdf.to_sql("video_details", engine, index=False, if_exists='replace')
                cmdf.to_sql("comment_details", engine, index=False, if_exists='replace')

                # Commit the changes
                engine.dispose()
                st.success('Successfully uploaded the Channel details',icon='✔️')
              else:
                st.warning(f'Warning !!Data already exists for the channel ID: {channel_id}', icon="❗")
        except Exception as e:
          st.error("Error occurred while uploading channel information, Please check the channel ID")  

#Frequently Asked Questions Menu
elif selected == "Frequently Asked Questions":
   st.title('SQL :blue[Queries] :white_check_mark')
   # Creating an Engine to connect to MySql Database using SQlAlchemy
   engine = create_engine('mysql+mysqlconnector://root:7295*MAthew@localhost/youtubeData')
   if option == 'Question 1':
      st.markdown("## What are the names of all the videos and their corresponding channels?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select videoname,channeltitle from video_details'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 2':
      st.markdown("## Which channels have the most number of videos, and how many videos do they have?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select title,videocount as "Most videos" from channel_details order by videocount desc'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 3':
      st.markdown("## What are the top 10 most viewed videos and their respective channels?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select videoname,channeltitle, viewcount from video_details order by viewcount desc limit 10'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 4':
      st.markdown("## How many comments were made on each video, and what are their corresponding video names?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select videoname,commentCount from video_details order by commentCount desc'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 5':
      st.markdown("## Which videos have the highest number of likes, and what are their corresponding channel names?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select videoname,channeltitle,likeCount as "Highest Likes" from video_details order by likeCount desc'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 6':
      st.markdown("## What is the total number of likes for each video, and what are their corresponding video names?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select videoname,likeCount from video_details order by likeCount desc '
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 7':
      st.markdown("## What is the total number of views for each channel, and what are their corresponding channel names?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select title,viewcount from channel_details'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 8':
      st.markdown("## What are the names of all the channels that have published videos in the year 2022?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select distinct channeltitle from video_details where YEAR(publishedAt) = 2022'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 9':
      st.markdown("## What is the average duration of all videos in each channel, and what are their corresponding channel names?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select channeltitle,sec_to_time(avg(time_to_sec(duration))) as Average from video_details group by channeltitle'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
   if option == 'Question 10':
      st.markdown("## Which videos have the highest number of comments, and what are their corresponding channel names?")
      with st.spinner('Fetching information...'):
        time.sleep(2)
      query = 'select channeltitle,videoname,commentCount from video_details order by commentCount desc'
      sqdf = pd.read_sql_query(query, engine)
      st.dataframe(sqdf, hide_index=True)
      

   
   
         
 





