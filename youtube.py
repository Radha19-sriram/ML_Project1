#import necessarry files
from googleapiclient.discovery import build
import pymongo 
import mysql.connector
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import re

#Api key connection
def Api_connect():

    Api_Id = "AIzaSyDrAYaqVvWH28lUrHkjSKz1BoT1C4L0va4"
    
    Api_service_name = "youtube"
    Api_version = "v3"
   
    youtube= build(Api_service_name, Api_version, developerKey=Api_Id)
    return youtube

youtube= Api_connect()

# get channel information
def get_channel_info(channel_id):
    request= youtube.channels().list(   
                    part= "snippet, ContentDetails, statistics",
                    id= channel_id
    )
    response=request.execute()

    for i in response['items']:
        data= dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i['id'],
                Subscriber=i["statistics"]['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                Channel_description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
        return data

#10 youtube channels and their channel_id

#SmarterEveryDay            ===  "UC6107grRI4m0o2-emgoDnAA"
# Captain Disillusion       ===  "UCEOXxzW2vU0P-0THehuIIeg"   
# Wendover Productions      ===  "UC9RM-iSvTu1uPJb8X5yp3EQ"   
# Real Engineering          ===  "UCR1IuLEqb6UEA_zQ81kwXfg"    
# Accented Cinema           ===  "UCH_VqR5rFFhgjZmM31xA3Ag"    
# Bald and Bankrupt         ===  "UCxDZs_ltFFvn0FDHT6kmoXA"   
# Kento Bento               ===  "UCLOwKVD0bYHxaDZxXkK4piw"
# Travel Tales Unfolded     ===  "UCtnDvv22ifV7xDLdhjB74yA"
# Tech Toy Tinker           ===  "UC6isczC9ShzPqnXTcEbVnTg"  
# Science In Seconds        ===  "UCch_GZx52D8TVvzvNgir6lw" 

    
#get video id's
def get_video_ids(channel_id):
    video_ids=[]
    response1=youtube.channels().list(id=channel_id,
                    part='contentDetails').execute()
    Playlist_Id=response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:

        response1=youtube.playlistItems().list(
                        part='snippet',
                        playlistId=Playlist_Id,
                        maxResults=50,
                        pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
                                

#get video information
def get_video_info(video_ids): 
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part='snippet, ContentDetails, statistics',
            id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favourite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data


#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50         
            )
            response=request.execute()

            for item in response['items']:
                data =dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment'][videoId],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass
    return Comment_data

            
#get_playlist_details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet, contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    Published_At=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount']) 
            All_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


#upload to mongodb
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details, "playlist_information":pl_details,
                      "video_information":vi_details, "comment_information":com_details})
    return "upload completed successfully"

    
# MYSQL---table creation for channels, playlists, videos, comments
#channel table
def channels_table():
    
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="test",
    database="Youtube_data",
    port="3306",
    auth_plugin='mysql_native_password',
    charset='utf8mb4'  # Set the character set for the connection
    )

    cursor = mydb.cursor()

    drop_query = """DROP TABLE IF EXISTS channels """
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """CREATE TABLE IF NOT EXISTS channels(
                    Channel_Name varchar(100),
                    Channel_Id varchar(80) primary key,
                    Subscriber bigint,
                    Views bigint,
                    Total_videos int,
                    Channel_description text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    Playlist_Id varchar(80)
                )"""

    cursor.execute(create_query)
    mydb.commit()

    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = """INSERT INTO channels(
                            Channel_Name,
                            Channel_Id,
                            Subscriber,
                            Views,
                            Total_videos,
                            Channel_description,
                            Playlist_Id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                        
        values = (
            row["Channel_Name"],
            row["Channel_Id"],
            row["Subscriber"],
            row["Views"],
            row["Total_videos"],
            row["Channel_description"],
            row["Playlist_Id"]
        )

        cursor.execute(insert_query, values)
        mydb.commit()

#playlist table

def playlist_table():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="test",
    database="Youtube_data",
    port="3306",
    auth_plugin='mysql_native_password',
    charset='utf8mb4'
    )

    cursor = mydb.cursor()

    drop_query = """DROP TABLE IF EXISTS playlists """
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """CREATE TABLE IF NOT EXISTS playlists(
                        Playlist_Id varchar(100) primary key,
                        Title varchar(100) CHARACTER SET utf8mb4,
                        Channel_Id varchar(100),
                        Channel_Name varchar(100),
                        Published_At timestamp,
                        Video_Count int
                    )"""

    cursor.execute(create_query)
    mydb.commit()

    pl_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]

    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])

    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        published_at = datetime.strptime(row["Published_At"], '%Y-%m-%dT%H:%M:%SZ')
        
        insert_query = """INSERT INTO playlists(
                            Playlist_Id,
                            Title,
                            Channel_Id,
                            Channel_Name,
                            Published_At,
                            Video_Count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)"""

        values = (
            row["Playlist_Id"],
            row["Title"],
            row["Channel_Id"],
            row["Channel_Name"],
            published_at,
            row["Video_Count"]
        )

        cursor.execute(insert_query, values)
        mydb.commit()

#video table

def videos_table():

    mydb = mysql.connector.connect(host="localhost",
                        user="root",
                        password="test",
                        database="Youtube_data",
                        port="3306",
                        auth_plugin='mysql_native_password',
                        charset='utf8mb4' )
                        
    cursor = mydb.cursor()
    drop_query="""DROP TABLE IF EXISTS videos"""
    cursor.execute(drop_query)
    mydb.commit()


    create_query="""CREATE TABLE IF NOT EXISTS videos(Channel_Name varchar(100),
                                                    channel_Id varchar(100),
                                                    video_Id varchar(50) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration TIME,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favourite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50))"""

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({}, {"_id":0, "video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        # Convert Duration from ISO 8601 format to MySQL TIME format
        duration_iso = row["Duration"]
        duration_components = re.findall(r'(\d+)([HMST])', duration_iso)

        duration_dict = {'H': 0, 'M': 0, 'S': 0}

        for value, unit in duration_components:
            duration_dict[unit] = int(value)

        duration = str(timedelta(hours=duration_dict['H'], minutes=duration_dict['M'], seconds=duration_dict['S']))
        
        # Convert Published_Date to MySQL-compatible datetime format
        published_date = datetime.strptime(row["Published_Date"], "%Y-%m-%dT%H:%M:%SZ")

        insert_query = """INSERT INTO videos(Channel_Name,
                                            channel_Id,
                                            video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favourite_Count,
                                            Definition,
                                            Caption_Status)
                                                
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        # Convert Likes to a bigint (assuming it's a numeric type in DataFrame)
        likes = int(row["Likes"])

        # Convert values to a tuple of native Python types
        values = (
            str(row["Channel_Name"]),
            str(row["channel_Id"]),
            str(row["video_Id"]),
            str(row["Title"]),
            str(row["Tags"]),
            str(row["Thumbnail"]),
            str(row["Description"]),
            published_date.strftime("%Y-%m-%d %H:%M:%S"),  # Format to MySQL datetime
            duration,
            int(row["Views"]),
            likes,
            int(row["Comments"]),
            int(row["Favourite_Count"]),
            str(row["Definition"]),
            str(row["Caption_Status"])
        )

        cursor.execute(insert_query, values)
        mydb.commit()

#comments table


def comments_table():
    mydb = mysql.connector.connect(host="localhost",
                        user="root",
                        password="test",
                        database="Youtube_data",
                        port="3306",
                        auth_plugin='mysql_native_password',
                        charset='utf8mb4')
                        
    cursor = mydb.cursor()
    drop_query="""DROP TABLE IF EXISTS comments """
    cursor.execute(drop_query)
    mydb.commit()


    create_query="""CREATE TABLE IF NOT EXISTS comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp)"""

    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({}, {"_id":0, "comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():   

            Comment_Published = datetime.strptime(row["Comment_Published"], '%Y-%m-%dT%H:%M:%SZ')
            
            # Check if Comment_Id is not empty
            if row["Comment_Id"]:
                
                insert_query="""INSERT INTO comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published)
                                                
                                                    values(%s, %s, %s, %s, %s)"""      


                values = (row["Comment_Id"],
                    row["Video_Id"],
                    row["Comment_Text"],
                    row["Comment_Author"],
                    Comment_Published)
                cursor.execute(insert_query,values)
                mydb.commit() 


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "tables created successfully"

#display of tables

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({}, {"_id":0, "channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({}, {"_id":0, "playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1


def show_video_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({}, {"_id":0, "video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)
    
    return df2


def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({}, {"_id":0, "comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3=st.dataframe(com_list)

    return df3


#streamlit part

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management Using MongoDB and SQL")

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({}, {"_id":0, "channel_information":1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])

    if channel_id in ch_ids:
        st.success("The details of the given channel id is already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)

show_table = st.radio("Select The Table", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))
if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comments_table()

#handling queries

# SQL connection
mydb = mysql.connector.connect(host="localhost",
                user="root",
                password="test",
                database="Youtube_data",
                port="3306",
                auth_plugin='mysql_native_password')
                
cursor = mydb.cursor()

question=st.selectbox("Choose your question", ("1. What are the names of all the videos and their corresponding channels?",
                      "2.  Which channels have the most number of videos, and how many videos do they have",
                      "3.  What are the top 10 most viewed videos and their respective channels?",
                      "4.  How many comments were made on each video, and what are their corresponding video names?",
                      "5.  Which videos have the highest number of likes, and what are their corresponding channel names?",
                      "6.  What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                      "7.  What is the total number of views for each channel, and what are their corresponding channel names?",
                      "8.  What are the names of all the channels that have published videos in the year 2022?",
                      "9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                      "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1="""select title as videos,channel_name as channelname from videos"""
    cursor.execute(query1)
    
    result1=cursor.fetchall()
    df1=pd.DataFrame(result1, columns=["video title",'channel name'])
    st.write(df1)

elif question=="2.  Which channels have the most number of videos, and how many videos do they have":
    query2="""select channel_name as channelname,total_videos as no_videos from channels
              order by total_videos desc"""
    cursor.execute(query2)
    
    result2=cursor.fetchall()
    df2=pd.DataFrame(result2, columns=["channel name",'No of videos'])
    st.write(df2)

elif question=="3.  What are the top 10 most viewed videos and their respective channels?":
    query3="""select views as views,channel_name as channelname,title as videotitle from videos 
              where views is not null order by views desc limit 10"""
    cursor.execute(query3)
    
    result3=cursor.fetchall()
    df3=pd.DataFrame(result3, columns=["views",'channel name', "video title"])
    st.write(df3)

elif question=="4.  How many comments were made on each video, and what are their corresponding video names?":
    query4="""select comments as no_comments,title as videotitle from videos where comments is not null """
    cursor.execute(query4)
    
    result4=cursor.fetchall()
    df4=pd.DataFrame(result4, columns=["No of comments", "videotitle"])
    st.write(df4)

elif question=="5.  Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5="""select title as videotitle,channel_name as channelname,likes as likecount
              from videos where likes is not null order by likes desc"""
    cursor.execute(query5)
    
    result5=cursor.fetchall()
    df5=pd.DataFrame(result5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif question=="""6.  What is the total number of likes for each video, and 
               what are their corresponding video names?""":
    query6 = """select likes as likecount, title as videotitle from videos"""

    cursor.execute(query6)
    
    result6=cursor.fetchall()

    df6=pd.DataFrame(result6, columns=["likecount", "videotitle"])
    st.write(df6)
    

elif question == """7.  What is the total number of views for each channel, and what are their
                 corresponding channel names?""":
    query7 = """SELECT channel_name AS channelname, views AS totalviews FROM channels"""
    cursor.execute(query7)
    
    result7 = cursor.fetchall()
    df7 = pd.DataFrame(result7, columns=["channel name", "totalviews"])
    st.write(df7)


elif question == "8.  What are the names of all the channels that have published videos in the year 2022?":
    query8 = """SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname
                FROM videos WHERE EXTRACT(YEAR FROM published_date) = 2022"""
    cursor.execute(query8)
    
    result8 = cursor.fetchall()
    df8 = pd.DataFrame(result8, columns=["video_title", "published_date", "channelname"])
    st.write(df8)



elif question=="""9.  What is the average duration of all videos in each channel, and 
             what are their corresponding channel names?""":
    query9="""select channel_name as channelname,AVG(duration) as averageduration from videos groupby channel_name """
    
    cursor.execute(query9)
   
    result9=cursor.fetchall()
    df9=pd.DataFrame(result9, columns=["channelname", "averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df=pd.DataFrame(T9) 
    st.write(df)  


elif question=="""10. Which videos have the highest number of comments, and what are their 
                corresponding channel names?""":
    query10="""select title as videotitle,channel_name as channelname,comments as comments 
                from videos where comments is not null order by comments desc"""
    cursor.execute(query10)
    result10=cursor.fetchall()
    df10=pd.DataFrame(result10, columns=["video title", "channel name","comments"])
    st.write(df10)  


