#import necessarry files
from googleapiclient.discovery import build
import pymongo 
import mysql.connector
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np

from datetime import datetime, timedelta
import re

# Function to connect to the YouTube API
def Api_connect():
    Api_Id = "AIzaSyDrAYaqVvWH28lUrHkjSKz1BoT1C4L0va4"
    Api_service_name = "youtube"
    Api_version = "v3"
    youtube= build(Api_service_name, Api_version, developerKey=Api_Id)
    return youtube
youtube= Api_connect()

# Function to get channel information from YouTube API
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

# Function to get video IDs from a channel
def get_video_ids(channel_id,):
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
                                

# Function to get video information
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

# Function to get comment information
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
                        Video_Id=item['snippet']['topLevelComment']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# Function to get playlist details from a channel
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

# Function to upload data to MongoDB
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

# Function to create MySQL tables
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
                    Channel_Name VARCHAR(255),
                    Channel_Id VARCHAR(255) PRIMARY KEY,
                    Subscribers VARCHAR(255),
                    Views VARCHAR(255),
                    Total_Videos VARCHAR(255),
                    Channel_description TEXT,
                    Playlist_Id VARCHAR(255) )"""
    cursor.execute(create_query)
    mydb.commit()

def playlists_table():
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

    drop_query = """DROP TABLE IF EXISTS playlists """
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """CREATE TABLE IF NOT EXISTS playlists(
                    Playlist_Id VARCHAR(255) PRIMARY KEY,
                    Title VARCHAR(255),
                    Channel_Id VARCHAR(255),
                    Channel_Name VARCHAR(255),
                    Published_At VARCHAR(255),
                    Video_Count VARCHAR(255) )"""
    cursor.execute(create_query)
    mydb.commit()

def videos_table():
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

    drop_query = """DROP TABLE IF EXISTS videos """
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """CREATE TABLE IF NOT EXISTS videos(
                    Channel_Name VARCHAR(255),
                    channel_Id VARCHAR(255),
                    video_Id VARCHAR(255) PRIMARY KEY,
                    Title VARCHAR(255),
                    Tags TEXT,
                    Thumbnail VARCHAR(255),
                    Description TEXT,
                    Published_Date VARCHAR(255),
                    Duration VARCHAR(255),
                    Views VARCHAR(255),
                    Likes VARCHAR(255),
                    Comments VARCHAR(255),
                    Favourite_Count VARCHAR(255),
                    Definition VARCHAR(255),
                    Caption_Status VARCHAR(255) )"""
    cursor.execute(create_query)
    mydb.commit()

def comments_table():
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

    drop_query = """DROP TABLE IF EXISTS comments """
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """CREATE TABLE IF NOT EXISTS comments(
                    Comment_Id VARCHAR(255) PRIMARY KEY,
                    Video_Id VARCHAR(255),
                    Comment_Text TEXT,
                    Comment_Author VARCHAR(255),
                    Comment_Published VARCHAR(255) )"""
    cursor.execute(create_query)
    mydb.commit()

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return "tables created successfully"

#display of tables

def zebra_stripe(df):
        return df.style.apply(lambda x: ['background-color: #ACE1AF' if i % 2 == 0 else '' for i in range(len(x))])



def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({}, {"_id":0, "channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    
    # Apply zebra striping to the DataFrame
    styled_df = zebra_stripe(df)

    # Display the styled DataFrame
    st.dataframe(styled_df)

    return df


def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({}, {"_id":0, "playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1 = pd.DataFrame(pl_list)
    
    # Apply zebra striping
    styled_df = zebra_stripe(df1)
    
    # Display the styled DataFrame
    st.dataframe(styled_df)

    return df1



def show_video_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({}, {"_id":0, "video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2 = pd.DataFrame(vi_list)
    
    # Apply zebra striping
    styled_df = zebra_stripe(df2)
    
    # Display the styled DataFrame
    st.dataframe(styled_df)
    
    return df2


def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({}, {"_id":0, "comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3 = pd.DataFrame(com_list)
    
    # Apply zebra striping
    styled_df = zebra_stripe(df3)
    
    # Display the styled DataFrame
    st.dataframe(styled_df)

    return df3


#streamlit part
# Function to display About details
def display_about():
    st.markdown("<h1 style='text-align: center; color: #FFD700;'>YOUTUBE DATA HARVESTING AND WAREHOUSING</h1>", unsafe_allow_html=True)
    st.markdown("""
    <p style='color:purple;'>This is a web application for harvesting and warehousing data from YouTube channels. It allows users to enter a YouTube channel ID, 
    collect various data such as channel information, playlists, videos, and comments, and store them in a database for further analysis.</p>
    """, unsafe_allow_html=True)

    st.header("SKILL TAKE AWAY")
    st.write("In this project, you will gain proficiency in a range of essential skills:")
    st.markdown("""
        <style>
            .python { background-color: #ffcccb; padding: 10px; border-radius: 5px; }
            .data-collection { background-color: #c2e0c6; padding: 10px; border-radius: 5px; }
            .mongodb { background-color: #f0e68c; padding: 10px; border-radius: 5px; }
            .api { background-color: #afeeee; padding: 10px; border-radius: 5px; }
            .data-management { background-color: #ffb6c1; padding: 10px; border-radius: 5px; }
        </style>
        """, unsafe_allow_html=True)
    st.markdown("- <div class='python'>**Python Scripting**: Users will harness the power of Python scripting to automate data collection, manipulation, and integration tasks, ensuring efficient and reliable workflows.</div>", unsafe_allow_html=True)
    st.markdown("- <div class='data-collection'>**Data Collection**: Learn effective strategies for collecting data from diverse sources, including web APIs such as the YouTube Data API, ensuring comprehensive access to valuable information.</div>", unsafe_allow_html=True)
    st.markdown("- <div class='mongodb'>**MongoDB**: Dive into MongoDB, a leading NoSQL database solution, and master its usage for storing, organizing, and managing vast volumes of unstructured data with flexibility and scalability.</div>", unsafe_allow_html=True)
    st.markdown("- <div class='api'>**API Integration**: Acquire expertise in integrating with web APIs, enabling seamless communication between your Python scripts and external platforms to retrieve and process real-time data.</div>", unsafe_allow_html=True)
    st.markdown("- <div class='data-management'>**Data Management Using MongoDB and SQL**: Explore the nuances of managing data across different storage systems, from NoSQL databases like MongoDB to relational databases like MySQL, ensuring effective data warehousing and analysis capabilities.</div>", unsafe_allow_html=True)
    st.write("These skills collectively empower users to architect robust data pipelines, from ingestion to storage to analysis, positioning them as a proficient data engineers capable of handling complex data-centric projects.")

# Function to display Demo details
def display_demo():
    st.markdown("""
    <p style='color:blue;'>This demo provides a simple interface to collect data from YouTube channels using their channel ID. 
    Users can input the channel ID and click on the 'Collect and Store Data' button to initiate the data collection process.</p>
    """, unsafe_allow_html=True)

    st.write("Here you can enter a YouTube channel ID to collect and store data.")

    channel_id = st.text_input("Enter Channel ID")

    if st.button("Collect and Store Data"):
        # Check if the channel ID already exists in the database
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
        cursor.execute("SELECT * FROM channels WHERE Channel_Id = %s", (channel_id,))
        existing_channel = cursor.fetchone()
        
        
        ch_ids=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({}, {"_id":0, "channel_information":1}):
            ch_ids.append(ch_data['channel_information']['Channel_Id'])


        if channel_id in ch_ids:
            st.success("Channel information already exists!")
        else:
            insert=channel_details(channel_id)
            st.success(insert)
    
    
    st.write("View By:")

    # Add tabs for different views
    view_option = st.radio("Choose view option:", ("Channels", "Playlists", "Videos", "Comments"))

    if view_option == "Channels":  
        show_channels_table() 

    elif view_option == "Playlists":
        show_playlists_table()

    elif view_option == "Videos":
        show_video_table()

    elif view_option == "Comments":
        show_comments_table()

    # Choose your question dropdown
    question = st.selectbox("Choose your question", ("1. What are the names of all the videos and their corresponding channels?",
                      "2.  Which channels have the most number of videos, and how many videos do they have",
                      "3.  What are the top 10 most viewed videos and their respective channels?",
                      "4.  How many comments were made on each video, and what are their corresponding video names?",
                      "5.  Which videos have the highest number of likes, and what are their corresponding channel names?",
                      "6.  What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                      "7.  What is the total number of views for each channel, and what are their corresponding channel names?",
                      "8.  What are the names of all the channels that have published videos in the year 2022?",
                      "9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                      "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

    # Add buttons for insights representation
    insights_representation = st.radio("Insights Representation:", ("Table View", "Chart View"))

    # Handling queries
    # SQL connection
    mydb = mysql.connector.connect(host="localhost",
                    user="root",
                    password="test",
                    database="Youtube_data",
                    port="3306",
                    auth_plugin='mysql_native_password',
                    charset='utf8mb4')
                    
    cursor = mydb.cursor()

    if question == "1. What are the names of all the videos and their corresponding channels?":
        query1 = """SELECT title AS videos, channel_name AS channelname FROM videos"""
        cursor.execute(query1)
        result1 = cursor.fetchall()
        df1 = pd.DataFrame(result1, columns=["video title", 'channel name'])

        if insights_representation == "Table View":
            # Apply zebra striping
            styled_df = zebra_stripe(df1)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
        else:
            # Prepare data for pie chart
            video_counts = df1["channel name"].value_counts()
            labels = video_counts.index.tolist()
            values = video_counts.values.tolist()

            # Plot pie chart
            fig, ax = plt.subplots()
            ax.pie(values, labels=labels, autopct='%1.1f%%')
            ax.axis('equal')
            st.pyplot(fig)

    elif question == "2.  Which channels have the most number of videos, and how many videos do they have":
        query2 = "SELECT channel_name AS channelname, total_videos AS no_videos FROM channels ORDER BY total_videos DESC"
        cursor.execute(query2)
        result2 = cursor.fetchall()
        df2 = pd.DataFrame(result2, columns=["channel name", 'No of videos'])
        
        if insights_representation == "Table View":
            # Apply zebra striping
            styled_df = zebra_stripe(df2)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
        else:
            # Prepare data for pie chart
            channel_counts = df2["No of videos"].tolist()
            labels = df2["channel name"].tolist()

            # Debugging: Print dataframe and lists for debugging
            print("Dataframe:")
            print(df2)
            print("Labels:", labels)
            print("Values:", channel_counts)

            # Plot pie chart with correct values
            fig, ax = plt.subplots()
            ax.pie(channel_counts, labels=labels, autopct='%1.1f%%')
            ax.axis('equal')
            st.pyplot(fig)



    elif question == "3.  What are the top 10 most viewed videos and their respective channels?":
        query3 = """SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos 
                    WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10"""
        cursor.execute(query3)
        result3 = cursor.fetchall()
        df3 = pd.DataFrame(result3, columns=["views", 'channel name', "video title"])
        if insights_representation == "Table View":
             # Apply zebra striping
            styled_df = zebra_stripe(df3)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
        else:
            # Prepare data for pie chart
            video_views = df3["video title"].tolist()
            views = df3["views"].tolist()

            # Plot pie chart
            fig, ax = plt.subplots()
            ax.pie(views, labels=video_views, autopct='%1.1f%%')
            ax.axis('equal')
            st.pyplot(fig)

    elif question == "4.  How many comments were made on each video, and what are their corresponding video names?":
        query4 = """SELECT comments AS no_comments, channel_name FROM videos WHERE comments IS NOT NULL"""
        
        # Execute the SQL query and fetch the results
        cursor.execute(query4)
        result4 = cursor.fetchall()
        df4 = pd.DataFrame(result4, columns=["No of comments", "channel_name"])
            
        # Check the insights representation option and display accordingly
        if insights_representation == "Table View":
             # Apply zebra striping
            styled_df = zebra_stripe(df4)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
           
        else:
            # Aggregate comment counts by channel
            channel_comments = df4.groupby("channel_name")["No of comments"].sum().reset_index()

            # Prepare data for pie chart
            channel_names = channel_comments["channel_name"].tolist()
            comment_counts = channel_comments["No of comments"].tolist()

            # Plot pie chart
            fig, ax = plt.subplots()
            ax.pie(comment_counts, labels=channel_names, autopct='%1.1f%%')
            ax.axis('equal')

            # Display the pie chart
            st.pyplot(fig)


    elif question == "5.  Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = """SELECT title AS videotitle, channel_name AS channelname, likes AS likecount
                FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC"""
        
        # Execute the SQL query and fetch the results
        cursor.execute(query5)
        result5 = cursor.fetchall()
        df5 = pd.DataFrame(result5, columns=["videotitle", "channelname", "likecount"])
            
        # Check the insights representation option and display accordingly
        if insights_representation == "Table View":
             # Apply zebra striping
            styled_df = zebra_stripe(df5)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
           
        else:
            # Aggregate like counts by channel
            channel_likes = df5.groupby("channelname")["likecount"].sum().reset_index()

            # Prepare data for pie chart
            channel_names = channel_likes["channelname"].tolist()
            like_counts = channel_likes["likecount"].tolist()

            # Plot pie chart
            fig, ax = plt.subplots()
            ax.pie(like_counts, labels=channel_names, autopct='%1.1f%%')
            ax.axis('equal')

            # Display the pie chart
            st.pyplot(fig)



    elif question == "6.  What is the total number of likes for each video, and what are their corresponding video names?":
        query6 = """SELECT channel_name, SUM(likes) AS total_likes
                    FROM videos
                    GROUP BY channel_name"""
            
        # Execute the SQL query and fetch the results
        cursor.execute(query6)
        result6 = cursor.fetchall()
        df6 = pd.DataFrame(result6, columns=["channel_name", "total_likes"])
            
        # Check the insights representation option and display accordingly
        if insights_representation == "Table View":
             # Apply zebra striping
            styled_df = zebra_stripe(df6)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
            
        else:
            # Prepare data for pie chart
            channel_names = df6["channel_name"].tolist()
            like_counts = df6["total_likes"].tolist()

            # Plot pie chart
            fig, ax = plt.subplots()
            ax.pie(like_counts, labels=channel_names, autopct='%1.1f%%')
            ax.axis('equal')

            # Display the pie chart
            st.pyplot(fig)


    elif question == """7.  What is the total number of views for each channel, and what are their corresponding channel names?""":
        query7 = """SELECT channel_name AS channelname, views AS totalviews FROM channels"""
        
        # Execute the SQL query and fetch the results
        cursor.execute(query7)
        result7 = cursor.fetchall()
        df7 = pd.DataFrame(result7, columns=["channel name", "totalviews"])
        
        # Check the insights representation option and display accordingly
        if insights_representation == "Table View":
             # Apply zebra striping
            styled_df = zebra_stripe(df7)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
           
        else:
            # Prepare data for pie chart
            channel_names = df7["channel name"].tolist()
            total_views = df7["totalviews"].tolist()

            print("Channel Names:", channel_names)  # Debugging statement
            print("Total Views:", total_views)  # Debugging statement

            # Plot pie chart
            fig, ax = plt.subplots()
            ax.pie(total_views, labels=channel_names, autopct='%1.1f%%')
            ax.axis('equal')
            st.pyplot(fig)

    elif question == "8.  What are the names of all the channels that have published videos in the year 2022?":
        query8 = """SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname
                    FROM videos WHERE EXTRACT(YEAR FROM published_date) = 2022"""
        
        # Execute the SQL query and fetch the results
        cursor.execute(query8)
        result8 = cursor.fetchall()
        df8 = pd.DataFrame(result8, columns=["video_title", "published_date", "channelname"])
        
        # Check the insights representation option and display accordingly
        if insights_representation == "Table View":
             # Apply zebra striping
            styled_df = zebra_stripe(df8)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
            
        else:
            # Prepare data for pie chart (not applicable for this query)
            st.write("Chart view is not applicable for this query.")

    elif question == "9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = """SELECT channel_name AS channelname, AVG(duration) AS averageduration 
                    FROM videos GROUP BY channel_name"""
        
        # Execute the SQL query and fetch the results
        cursor.execute(query9)
        result9 = cursor.fetchall()
        df9 = pd.DataFrame(result9, columns=["channelname", "averageduration"])

        if insights_representation == "Table View":
             # Apply zebra striping
            styled_df = zebra_stripe(df9)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
           
        else:
            # Prepare data for pie chart
            channel_names = df9["channelname"].tolist()
            average_durations = df9["averageduration"].tolist()

            # Plot pie chart
            plt.figure(figsize=(8, 8))
            plt.pie(average_durations, labels=channel_names, autopct='%1.1f%%', startangle=140)
            plt.axis('equal')
            plt.title('Average Duration of Videos in Each Channel')
            plt.tight_layout()

            # Pass the figure object explicitly to st.pyplot()
            st.pyplot(plt.gcf())

    elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10 = """SELECT channel_name AS channelname, SUM(comments) AS total_comments
                    FROM videos
                    WHERE comments IS NOT NULL
                    GROUP BY channel_name
                    ORDER BY total_comments DESC"""

        # Execute the SQL query and fetch the results
        cursor.execute(query10)
        result10 = cursor.fetchall()
        df10 = pd.DataFrame(result10, columns=["channel name", "total comments"])

        if insights_representation == "Table View":
             # Apply zebra striping
            styled_df = zebra_stripe(df10)
            
            # Display the styled DataFrame
            st.dataframe(styled_df)
            
        else:
            # Prepare data for pie chart
            labels = df10["channel name"].tolist()
            values = df10["total comments"].tolist()

            # Plot pie chart with rotated labels
            fig, ax = plt.subplots()
            ax.pie(values, labels=labels, autopct='%1.1f%%')
            ax.axis('equal')

            # Rotate labels to prevent overlapping
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()

            st.pyplot(fig)



def main():
    ch_ids = [] 
   
    
    # Sidebar navigation
    nav = st.sidebar.radio("My Project", ["About", "Demo"])

    # Display content based on navigation selection
    if nav == "About":
        display_about()
    else:
        display_demo()

    


# Execute the main function
if __name__ == "__main__":
    main()
