#LIBRARIES TO IMPORT

import googleapiclient.discovery
from googleapiclient.errors import HttpError
import streamlit as st
import mysql.connector
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import re

# Interpret the YouTube API service
api_service_name = "youtube"
api_version = "v3"
api_key1 = "AIzaSyAmZjyyefYcIO0_CZ4IljLP4q5B_LrSWgI"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key1)

# To fetch Data from MySQL database
def Fetch_data(query):
    mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YOUTUBE_DATA")
    df = pd.read_sql(query, mydb)
    mydb.close()
    return df

# To fetch channel data using YouTube API
def Fetch_channel_data(channel_id):
    try:
        response = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id).execute()
        if 'items' in response and len(response['items']) > 0:
            channel_data = {
                "channel_name": response['items'][0]['snippet']['title'],
                "channel_id": channel_id,
                "channel_dis": response['items'][0]['snippet']['description'],
                "channel_pid": response['items'][0]['contentDetails']['relatedPlaylists'],
                "channel_subc": response['items'][0]['statistics']['subscriberCount'],
                "channel_vidc": response['items'][0]['statistics']['videoCount'],
                "channel_vc": response['items'][0]['statistics']['viewCount']
            }

            cnl_data=(channel_data['channel_name'], channel_data['channel_id'], channel_data['channel_dis'], channel_data['channel_pid'], channel_data['channel_subc'], channel_data['channel_vidc'], channel_data['channel_vc'])



            with mysql.connector.connect(host="localhost", user="root", password="", database="YOUTUBE_DATA") as mydb:
                with mydb.cursor() as mycursor:
                    cnl_sqlQ1="""
                        INSERT INTO YOUTUBE_CHANNEL_DATA (channel_name, channel_id, channel_dis, channel_pid, channel_subc, channel_vidc, channel_vc)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    mycursor.execute=(cnl_sqlQ1,cnl_data)
                    mydb.commit()
            return pd.DataFrame(channel_data, index=[0])
        else:
            st.error("No items found in the response.")
            return pd.DataFrame()
    except HttpError as e:
        st.error(f"HTTP Error: {e}")
        return pd.DataFrame()
    except KeyError as e:
        st.error(f"KeyError: {e}. Please make sure the channel ID is correct.")
        return pd.DataFrame()

# To fetch VIDEO data using YouTube API
def yt_video_ID(channel_id):
    all_video_ids = []
    try:
            response = youtube.channels().list(part='contentDetails', id=channel_id).execute()
            Plid = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            nxt_page_token = None
            while True:
                response1 = youtube.playlistItems().list(
                    part='snippet',
                    playlistId=Plid,
                    maxResults=50,
                    pageToken=nxt_page_token).execute()
                for i in range(len(response1['items'])):
                    all_video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                nxt_page_token = response1.get('nextPageToken')
                if nxt_page_token is None:
                    break
    except HttpError as e:
        st.error(f"HTTP Error: {e}")
    except KeyError as e:
        st.error(f"KeyError: {e}")
    else:
        st.error(f"No channels found for ID: {channel_id}")

    return all_video_ids

def Fetch_video_data(video_ids):
    video_datas = []
    for video_data in video_ids:
        response2 = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_data).execute()
        for Details in response2["items"]:
            Video_Details = {
                'Channel_Name': Details['snippet']['channelTitle'],
                'Channel_id': Details['snippet']['channelId'],
                'Video_id': Details['id'],
                'PublishedDate': Details['snippet']['publishedAt'],
                'Video_title': Details['snippet']['title'],
                'Description': Details['snippet']['description'],
                'Duration': du_to_sec(Details['contentDetails']['duration']),
                'Views': Details['statistics']['viewCount'],
                'Likecount': Details['statistics'].get('likeCount'),
                'Comments': Details.get('commentCount'),
            }
            video_datas.append(Video_Details)

            mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YOUTUBE_DATA")
            mycursor = mydb.cursor(buffered=True)

            vd_data = (Video_Details['Channel_Name'], Video_Details['Channel_id'], Video_Details['Video_id'],
                       Video_Details['PublishedDate'], Video_Details['Video_title'], Video_Details['Description'],
                       Video_Details['Duration'], Video_Details['Views'], Video_Details['Likecount'],Video_Details['Comments'])

            Vd_sqlQ1 = """
                    INSERT INTO YOUTUBE_VIDEOS_DATA (Channel_Name, Channel_id, Video_id, PublishedDate, Video_title, Description, Duration, Views, Likecount, Comments)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            mycursor.execute(Vd_sqlQ1, vd_data)
            mydb.commit()
            mydb.close()

    return pd.DataFrame(video_datas)

def du_to_sec(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

# To Fetch comment Data's
def Fetch_comment(channel_id):
    comment_datas = []
    all_video_ids = yt_video_ID([channel_id])

    for cmnt_dt in all_video_ids:
        try:
            response3 = youtube.commentThreads().list(part="snippet", videoId=cmnt_dt, maxResults=100).execute()
            for commentdata in response3["items"]:
                Comment_Details = {
                    'Comment_id': commentdata['snippet']['topLevelComment']['id'],
                    'Video_id': commentdata['snippet']['topLevelComment']['snippet']['videoId'],
                    'channel_id': commentdata['snippet']['channelId'],
                    'comment_author_name': commentdata.get('authorDisplayName'),
                    'comment_text': commentdata.get('textDisplay'),
                    'commentPublished_date': commentdata['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comment_datas.append(Comment_Details)

        except HttpError as e:
            if e.resp.status == 403:
                st.warning(f"Comments are disabled for some videos in channel ID: {channel_id}")
                break
            else:
                raise

    mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YOUTUBE_DATA")
    mycursor = mydb.cursor()

    for Comment_Details in comment_datas:
        cmmnt_data = (Comment_Details['Comment_id'], Comment_Details['Video_id'], Comment_Details['channel_id'],
                      Comment_Details['comment_author_name'], Comment_Details['comment_text'],
                      Comment_Details['commentPublished_date'])
        Cmmnt_sqlQ1 = """
                INSERT INTO YOUTUBE_COMMENTS_DATA (Comment_id, Video_id, Channel_id, comment_author_name, comment_text, commentPublished_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
        mycursor.execute(Cmmnt_sqlQ1, cmmnt_data)
    mydb.commit()
    mydb.close()

    return pd.DataFrame(comment_datas)

# To execute the predefined Queries
def execute_query(question):
    Query_mapping = {
        "1. What are the names of all the videos and their corresponding channels?": """
            SELECT YOUTUBE_VIDEOS_DATA.Video_title, YOUTUBE_CHANNEL_DATA.channel_name
            FROM YOUTUBE_VIDEOS_DATA
            JOIN YOUTUBE_CHANNEL_DATA ON YOUTUBE_VIDEOS_DATA.channel_id = YOUTUBE_CHANNEL_DATA.channel_id;
        """,
        "2. Which channels have the most number of videos, and how many videos do they have?": """
            SELECT YOUTUBE_CHANNEL_DATA.channel_name, COUNT(YOUTUBE_VIDEOS_DATA.video_id) AS video_count
            FROM YOUTUBE_VIDEOS_DATA
            JOIN YOUTUBE_CHANNEL_DATA ON YOUTUBE_VIDEOS_DATA.channel_id = YOUTUBE_CHANNEL_DATA.channel_id
            GROUP BY YOUTUBE_CHANNEL_DATA.channel_name
            ORDER BY video_count DESC
            LIMIT 1;
        """,
        "3. What are the top 10 most viewed videos and their respective channels?": """
            SELECT YOUTUBE_VIDEOS_DATA.Video_title, YOUTUBE_CHANNEL_DATA.channel_name
            FROM YOUTUBE_VIDEOS_DATA
            JOIN YOUTUBE_CHANNEL_DATA ON YOUTUBE_VIDEOS_DATA.channel_id = YOUTUBE_CHANNEL_DATA.channel_id
            ORDER BY YOUTUBE_VIDEOS_DATA.Views DESC
            LIMIT 10;
        """,
        "4. How many comments were made on each video, and what are their corresponding video names?": """
            SELECT YOUTUBE_VIDEOS_DATA.Video_title, COUNT(*) AS comment_count
            FROM YOUTUBE_VIDEOS_DATA
            JOIN YOUTUBE_COMMENTS_DATA ON YOUTUBE_VIDEOS_DATA.Video_Id = YOUTUBE_COMMENTS_DATA.video_id
            GROUP BY YOUTUBE_VIDEOS_DATA.Video_title;
        """,
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?": """
            SELECT YOUTUBE_VIDEOS_DATA.Video_title, YOUTUBE_CHANNEL_DATA.channel_name
            FROM YOUTUBE_VIDEOS_DATA
            JOIN YOUTUBE_CHANNEL_DATA ON YOUTUBE_VIDEOS_DATA.channel_id = YOUTUBE_CHANNEL_DATA.channel_id
            ORDER BY YOUTUBE_VIDEOS_DATA.Likecount DESC
            LIMIT 1;
        """,
        "6. What is the total number of likes for each video, and what are their corresponding video names?": """
            SELECT YOUTUBE_VIDEOS_DATA.Video_title, SUM(YOUTUBE_VIDEOS_DATA.Likecount) AS total_likes
            FROM YOUTUBE_VIDEOS_DATA
            GROUP BY YOUTUBE_VIDEOS_DATA.Video_title;
        """,
        "7. What is the total number of views for each channel, and what are their corresponding channel names?": """
            SELECT YOUTUBE_CHANNEL_DATA.channel_name, SUM(YOUTUBE_VIDEOS_DATA.Views) AS total_views
            FROM YOUTUBE_VIDEOS_DATA
            JOIN YOUTUBE_CHANNEL_DATA ON YOUTUBE_VIDEOS_DATA.channel_id = YOUTUBE_CHANNEL_DATA.channel_id
            GROUP BY YOUTUBE_CHANNEL_DATA.channel_name;
        """,
        "8. What are the names of all the channels that have published videos in the year 2022?": """
            SELECT DISTINCT YOUTUBE_CHANNEL_DATA.channel_name
            FROM YOUTUBE_CHANNEL_DATA
            JOIN YOUTUBE_VIDEOS_DATA ON YOUTUBE_CHANNEL_DATA.channel_id = YOUTUBE_VIDEOS_DATA.channel_id
            WHERE YEAR(YOUTUBE_VIDEOS_DATA.PublishedDate) = 2022;
        """,
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?": """
            SELECT YOUTUBE_CHANNEL_DATA.channel_name, AVG(YOUTUBE_VIDEOS_DATA.Duration) AS average_duration
            FROM YOUTUBE_VIDEOS_DATA
            JOIN YOUTUBE_CHANNEL_DATA ON YOUTUBE_VIDEOS_DATA.channel_id = YOUTUBE_CHANNEL_DATA.channel_id
            GROUP BY YOUTUBE_CHANNEL_DATA.channel_name;
        """,
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?": """
            SELECT YOUTUBE_VIDEOS_DATA.Video_title, YOUTUBE_CHANNEL_DATA.channel_name
            FROM YOUTUBE_VIDEOS_DATA
            JOIN YOUTUBE_CHANNEL_DATA ON YOUTUBE_VIDEOS_DATA.channel_id = YOUTUBE_CHANNEL_DATA.channel_id
            ORDER BY YOUTUBE_VIDEOS_DATA.Comments DESC
            LIMIT 1;
        """
    }
    query = Query_mapping.get(question)
    if query:
        return Fetch_data(query)
    else:
        return pd.DataFrame()


def main():
    st.title("YouTube Data Harvesting and Warehousing using SQL and Streamlit")
    st.sidebar.header("Tables")

    selected_option = st.sidebar.radio("Select Option", ("Channels", "Videos", "Comments", "Enter YouTube Channel ID", "Queries"))

    if selected_option == "Channels":
        st.header("Channels")
        channels_df = Fetch_data("SELECT * FROM YOUTUBE_CHANNEL_DATA;")
        channels_df.index += 1
        st.dataframe(channels_df)

    elif selected_option == "Videos":
        st.header("Videos")
        videos_df = Fetch_data("SELECT * FROM YOUTUBE_VIDEOS_DATA;")
        videos_df.index += 1
        st.dataframe(videos_df)

    elif selected_option == "Comments":
        st.header("Comments")
        comments_df = Fetch_data("SELECT * FROM YOUTUBE_COMMENTS_DATA;")
        comments_df.index += 1
        st.dataframe(comments_df)

    elif selected_option == "Queries":
        st.header("Queries")
        query_question = st.selectbox("Select Query", [
            "1. What are the names of all the videos and their corresponding channels?",
            "2. Which channels have the most number of videos, and how many videos do they have?",
            "3. What are the top 10 most viewed videos and their respective channels?",
            "4. How many comments were made on each video, and what are their corresponding video names?",
            "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
            "6. What is the total number of likes for each video, and what are their corresponding video names?",
            "7. What is the total number of views for each channel, and what are their corresponding channel names?",
            "8. What are the names of all the channels that have published videos in the year 2022?",
            "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
        ], key="queries")
        if query_question:
            query_result_df = execute_query(query_question)
            query_result_df.index += 1
            st.dataframe(query_result_df)

    elif selected_option == "Enter YouTube Channel ID":
        st.header("Enter YouTube Channel ID")
        channel_id = st.text_input("Channel ID")
        if st.button("Fetch Channel Data", key="fetch_channel_data"):
            channel_df = Fetch_channel_data(channel_id)
            channel_df.index += 1
            st.subheader("Channel Data")
            st.write(channel_df)

        if st.button("Fetch Video Data", key="fetch_video_data"):
            all_video_ids = yt_video_ID([channel_id])
            video_df = Fetch_video_data(all_video_ids)
            video_df.index += 1
            st.subheader("Video Data")
            st.write(video_df)

        if st.button("Fetch Comment Data", key="fetch_comment_data"):
            comment_df = Fetch_comment(channel_id)
            comment_df.index += 1
            st.subheader("Comment Data")
            st.write(comment_df)

if __name__ == "__main__":
    main()
