from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

api_service_name = "youtube"
api_version = "v3"

# MAKE CONNECTION (YOUTUBE) BY USING API KEY

API_KEY = 'AIzaSyCNRuo72o4ydV_SgWQOgxiVxrvplZPHTCo' 
youtube = build("youtube", "v3", developerKey= API_KEY )

def get_channel_details(channel_id):
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
              )
        response = request.execute()

        return response

#GETTING CHANNELS DETAILS--

def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data

#GETTING PLAYLIST DETAILS

def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data

#GETTING VIDEO IDS

def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

#GETTING VIDEO INFORMATION
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

#GETTING COMMENTS DETAILS

def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information
        
# CONNECTING TO MONGODB
client=pymongo.MongoClient("mongodb+srv://Elango:******@cluster00.4pz5glp.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]

#UPPLOADING CHANEEL DETAILS INTO MONGODB 
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

#ravi ips ('UCxzXJW9HUXdefobUqOV-8QA')
#Alex the analyst ('UC7cs8q-gJRlGwj4A8OmCmXg')

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "upload completed successfully"

# CREATTING THE CONNECTION TO POSTGRAYSQL AND CREATING TABLES
def channels_table():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="****",
            database= "youtube_data",
            port = "5432"
            )
    cursor = mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                        Channel_Id varchar(60) primary key, 
                        Subscription_Count int, 
                        Views int,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel already created" )


    ch_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information']) 
    df=pd.DataFrame(ch_list)  

    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                                                    
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write('channel values already printed')  

# CREATING THE PLAYLIST TABLE
def playlist_table():
        mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="******",
                database= "youtube_data",
                port = "5432"
                )
        cursor = mydb.cursor()

        drop_query='''drop table if exists playlists'''
        cursor.execute(drop_query)
        mydb.commit()

        try:
                create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(50),
                        Channel_Id varchar(160), 
                        ChannelName varchar(50),
                        PublishedAt timestamp,
                        videoCount int)'''
                cursor.execute(create_query)
                mydb.commit()
        except:
        
           print("playlists already created" )


        pl_list=[]
        db=client['youtube_data']
        coll1=db['channel_details']
        for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
         for i in range(len(pl_data['playlist_information'])):
          pl_list.append((pl_data['playlist_information'][i]))
          df1=pd.DataFrame(pl_list)   


        for index,row in df1.iterrows():
                insert_query='''insert into playlists(
                                                        PlaylistId,
                                                        Title,
                                                        Channel_Id, 
                                                        ChannelName,
                                                        PublishedAt,
                                                        videoCount)
                                                                        
                                                        values(%s,%s,%s,%s,%s,%s)'''
                values=(row['PlaylistId'],
                        row['Title'],
                        row['ChannelId'],
                        row['ChannelName'],
                        row['PublishedAt'],
                        row['VideoCount'])

                try:
                 cursor.execute(insert_query,values)
                 mydb.commit()
                except:
                 print('channel values already printed')  
                
                
#CREATING THE VIDEOS TABLE
def videos_table():

    mydb = psycopg2.connect(host="localhost",
                    user="postgres",
                    password="*********",
                    database= "youtube_data",
                    port = "5432"
                    )
    cursor = mydb.cursor()

    drop_query='''drop table if exists videodetails'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
            create_query = '''create table if not exists videodetails(Channel_Name varchar(1000),
                    Channel_Id varchar(1050),
                    Video_Id varchar(1050) primary key,
                    Title varchar(300), 
                    Tags varchar(1400),
                    Thumbnail varchar(1400),
                    Description text,
                    Published_Date timestamp,
                    Duration interval, 
                    Views bigint,
                    Likes bigint,
                    Comments varchar(1000),
                    Favorite_Count bigint,
                    Definition varchar(1380),
                    Caption_Status varchar(1480)
                    )'''
            cursor.execute(create_query)
            mydb.commit()
    except:

        st.write("playlists already created" )

    vd_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for vd_data in coll1.find({},{'_id':0,'video_information':1}):
     for i in range(len(vd_data ['video_information'])):
      vd_list.append((vd_data ['video_information'][i]))
    df2=pd.DataFrame(vd_list)   

    for index, row in df2.iterrows():
            insert_query = '''
                        INSERT INTO videodetails (Channel_Name,
                            Channel_Id,
                            Video_Id, 
                            Title, 
                            Tags,
                            Thumbnail,
                            Description, 
                            Published_Date,
                            Duration, 
                            Views, 
                            Likes,
                            Comments,
                            Favorite_Count, 
                            Definition, 
                            Caption_Status 
                            )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                    '''
            values = (
                        row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'])
                                    
                
            cursor.execute(insert_query,values)
            mydb.commit()
#CREATING THE COMMENTS TABLE
def comment_table():
        mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="*******",
                database= "youtube_data",
                port = "5432"
                )
        cursor = mydb.cursor()                                                          
        drop_query='''drop table if exists commentdata'''
        cursor.execute(drop_query)
        mydb.commit()

        try:
                create_query = '''create table if not exists commentdata(
                        Comment_Id varchar(100) primary key,
                        Video_Id  varchar(50),
                                Comment_Text text, 
                                Comment_Autho text,
                        Comment_Published timestamp)'''
                cursor.execute(create_query)
                mydb.commit()
        except:

                print("comments table already created" )

        cm_list=[]
        db=client['youtube_data']
        coll1=db['channel_details']
        for cm_data in coll1.find({},{'_id':0,'comment_information':1}):
         for i in range(len(cm_data['comment_information'])):
          cm_list.append((cm_data['comment_information'][i]))
        df3=pd.DataFrame(cm_list)   

        for index,row in df3.iterrows():
                        insert_query='''insert into commentdata(
                                                                comment_Id,
                                                                Video_Id,
                                                                Comment_Text, 
                                                                Comment_Autho,
                                                                Comment_Published)
                                                                
                                                                                
                                                                values(%s,%s,%s,%s,%s)'''
                        values=(row['Comment_Id'],
                                row['Video_Id'],
                                row['Comment_Text'],
                                row['Comment_Author'],
                                row['Comment_Published'])

                        
                        cursor.execute(insert_query,values)
                        mydb.commit()
    

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comment_table()
    return ('tables created')


def show_channels_table():
    ch_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])  
    channel_table= st.dataframe(ch_list)
    return channel_table

def show_playlists_table():
    pl_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
      for i in range(len(pl_data['playlist_information'])):
         pl_list.append((pl_data['playlist_information'][i]))
    playlist_table=st.dataframe(pl_list)   
    return playlist_table

def show_videos_table():
 vd_list=[]
 db=client['youtube_data']
 coll1=db['channel_details']
 for vd_data in coll1.find({},{'_id':0,'video_information':1}):
    for i in range(len(vd_data ['video_information'])):
      vd_list.append((vd_data ['video_information'][i]))
 df2=st.dataframe(vd_list)   
 return df2

def show_comments_table():
    cm_list=[]
    db=client['youtube_data']
    coll4=db['channel_details']
    for cm_data in coll4.find({},{'_id':0,'comment_information':1}):
        for i in range(len(cm_data['comment_information'])):
         cm_list.append((cm_data['comment_information'][i]))
    df3=st.dataframe(cm_list)  
    return df3 


#streamlit

with st.sidebar:
    st.title(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
    st.header('skills take away')
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")

#Getting channel ID AS INPUT
channel_id=st.text_input("Enter the channel ID")

if st.button('select'): 
    ch_ids=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
     ch_ids.append(ch_data['channel_information']['Channel_Id'])

    if channel_id in ch_ids:
        st.success('channelid already exists')

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("Select the table for view",("channels","Playlists","Videos","comments"))

if show_table=="channels":
    show_channels_table()
    
elif show_table=="Playlists":  
    show_playlists_table()

elif show_table=="Videos": 
    show_videos_table() 
            
elif show_table=="comments": 
    show_comments_table() 


#SQL connection & querys
mydb = psycopg2.connect(host="localhost",
 user="postgres",
 password="*******",
 database= "youtube_data",
 port = "5432"
 )
cursor = mydb.cursor() 

question=st.selectbox("Select your qustions",('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from Videodetails;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    table1=(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))
    st.write(table1)

elif question=='2. Channels with most number of videos':
    question2="select channel_name as Channel_Name ,total_videos as video_count from channels order by total_videos desc;"
    cursor.execute(question2)
    mydb.commit()
    C1=cursor.fetchall()
    table2=(pd.DataFrame(C1,columns=["channel Name","total videos"]))
    st.write(table2)

elif question=='3. 10 most viewed videos':
    question3='''select views as most_viewed,title as videos,channel_name as ChannelName from videodetails order by views desc limit 10'''
    cursor.execute(question3)
    mydb.commit()
    c2=cursor.fetchall()
    table3=pd.DataFrame(c2,columns=['number_of_view','Video_title','channel_name'])
    st.write(table3)

elif question == '4. Comments in each video':
    question4 = '''SELECT comments AS No_comments, title AS VideoTitle FROM videodetails WHERE comments IS NOT NULL'''
    cursor.execute(question4)
    mydb.commit()
    c3 = cursor.fetchall()
    table4 = pd.DataFrame(c3, columns=["No_comments", "Video_Title"])
    st.write(table4)

elif question == '5. Videos with highest likes':
    query5 = '''select title as VideoTitle, channel_name as ChannelName, likes as LikesCount from videodetails 
                       where likes is not null order by likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    table5=(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))
    st.write(table5)

elif question == '6. likes of all videos':
    query6 = '''select likes as likeCount,title as VideoTitle from videodetails;'''
    cursor.execute(query6)
    mydb.commit()
    c6= cursor.fetchall()
    st.write(pd.DataFrame(c6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select channel_name as ChannelName,views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    c7=cursor.fetchall()
    st.write(pd.DataFrame(c7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select title as Video_Title, published_date as VideoRelease, channel_name as ChannelName from videodetails
                where extract(year from published_date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    table8=cursor.fetchall()
    st.write(pd.DataFrame(table8,columns=["Video Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT channel_name as ChannelName, AVG(duration) AS average_duration FROM videodetails GROUP BY channel_name;"
    cursor.execute(query9)
    mydb.commit()
    table9=cursor.fetchall()
    t9 = pd.DataFrame(table9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select title as VideoTitle, channel_name as ChannelName, comments as comments from videodetails
                       where comments is not null order by comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    table10=cursor.fetchall()
    st.write(pd.DataFrame(table10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
