# YouTube Data Harvesting and Warehousing #

This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

**NAME : Guru Ragavendar**

**BATCH : MDE88**

**Capstone Guvi PROJECT : 1**

## Skills take away From This Project ##

**-> Python scripting** 
**-> Data Collection**
**-> MongoDB**
**-> SQL**
**-> Streamlit** 
**-> API integration** 
**-> Data Management**

## Approach ##

1. Set up a Streamlit app: Streamlit is a great choice for building data visualization and analysis tools quickly and easily. You can use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.
2. Connect to the YouTube API: You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.
3. Store data in a MongoDB data lake: Once you retrieve the data from the YouTube API, you can store it in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.
4. Migrate data to a SQL data warehouse: After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL or PostgreSQL for this.
5. Query the SQL data warehouse: You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. You can use a Python SQL library such as SQLAlchemy to interact with the SQL database.
6. Display data in the Streamlit app: Finally, you can display the retrieved data in the Streamlit app. You can use Streamlit's data visualization features to create charts and graphs to help users analyze the data.


**Example Data Extraction from Youtube to MongoDB:**
```
{
"Channel_Name": {
"Channel_Name": "Example Channel",
"Channel_Id": "UC1234567890",
"Subscription_Count": 10000,
"Channel_Views": 1000000,
"Channel_Description": "This is an example channel.",
"Playlist_Id": "PL1234567890"},

"Video_Id_1": {
"Video_Id": "V1234567890",
"Video_Name": "Example Video 1",
"Video_Description": "This is an example video.",
"Tags": ["example", "video"],
"PublishedAt": "2022-01-01T00:00:00Z",
"View_Count": 1000,
"Like_Count": 100,
"Dislike_Count": 10,
"Favorite_Count": 5,
"Comment_Count": 20,
"Duration": "00:05:00",
"Thumbnail": "https://example.com/thumbnail.jpg",
"Caption_Status": "Available",

"Comments": {
"Comment_Id_1": {
"Comment_Id": "C1234567890",
"Comment_Text": "This is a comment.",
"Comment_Author": "Example User",
"Comment_PublishedAt": "2022-01-01T00:01:00Z"},

"Comment_Id_2": {
"Comment_Id": "C2345678901",
"Comment_Text": "This is another comment.",
"Comment_Author": "Another User",
"Comment_PublishedAt": "2022-01-01T00:02:00Z"}}},

"Video_Id_2": {
"Video_Id": "V2345678901",
"Video_Name": "Example Video 2",
"Video_Description": "This is another example video.",
"Tags": ["example", "video"],
"PublishedAt": "2022-01-02T00:00:00Z",
"View_Count": 2000,
"Like_Count": 200,
"Favorite_Count": 10,
"Comment_Count": 30,
"Duration": "00:10:00",
"Thumbnail": "https://example.com/thumbnail.jpg",
"Caption_Status": "Not Available",
"Comments": {}}
}
```

**Example SQL Tables:**

![image](https://github.com/Nirmal-K-N/YouTube-Data-Harvesting/assets/150317924/c40b9a48-c4ba-4a2e-ac93-21c797d3a841)

![image](https://github.com/Nirmal-K-N/YouTube-Data-Harvesting/assets/150317924/12ec7d67-eed8-4f86-97d2-816ab7f8b25b)

![image](https://github.com/Nirmal-K-N/YouTube-Data-Harvesting/assets/150317924/a5894e80-18c7-4e9f-912f-ff22ac4229af)

![image](https://github.com/Nirmal-K-N/YouTube-Data-Harvesting/assets/150317924/3382e834-a475-42af-b970-47d90a55b6dd)

## Conclusion ##

Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
