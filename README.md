# ML_Project1
YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit
## Description

YouTube Data Harvesting and Warehousing is a project designed to collect and store YouTube data efficiently using both SQL and MongoDB databases. The project includes a Streamlit web application that allows users to explore and analyze the collected data easily.

### Features

- **Data Harvesting:** Utilizes YouTube API to fetch information such as video details, channel statistics, and user interactions.
- **Dual Database Support:** Stores data in both SQL and MongoDB databases for flexibility and performance.
- **Streamlit Interface:** Provides an intuitive Streamlit interface for users to query, visualize, and gain insights into the collected data.
- **Data Exploration:** Supports various queries, including top-viewed videos, most active channels, and more.

### Technologies Used

- Python
- MySQL for SQL database
- MongoDB for NoSQL database
- Streamlit for the web application

## Installation

To set up the project locally, follow the installation instructions in the [Installation](#installation) section.

## Usage

1. Configure the database connections in the respective configuration files.
2. Run the Streamlit app to interact with the collected YouTube data.

## Folder Structure

- youtube.py: Contains the source code files for data collection and the Streamlit app.
- `data/`: Stores any sample or processed data files.
- `config/`: Configuration files for database connections.

## Database Schema

### SQL Database

- `videos` table: title, channel_name, views, likes, dislikes, comments, ...
- `comments` table: comment_id, video_id, comment_text, comment_published_At...
- `channels` table: channel_id, channel_name, subscriber, total videos...
- `playlists` table: channel_id, channel_name, playlist_name, title, video_count...
### MongoDB Database

- Collections: videos, channels, ...
