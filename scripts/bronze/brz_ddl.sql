/*
Layer: BRONZE

Script Purpose: to define the structure
and create all objects of the bronze layer of the warehouse

WARNING: Runing the script will delete and recreate existing tables
with the same namee
*/

IF OBJECT_ID('bronze.ctrl_load', 'U') IS NOT NULL
	DROP TABLE bronze.ctrl_load;
GO
CREATE TABLE bronze.ctrl_load (
	load_key BIGINT IDENTITY(1, 1) PRIMARY KEY,
	source_system NVARCHAR(255),
	load_status NVARCHAR(50),
	target_table_name NVARCHAR(128),
	loaded_rows INT,
	watermark_value NVARCHAR(128),
	start_time DATETIME2,
	finish_time DATETIME2,
	load_time_seconds FLOAT,
	error_message NVARCHAR(1000) NULL
)

IF OBJECT_ID('bronze.spotify_data', 'U') IS NOT NULL
	DROP TABLE bronze.spotify_data;
GO
CREATE TABLE bronze.spotify_data (
	track_id NVARCHAR(50),
	track_name NVARCHAR(100),
	track_number INT, 
	track_popularity INT,
	explicit NVARCHAR(50),
	artist_name NVARCHAR(100),
	artist_popularity INT,
	artist_followers INT,
	artist_genres NVARCHAR(100),
	album_id NVARCHAR(50),
	album_name NVARCHAR(50),
	album_release_date DATE, 
	album_total_tracks INT,
	album_type NVARCHAR(50),
	track_duration_min DECIMAL(4, 3),
	meta_ingested_at DATETIME2 DEFAULT SYSUTCDATETIME(),
	meta_source_system NVARCHAR(255),
	meta_is_proccessed BIT DEFAULT 0  --whether was proccessed by silver layer
)

IF OBJECT_ID('bronze.spotify_tracks_historical', 'U') IS NOT NULL
	DROP TABLE bronze.spotify_tracks_historical;
GO
CREATE TABLE bronze.spotify_tracks_historical (
	track_id NVARCHAR(50),
	track_name NVARCHAR(100),
	track_number INT, 
	track_popularity INT,
	track_duration_ms INT,
	explicit NVARCHAR(50),
	artist_name NVARCHAR(100),
	artist_popularity INT,
	artist_followers INT,
	artist_genres NVARCHAR(100),
	album_id NVARCHAR(50),
	album_name NVARCHAR(50),
	album_release_date DATE, 
	album_total_tracks INT,
	album_type NVARCHAR(50),
	meta_ingested_at DATETIME2 DEFAULT SYSUTCDATETIME(),
	meta_source_system NVARCHAR(255),
	meta_is_proccessed BIT DEFAULT 0 
)
GO

IF OBJECT_ID('bronze.youtube_videos', 'U') IS NOT NULL
	DROP TABLE bronze.youtube_videos;
GO
CREATE TABLE bronze.youtube_videos (
	video_key INT IDENTITY(1, 1) PRIMARY KEY,  
	raw_content NVARCHAR (1000), -- Raw JSON 
	meta_ingested_at DATETIME2 DEFAULT SYSUTCDATETIME(),
	meta_source_system NVARCHAR(255),
	meta_is_proccessed BIT DEFAULT 0 
)


IF OBJECT_ID('bronze.youtube_playlists', 'U') IS NOT NULL
	DROP TABLE bronze.youtube_playlists;
GO
CREATE TABLE bronze.youtube_playlists (
	playlist_key INT IDENTITY(1, 1) PRIMARY KEY,
	raw_content NVARCHAR (1000),
	meta_ingested_at DATETIME2 DEFAULT SYSUTCDATETIME(),
	meta_source_system NVARCHAR(255),
	meta_is_proccessed BIT DEFAULT 0 
)

IF OBJECT_ID('bronze.kworb_chart_worldwide', 'U') IS NOT NULL
	DROP TABLE bronze.kworb_chart_worldwide;
GO
CREATE TABLE bronze.kworb_chart_worldwide (
	page_key INT IDENTITY(1, 1) PRIMARY KEY,
	raw_content NVARCHAR (MAX), -- Raw scraped html,
	meta_ingested_at DATETIME2 DEFAULT SYSUTCDATETIME(),
	meta_source_system NVARCHAR(255),
	meta_is_proccessed BIT DEFAULT 0 
)

IF OBJECT_ID('bronze.kworb_chart_worldwide', 'U') IS NOT NULL
	DROP TABLE bronze.kworb_chart_worldwide;
GO
CREATE TABLE bronze.kworb_chart_worldwide (
	page_key INT IDENTITY(1, 1) PRIMARY KEY,
	raw_content NVARCHAR (MAX), -- Raw scraped html,
	meta_ingested_at DATETIME2 DEFAULT SYSUTCDATETIME(),
	meta_source_system NVARCHAR(255),
	meta_is_proccessed BIT DEFAULT 0 
)

