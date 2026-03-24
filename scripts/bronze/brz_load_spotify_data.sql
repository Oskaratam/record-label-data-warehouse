CREATE OR ALTER PROCEDURE bronze.load_incremental_spotify AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME,
			@start_batch_time DATETIME, @end_batch_time DATETIME,
			@watermark DATE

	PRINT '================================================';
	PRINT 'Loading Spotify Data using Incremental Load strategy';
	PRINT '================================================';

	PRINT '------------------------------------------------';
	PRINT 'Loading 2025 data ';
	PRINT '------------------------------------------------';

	SET @Watermark = (  SELECT TOP 1 
						watermark_value
						FROM bronze.ctrl_load
						WHERE target_table_name = 'bronze.spotify_data'
							AND load_status = 'Success'
							AND watermark_value IS NOT NULL
						ORDER BY finish_time DESC);
	SET @start_time = GETDATE()

	/*
	Next steps
	1.Bulk insert into staging table
	2.Then create select into, while filtering out using watermark

	*/
	BEGIN TRY 

		CREATE TABLE #staging_spotify_data (
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
			meta_is_proccessed BIT DEFAULT 0 
		);

		BULK INSERT #staging_spotify_data 
		FROM "C:/REPOS/DataProjects/music-label-data-warehouse/data/source_spotify/data_2025.csv"
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			TABLOCK
		)
		SET @end_time = GETDATE()
		PRINT 'Loaded 2025 spotify data in: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'


		SELECT * FROM #staging_spotify_data


		DROP TABLE #staging_spotify_data 
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SPOTIFY DATA'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END; 

bronze.load_incremental_spotify