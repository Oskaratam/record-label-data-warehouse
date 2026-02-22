
**File purpose**: Clearly define all the naming conventions for objects in the project  

**Objects**: file names, database tables/views/columns/procedures/triggers, docs
--------

# General Conventions #

***Naming style*** - snake case, with lower case letters with '_' underscore to separate the words 

***Language*** - English

***Script names*** - `<Abbreviated layer name>_<script purpose>.<extension>`  
  - 'Abbreviated layer name' reprsents shortened name of the data processing stage where the script is applied (e.g., bronze - brz, silver - slv, gold gd)  
  - Examples: brz_load_json.py, gd_ddl.sql

***Data quality test script names*** - `test_<layer name>_<aspect>`  
  - All test have to start with the prefix `test_` and be followed by layer name and then the tested aspect  
  - Example: test_silver_duplicates, test_gold_nulls

## Docs Conventions ##

***Visualization based docs*** - `<order>_<artifact_type>_<description>.<extension>`  
  - Order - level of abstraction (01 beeing the highest)  
  - Examples - 01_architecture_layers.drawio, 03_model_star_schema.png, 02_architecture_data_flow.drawio  

***Text based docs*** - `<descriptive name using the general naming style>.<extension>`  
  - Examples: model_evaluation.md, data_dictionary.md

***Standard Top-Level Files*** - `<descriptive name all caps>.<extension>`  
  - Examples: README.md, CONTRIBUTING.md, CHANGELOG.md

## Layer's Naming Conventions ##

### Bronze Layer ###
All names must start with the source system name, and table names must clearly represent the entity  
- **`<sourcesystem>_<entity>`**  
  - `<sourcesystem>`: Name of the source system (e.g., `spotify`, `youtube`).    
  Example: `spotify_tracks_historical` → Historical information about tracks from the Spotify source

### **Silver Rules**  
All names must start with the source system name, and table names must match their original names in bronze level without renaming   
- **`<sourcesystem>_<entity>`**  
    - `<sourcesystem>`: Name of the source system (e.g., `spotify`, `youtube`).  
    - `<entity>`: Exact table name from the bronze level.   
    Example: `youtube_music_playlist_info` → Information about youtube music playlists  

  ### **Gold Rules**  
All names must use meaningful, business-aligned names for tables, starting with the category prefix.  
- **`<category>_<entity>`**  
  - `<category>`: Describes the role of the table, such as `dim` (dimension) or `fact` (fact table).  
  - `<entity>`: Descriptive name of the table, aligned with the business domain (e.g., `videos`, `artists`, `tracks`).   
  Examples:  
    `dim_artists` → Dimension table for artist data.    
    `fact_genre_performance` → Fact table containing genre performance by months.  


## Column Naming Conventions

### **Technical Columns**   
All technical columns must start with the prefix `meta_`, followed by a descriptive name indicating the column's purpose.  
**`meta_<column_name>`**   
  - `meta`: Prefix exclusively for system-generated metadata.  
  - `<column_name>`: Descriptive name indicating the column's purpose.  
  Example: `meta_load_date` → System-generated column used to store the date when the record was loaded.  

  ### **Surrogate Keys**  
All primary keys in dimension tables must use the suffix `_key`.  
**`<table_name>_key`**  
  - `<table_name>`: Refers to the name of the table or entity the key belongs to.  
  - `_key`: A suffix indicating that this column is a surrogate key.   
  Example: `video_key` → Surrogate key in the `dim_videos` table.  
