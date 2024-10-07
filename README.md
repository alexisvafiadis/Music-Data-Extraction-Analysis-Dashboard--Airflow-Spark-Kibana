# Music Data Analysis Pipeline and Dashboard
![Spotify Icon](link-to-spotify-icon) ![Last.fm Icon](link-to-lastfm-icon)

## Introduction
This project focuses on analyzing music data to uncover trends in the industry, identify listener preferences across different countries, and explore factors contributing to artists' success. The data pipeline ingests and processes music data from two major platforms—**Spotify** and **Last.fm**—to combine insights from both sources.

The project showcases a complete end-to-end data pipeline that performs ingestion, transformation, augmentation, and visualization of music data. The goal is to demonstrate the capabilities of big data technologies for real-time analysis and visual representation of large-scale data.

## Features & Functionality
- **Data Ingestion**: Extracts top tracks and related information (audio features, popularity, listener counts) from Spotify and Last.fm APIs for 15 different countries.
- **Data Enrichment**: Enhances the Last.fm data with additional audio features from Spotify.
- **Data Transformation**: Formats and normalizes data using scalable processing tools, preparing it for analysis.
- **Data Augmentation**: Augments the datasets with artist and album details for comprehensive analysis.
- **Machine Learning Enhancement**: Applies clustering and dimensionality reduction techniques to uncover hidden patterns in the music data.
- **Dashboard Visualizations**: Presents insights on music trends, genre performance, and artist success via a Kibana dashboard.

## Project Structure
The project is organized into several key steps within the data pipeline:

### 1. Data Extraction:
- Extracts top tracks data from Spotify and Last.fm APIs.
- Stores raw data in Parquet format.

### 2. Data Enrichment:
- Augments the Last.fm data with audio features from Spotify.
- Ensures both datasets are comparable.

### 3. Data Transformation:
- Uses Apache Spark to clean and normalize the data, applying transformations such as scaling audio features and calculating new fields like track age and popularity categories.

### 4. Cross-Source Data Augmentation:
- Retrieves additional information about artists and albums from Spotify.
- Generates separate datasets for artists and albums.

### 5. Data Combination:
- Combines the extracted and enriched data sources using joins and aggregations.
- Generates multiple datasets, including:
  - **Unique Songs Dataset**: Aggregated view of all unique songs.
  - **Top Charts Dataset**: Combines Spotify and Last.fm top charts.
  - **Genre Performance Dataset**: Analyzes the performance of each genre across platforms.
  - **Artist Performance Dataset**: Evaluates artists based on their global and recent performance.

### 6. Machine Learning Enhancement:
- Applies KMeans clustering and t-SNE dimensionality reduction to group songs and reveal hidden relationships between genres.

### 7. Indexing & Visualization:
- Indexes the processed data into Elasticsearch for easy retrieval and visualization.
- Visualized in a Kibana dashboard, including charts, tables, and interactive maps.

## Technologies Used
- **Python**: For scripting and API interactions (Spotify, Last.fm).
- **Apache Airflow**: For orchestrating the end-to-end data pipeline.
- **Apache Spark**: For processing and transforming the large extracted datasets efficiently.
- **Elasticsearch**: For indexing data, which is used by the Kibana Dashboard for querying.
- **Kibana**: For visualizing insights through various charts and interactive maps.
- **Pandas**: For initial data handling and preprocessing.
- **Scikit-Learn**: For applying machine learning algorithms for dimensionality reduction and clustering.

## APIs Used
- [Spotify API](https://developer.spotify.com/documentation/web-api/)
- [Last.fm API](https://www.last.fm/api)

## Video, Screenshots & Illustrations

### DAG Structure:
![DAG Structure](link-to-dag-image)

### Kibana Dashboard:
- **Song Trends & Taste**  
  ![Song Trends Image](images/trends)
  
- **Genre Traits & Appeal**  
  ![Genre Traits Image](link-to-genre-traits-image)
  
- **Artist Success Factors**  
  ![Artist Success Image](link-to-artist-success-image)

### Presentation Video:
[Watch the presentation video]([youtube-link](https://youtu.be/H_xVGKHc3zY))
