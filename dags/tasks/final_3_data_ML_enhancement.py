import pandas as pd

def enhance_unique_songs_with_ml(config, reduction_method='TSNE', n_components=2, plot=False,
                                elbow_plot=False, debug=True):
    """
    Enhance songs DataFrame by adding ML features: principal components and cluster labels.
    """
    # Normalize the feature space
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import MinMaxScaler

    print("Enhancing unique songs DataFrame with ML features...")
    datalake_dir = config['paths']['datalake_dir']
    songs = pd.read_parquet(f"{datalake_dir}combined/unique_songs.parquet")

    audio_features = ['acousticness', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness',
                      'speechiness', 'tempo', 'valence', 'energy_efficiency', 'emotional_impact']
    # drop rows with missing values because there are not many of them
    if debug:
        print("Dropping rows with missing values for audio features : ")
        print(songs[audio_features].isnull().sum())
        print("Dataframe has shape : ", songs.shape)
        # check top 5 value _counts of track_name
        #print("Top 5 value counts of track_name : ")
        #print(songs['track_name'].value_counts().head(50))
    songs = songs.dropna(subset=audio_features)

    scaler = MinMaxScaler()
    features_scaled = scaler.fit_transform(songs[audio_features])
    if elbow_plot:
        import matplotlib.pyplot as plt
        kmeans_elbow_method(KMeans, features_scaled, plt)
        return
    # Apply dimensionality reduction
    if reduction_method == 'PCA':
        from sklearn.decomposition import PCA
        reducer = PCA(n_components=n_components)
    elif reduction_method == 'TSNE':
        from sklearn.manifold import TSNE
        reducer = TSNE(n_components=n_components, learning_rate='auto', random_state=42)
    elif reduction_method == 'ISOMAP':
        from sklearn.manifold import Isomap
        reducer = Isomap(n_components=n_components, n_neighbors=5)
    components = reducer.fit_transform(features_scaled)

    # Add components to DataFrame
    for i in range(n_components):
        songs[f'Component_{i + 1}'] = components[:, i]

    # Apply clustering
    n_clusters = 7 #4,5
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)

    songs['Cluster'] = kmeans.fit_predict(features_scaled)
    if plot:
        import matplotlib.pyplot as plt
        plot_clusters_and_genres(songs, plt)
    save_dir = f"{datalake_dir}combined/"
    songs.to_parquet(f"{save_dir}unique_songs_ml_enhanced.parquet", index=False)
    print(f"Unique songs DataFrame enhanced with {n_components} components and {n_clusters} clusters")

def kmeans_elbow_method(KMeans, X, plt, range_clusters=(2, 20)):

    inertias = []
    clusters = range(range_clusters[0], range_clusters[1] + 1)

    for k in clusters:
        model = KMeans(n_clusters=k, random_state=42)
        model.fit(X)
        inertias.append(model.inertia_)

    plt.figure(figsize=(10, 6))
    plt.plot(clusters, inertias, 'bo-')
    plt.xlabel('Number of Clusters')
    plt.ylabel('Inertia')
    plt.title('K-Means Elbow Method')
    plt.xticks(clusters)
    plt.grid(True)
    plt.show()

def plot_clusters_and_genres(df, plt, component_cols=['Component_1', 'Component_2'], cluster_col='Cluster',
                             genre_col='genre'):
    # Ensure the components and cluster columns exist
    if not set(component_cols).issubset(df.columns) or cluster_col not in df.columns or genre_col not in df.columns:
        raise ValueError("DataFrame must contain the specified 'component' and 'cluster' columns")
    from seaborn import color_palette
    # Create a color palette for clusters
    cluster_palette = color_palette('hsv', n_colors=len(df[cluster_col].unique()))

    # Prepare a set of markers, one for each genre
    markers = ['o', 's', 'X', '^', 'v', '>', '<', 'p', '*', '+', 'x', 'D']
    genre_list = df[genre_col].unique()
    if len(genre_list) > len(markers):
        raise ValueError("Not enough marker types for the number of genres")

    # Map genres to markers
    genre_marker_dict = dict(zip(genre_list, markers))

    # Plot
    plt.figure(figsize=(10, 8))
    for cluster in df[cluster_col].unique():
        for genre in genre_list:
            # Filter data by cluster and genre
            cluster_genre_df = df[(df[cluster_col] == cluster) & (df[genre_col] == genre)]
            plt.scatter(
                cluster_genre_df[component_cols[0]],
                cluster_genre_df[component_cols[1]],
                s=100,
                alpha=0.75,
                c=[cluster_palette[cluster]],
                marker=genre_marker_dict[genre],
                label=f'Cluster {cluster}, Genre {genre}' if not cluster_genre_df.empty else "")

    plt.title('Clusters and Genres in Reduced Space')
    plt.xlabel(component_cols[0])
    plt.ylabel(component_cols[1])
    #plt.legend(title='Clusters and Genres', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    from dags.tasks.tools.general import *
    from dags import data_params
    config = get_config()
    enhance_unique_songs_with_ml(config, plot=False)

