import igraph as ig
import pandas as pd

def leiden_comm(**kwargs):
    """
    Description:
        Function to compute leiden community detection on Graph and assign cluster_id to each node
        Ensure the weights column to be of type integer
    
    Parameters:
        edges_local_path: Path to the edges data directory
        preprocessed_edges_file_name: Preprocessed Edges File Name

    Return:
        new_df: Dataframe with Nodes and Cluster_id assigned to each node by leiden community detection algorithm
    """

    edges_path = kwargs["edges_local_path"] + kwargs["preprocessed_edges_file_name"]

    df = pd.read_csv(edges_path) 

    df['weight:Int'] = df['weight:Int'].astype(int)


    # Create a graph from the results returned
    g = ig.Graph.TupleList(df.itertuples(index=False), directed=False, weights=True)

    leiden_community = g.community_leiden(weights='weight', resolution=0.6)

    rows=[]
    all_coms = leiden_community.subgraphs()
    for idx, c in enumerate(leiden_community):
        for item in c:
            rows.append({'~id': str(g.vs[item]['name']), 'component(single)': idx})

    for comm in all_coms:
        for idx, v in enumerate(comm.vs):
            pg = comm.pagerank()        
            r = next(s for s in rows if s['~id'] == v['name'])
            r['pg(single)']  = pg[idx]
    
    new_df=pd.DataFrame(rows, columns=['~id','component(single)', 'pg(single)'])
    new_df["~id"] = new_df["~id"].astype(str)

    return new_df
    