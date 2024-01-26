from gremlin_python.process.graph_traversal import __, repeat, hasLabel, values, unfold, count, constant, inV, outE, simplePath, path
from connect_to_neptune import connect_to_neptune
import pandas as pd

def shortest_path(**kwargs):
    """
    Description:
        A function to calculate the shortest paths to a specified destination label ('Bankruptcy')
        for nodes in a Neptune Database using the Gremlin query language.
    
    Parameters:
        server: Server Name of Neptune Database
        port: Port Number of Neptune Database (Default=8182)
        gremlin_endpoint: Endpoint for Gremlin queries on the Neptune Database
        nodes_local_path: Path to the nodes data directory
        preprocessed_nodes_file_name: Name of the preprocessed nodes CSV file
    
    Returns:
        df_nodes_list: DataFrame containing node information with an added
                      'Shortest_Path' column indicating the calculated shortest paths.
    """

    gremlin_endpoint = f"https://{kwargs['server']}:{int(kwargs['port'])}/{kwargs['gremlin_endpoint']}"
    nodes_path = kwargs["nodes_local_path"] + kwargs["preprocessed_nodes_file_name"]

    g = connect_to_neptune(gremlin_endpoint)
    df_nodes_list = pd.read_csv(nodes_path)

    all_nodes = g.V().hasLabel("PERSON_PARENT_1111", "PERSON_MEMBER_1111").toList()

    for nodes in all_nodes:
        print(nodes.id)
        query = g.V().has("~id", str(nodes.id)). \
                repeat(__.outE().inV().simplePath()). \
                until(hasLabel('Bankruptcy')). \
                path().as_('p'). \
                map(__.select('p').unfold().values('CustomerName').fold()).as_('customerPath'). \
                select('customerPath')
        try:
            result = query.next()
            result = "_".join(result)
        except Exception as e:
            print(e.__class__.__name__)
            result = "No Path Found"
        
        df_nodes_list.loc[df_nodes_list["~id"] == str(nodes.id), "Shortest_Path"] = result
    return df_nodes_list