from graph_funcs.load_data import load_data
from helper_funcs.connect_to_neptune import connect_to_neptune
from graph_funcs.shortest_path import shortest_path
from graph_funcs.community_detection import leiden_comm
import pandas as pd
import awswrangler as wr

port = 8182
server = "neptune-database-1.cluster-c7w9wbbkmgi2.us-east-2.neptune.amazonaws.com"
loading_endpoint = f"https://{server}:{port}/loader"

gremlin_endpoint = f"https://{server}:{port}/gremlin"

object1 = "s3://shrikant-neptune-testing/Data_02_09_2023/nodes.csv"
object2 = "s3://shrikant-neptune-testing/Data_02_09_2023/20230831_edges_neptune.csv"

print("Clearing Data on Neptune ...")


print("Loading Data to Neptune ....")

resp1 = load_data(source=object1, endpoint=loading_endpoint)
if resp1 == 200:
    print(f"{object1} loaded successfully")

resp2 = load_data(source=object2, endpoint=loading_endpoint)
if resp2 == 200:
    print(f"{object2} loaded successfully")

print("Reading Nodes Data ....")
df_nodes_list = pd.read_csv("data/nodes.csv")
df_nodes_list = df_nodes_list[["~id", "CustomerName", "~label", "BusinessGroupName"]]
df_nodes_list["~id"] = df_nodes_list["~id"].astype(str)

print("Reading Edges Data ....")
df_edges = pd.read_csv("data/edges.csv")
df_edges = df_edges[["~from" ,"~to", "weight:Int"]]
df_edges["~from"] = df_edges["~from"].astype(str)
df_edges["~to"] = df_edges["~to"].astype(str)

g = connect_to_neptune(endpoint=gremlin_endpoint)
df_nodes_list = shortest_path(g, df_nodes_list)

new_df = leiden_comm(df_edges)
new_df = pd.merge(new_df, df_nodes_list, on="~id", how="left")

print(df_nodes_list.head())
print(df_edges.head())
print(new_df.head())

print("Number of Unique Communities: ", len(pd.unique(new_df["component(single)"])))

client = wr.neptune.connect(server, port, iam_enabled=False)
res = wr.neptune.to_property_graph(client, new_df, use_header_cardinality=True, batch_size=100)

