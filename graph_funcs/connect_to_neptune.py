from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __, repeat, hasLabel, values, unfold, count, constant, inV, outE, simplePath, path
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.aiohttp.transport import AiohttpTransport
from gremlin_python.process.traversal import *

def connect_to_neptune(endpoint):
    """
    Description:
        A function to establish remote connection to Amazon Neptune Graph Database and return the graph traversal source

    Parameters:
        endpoint: URL of Neptune Graph DB
    
    Returns:
        g: Graph Traversal source
    """
    # connect to Neptune DB instance
    graph=Graph()
    connection = DriverRemoteConnection(endpoint, 'g',
        transport_factory=lambda:AiohttpTransport(call_from_event_loop=True)
    )
    g = graph.traversal().withRemote(connection)

    return g