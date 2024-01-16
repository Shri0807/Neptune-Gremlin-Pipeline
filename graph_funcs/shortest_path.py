from gremlin_python.process.graph_traversal import __, repeat, hasLabel, values, unfold, count, constant, inV, outE, simplePath, path

def shortest_path(g, df_nodes_list):
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
        
        print(result)
    return df_nodes_list