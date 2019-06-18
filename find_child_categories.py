
def dijkstra(adjacency_lists, source_index, neigbor_distance=lambda x,y: 1):
    Q = set(range(len(adjacency_lists)))
    dist = [float('inf')] * len(adjacency_lists)
    prev = [None] * len(adjacency_lists)

    dist[source_index] = 0
    while Q:
        if len(Q) % 1000 == 0:
            print(f"{len(Q)} of {len(adjacency_lists)} remaining ...")
        u = None
        for q in Q:
            if u is None or dist[u] > dist[q]:
                u = q
        Q.remove(u)
        if dist[u] == float('inf'):
            # no connection to any remaining element in Q found
            break

        for v in adjacency_lists[u]:
            alt = dist[u] + neigbor_distance(u, v)
            if alt < dist[v]:
                dist[v] = alt
                prev[v] = u
    return dist, prev


def read_graph(categories_filename):
    parent_child = {}
    with open(categories_filename, "r", encoding="utf-8") as category_file:
        for line in category_file:
            child, parent = line.rstrip("\n").split("\t", 1)
            if parent not in parent_child:
                parent_child[parent] = [child]
            else:
                parent_child[parent].append(child)
    inverted_index = list(parent_child)
    index = {c: idx for idx, c in enumerate(inverted_index)}
    adjacency_lists = [[]] * len(index)
    for c, idx in index.items():
        adjacency_lists[idx] = [index[child] for child in parent_child[c] if child in index]
    del parent_child
    return adjacency_lists, index, inverted_index


def persist_shortest_paths(start, distances, predecessors, inverted_index, output_file):
    def get_path(i, max_depth):
        if max_depth <= 0:
            assert distances[i] == 0
            return [start]
        return get_path(predecessors[i], max_depth-1) + [inverted_index[i]]

    indexed_non_inf_dist = list(filter(lambda p: not p[1] == float('inf'), enumerate(distances)))
    for i, distance in indexed_non_inf_dist:
        destination = inverted_index[i]
        output_file.write(f"{start}\t{destination}\t{distance}\t")

        output_file.write("\t".join(get_path(i, max_depth=distance)[1:][:-1]))

        output_file.write("\n")
    print()


def run(categories_filename, output_filename, start_nodes):
    adjacency_lists, index, inverted_index = read_graph(categories_filename=categories_filename)

    with open(output_filename, "w", encoding="utf-8") as output_file:
        for start in start_nodes:
            print(f"running dijkstra to find shortest path for subcategories of {start}")
            distances, predecessors = dijkstra(adjacency_lists, index[start])
            persist_shortest_paths(start=start,
                                   distances=distances,
                                   predecessors=predecessors,
                                   inverted_index=inverted_index,
                                   output_file=output_file)


if __name__ == '__main__':
    import sys
    assert len(sys.argv) > 4, "USAGE: python find_child_categories.py [CATEGORY_FILE] [OUTPUT_FILE] [START_NODES...]"
    run(categories_filename=sys.argv[1], output_filename=sys.argv[2], start_nodes=sys.argv[3:])
