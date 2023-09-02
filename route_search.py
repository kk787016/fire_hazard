import heapq

class route_search:

    def __init__(self):
        super().__init__()

    def search_path(self, graph, first):
        distance = {node: [float('inf'), first] for node in graph}
        distance[first] = [0, first]
        queue = []

        heapq.heappush(queue, [distance[first][0], first])

        while queue:
            current_distance, current_node = heapq.heappop(queue)

            if distance[current_node][0] < current_distance:
                continue

            for next_node, weight in graph[current_node].items():
                total_distance = current_distance + weight

                if total_distance < distance[next_node][0]:
                    # 다음 노드까지 총 거리와 어떤 노드를 통해서 왔는지 입력
                    distance[next_node] = [total_distance, current_node]
                    heapq.heappush(queue, [total_distance, next_node])
        # 마지막 노드부터 첫번째 노드까지 순서대로 출력
        path_list = []

        min_distance = []
        min_distance.append(distance['escape00'])
        min_distance.append(distance['escape01'])

        min_escape_num = min_distance.index(min(min_distance))
        last = 'escape' + str(min_escape_num).zfill(2)

        path = last

        path_list.append(last)
        while distance[path][1] != first:
            path_list.append(distance[path][1])
            path = distance[path][1]
        path_list.append(first)
        path_list.reverse()

        return path_list

    def search(self, node, start_node):
        return self.search_path(node, start_node)
