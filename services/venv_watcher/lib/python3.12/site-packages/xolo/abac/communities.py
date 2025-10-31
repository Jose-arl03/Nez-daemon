# Algoritmos de detección de comunidades

import networkx as nx
from networkx.algorithms import community
from collections import defaultdict
from typing import Tuple,Dict,List


class CommunityDetector:

    def __init__(self):
        self.event_communities = {}
        self.event_to_community = {}
        self.policies_by_community = defaultdict(set)

    def detect_communities(self,graph: nx.Graph)->Tuple[Dict[str,int], Dict[str,int]]:
        """
        Detecta comunidades de eventos en el grafo usando Louvain
        y regresa:
        - Un diccionario evento_id -> comunidad_id
        - Y el mismo mapeo (event_to_community) para procesamiento posterior
        """
        communities = community.louvain_communities(graph, weight='weight', resolution=1)
        
        self.event_communities = {}
        self.event_to_community = {}
        for community_id, nodes in enumerate(communities):
            for node_id in nodes:
                self.event_communities[node_id] = community_id
                self.event_to_community[node_id] = community_id
        

        return self.event_communities, self.event_to_community

    def map_policies_to_communities(self,event_to_policy: Dict[str,str], event_to_community: Dict[str,int]) -> Dict[str,List[str]]:
        """
        Mapea políticas a comunidades basándose en los eventos.
        Cada política puede pertenecer a múltiples comunidades
        si tiene eventos distribuidos en distintas comunidades.
        """
        self.policies_by_community = defaultdict(set)
        for event_id, community_id in event_to_community.items():
            policy_id = event_to_policy.get(event_id)
            if policy_id is not None:
                self.policies_by_community[community_id].add(policy_id)

        # Convertir sets a listas para que sea JSON serializable
        self.policies_by_community = {cid: list(pids) for cid, pids in self.policies_by_community.items()}
        
        return self.policies_by_community
