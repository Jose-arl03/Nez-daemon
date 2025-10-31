import networkx as nx
from xolo.abac.models import Event,Policy
from typing import List
from collections import Counter
import numpy as np


class GraphBuilder:

    def __init__(self):
        self.graph = None

    def event_similarity(self,e1:Event, e2:Event, weights: dict) -> float:
        """
        Calcula la similitud entre dos eventos basada en la suma
        de los pesos de los atributos coincidentes.
        """
        score = 0.0
        if e1.subject.value.strip().lower() == e2.subject.value.strip().lower():
            score += weights.get("rol", 0)
        if e1.asset.value.strip().lower() == e2.asset.value.strip().lower():
            score += weights.get("recurso", 0)
        if e1.space.value.strip().lower() == e2.space.value.strip().lower():
            score += weights.get("ubicacion", 0)
        if e1.time.value.strip().lower() == e2.time.value.strip().lower():
            score += weights.get("rango_horario", 0)
        if e1.action.value.strip().lower() == e2.action.value.strip().lower():
            score += weights.get("accion", 0)
        return score


    def build_event_graph(self,policies: List[Policy], similarity_threshold: float = 0.3) -> nx.Graph:
        """
        Construye un grafo de eventos, conectando solo eventos cuya
        similitud supere un umbral especificado.
        """
        G = nx.Graph()
        attribute_weights = self.calculate_attribute_weights(policies)

        # Imprimir los pesos de atributos
        # print("\nPesos de atributos basados en entropía de Shannon:")
        # for attr, weight in attribute_weights.items():
            # print(f"   {attr}: {weight:.4f}")

        for policy in policies:
            for event in policy.events:
                G.add_node(event.event_id, event=event, policy_id=policy.policy_id)

        event_ids = list(G.nodes)

        for i in range(len(event_ids)):
            for j in range(i + 1, len(event_ids)):
                e1 = G.nodes[event_ids[i]]["event"]
                e2 = G.nodes[event_ids[j]]["event"]
                sim = self.event_similarity(e1, e2, attribute_weights)
                if sim >= similarity_threshold:
                    G.add_edge(e1.event_id, e2.event_id, weight=sim)

        self.graph = G
        return self.graph
        # return G

    @staticmethod
    def calculate_attribute_weights(policies: list[Policy]) -> dict:
        """
        Calcula pesos dinámicos para los atributos basado en su entropía (diversidad) en las políticas.
        """
        attribute_values = {
            "rol": [],
            "recurso": [],
            "ubicacion": [],
            "rango_horario": [],
            "accion": []
        }

        for policy in policies:
            for event in policy.events:
                attribute_values["rol"].append(event.subject.value)
                attribute_values["recurso"].append(event.asset.value)
                attribute_values["ubicacion"].append(event.space.value)
                attribute_values["rango_horario"].append(event.time.value)
                attribute_values["accion"].append(event.action.value)

        entropies = {}
        for attribute, values in attribute_values.items():
            if not values:
                entropies[attribute] = 0
                continue

            counts = Counter(values)
            probabilities = [freq / len(values) for freq in counts.values()]
            entropy = -sum(p * np.log2(p) for p in probabilities if p > 0)
            entropies[attribute] = entropy

        total_entropy = sum(entropies.values()) or 1  # Evitar división entre cero
        weights = {attr: entropies[attr] / total_entropy for attr in entropies}

        return weights
