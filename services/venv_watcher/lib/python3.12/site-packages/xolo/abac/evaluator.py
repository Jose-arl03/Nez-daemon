# core/evaluator_community.py

from typing import List, Dict
from xolo.abac.models import Policy, AccessRequest
from xolo.abac.matcher import EventMatcher
from xolo.abac.graph import GraphBuilder

class CommunityPolicyEvaluator:
    def __init__(self, policies: List[Policy] = [], policies_by_community: Dict[str, List[str]] = {}):
        self.policies: Dict[str, Policy] = {p.policy_id: p for p in policies}
        self.policies_by_community = policies_by_community

    def set_policies(self, policies: List[Policy] = []):
        self.policies = {p.policy_id: p for p in policies}

    def set_policies_by_community(self, policies_by_community: Dict[str, List[str]] = {}):
        self.policies_by_community = policies_by_community

    def run(self, policies: List[Policy] = [], policies_by_community: Dict[str, List[str]] = {}):
        self.set_policies(policies=policies)
        self.set_policies_by_community(policies_by_community=policies_by_community)

        # Cargar pesos dinámicos de atributos basados en entropía
        self.attribute_weights = GraphBuilder.calculate_attribute_weights(policies)

        # Precalcular eventos agrupados por comunidad
        self.events_by_community = {}
        for community_id, policy_ids in policies_by_community.items():
            events = []
            for pid in policy_ids:
                policy = self.policies.get(pid)
                if policy:
                    events.extend(policy.events)
            self.events_by_community[community_id] = events

    def remove_policy(self, policy_id: str):
        # Eliminar de diccionario de políticas
        self.policies.pop(policy_id, None)

        # Buscar y eliminar de su comunidad
        for community_id, policy_ids in self.policies_by_community.items():
            if policy_id in policy_ids:
                policy_ids.remove(policy_id)

                # Recalcular eventos de esa comunidad
                events = []
                for pid in policy_ids:
                    policy = self.policies.get(pid)
                    if policy:
                        events.extend(policy.events)
                self.events_by_community[community_id] = events
                break  # Solo puede estar en una comunidad

    def update_policy(self, updated_policy: Policy):
        pid = updated_policy.policy_id

        # Reemplazar en diccionario de políticas
        self.policies[pid] = updated_policy

        # Encontrar la comunidad a la que pertenece
        for community_id, policy_ids in self.policies_by_community.items():
            if pid in policy_ids:
                # Recalcular los eventos de esa comunidad
                events = []
                for other_pid in policy_ids:
                    policy = self.policies.get(other_pid)
                    if policy:
                        events.extend(policy.events)
                self.events_by_community[community_id] = events
                break
    
    def add_policy(self, policy: Policy):
        self.policies[policy.policy_id] = policy

        # Evaluar similitud con comunidades existentes
        best_community = None
        best_score = -1

        for community_id, policy_ids in self.policies_by_community.items():
            community_score = 0.0

            # Comparar eventos nuevos con eventos ya existentes en la comunidad
            for existing_pid in policy_ids:
                existing_policy = self.policies.get(existing_pid)
                if not existing_policy:
                    continue
                for existing_event in existing_policy.events:
                    for new_event in policy.events:
                        score = self._weighted_match_score(new_event, existing_event)
                        community_score += score

            if community_score > best_score:
                best_score = community_score
                best_community = community_id

        if best_community is None:
            # Si no se encontró comunidad, descartar o crear nueva (por ahora, ignoramos)
            return

        # Asociar política a la comunidad más afín
        self.policies_by_community[best_community].append(policy.policy_id)

        # Recalcular eventos de esa comunidad
        events = []
        for pid in self.policies_by_community[best_community]:
            p = self.policies.get(pid)
            if p:
                events.extend(p.events)
        self.events_by_community[best_community] = events


    def _weighted_match_score(self, event, request) -> float:
        score = 0.0

        def val(x): return x.value.strip().lower()

        if val(event.subject) == "*" or val(event.subject) == val(request.subject):
            score += self.attribute_weights.get("rol", 0)
        if val(event.asset) == "*" or val(event.asset) == val(request.asset):
            score += self.attribute_weights.get("recurso", 0)
        if val(event.space) == "*" or val(event.space) == val(request.space):
            score += self.attribute_weights.get("ubicacion", 0)
        if val(event.time) == "*" or val(event.time) == val(request.time):
            score += self.attribute_weights.get("rango_horario", 0)
        if val(event.action) == "*" or val(event.action) == val(request.action):
            score += self.attribute_weights.get("accion", 0)

        '''
        if val(event.time) == "*":
            score += self.attribute_weights.get("rango_horario", 0)
        elif "-" in val(event.time):
            try:
                start, end = val(event.time).split("-")
                h, m = map(int, val(request.time).split(":"))
                req_minutes = h * 60 + m
                start_minutes = int(start.split(":")[0]) * 60 + int(start.split(":")[1])
                end_minutes = int(end.split(":")[0]) * 60 + int(end.split(":")[1])
                if start_minutes <= req_minutes <= end_minutes:
                    score += self.attribute_weights.get("rango_horario", 0)
            except:
                pass
        '''
        return score

    def find_best_community(self, request: AccessRequest) -> str:
        best_community = None
        best_score = -1

        for community_id, policy_ids in self.policies_by_community.items():
            max_score_in_community = 0.0

            for pid in policy_ids:
                policy = self.policies.get(pid)
                if not policy:
                    continue

                for event in policy.events:
                    score = self._weighted_match_score(event, request)
                    if score > max_score_in_community:
                        max_score_in_community = score

            if max_score_in_community > best_score:
                best_score = max_score_in_community
                best_community = community_id

        return best_community

    def order_policies_by_similarity(self, community_id: str, request: AccessRequest) -> List[str]:
        similarity_scores = {}

        for pid in self.policies_by_community.get(community_id, []):
            policy = self.policies.get(pid)
            if not policy:
                continue
            max_score = 0
            for event in policy.events:
                score = self._weighted_match_score(event, request)
                if score > max_score:
                    max_score = score
            similarity_scores[pid] = max_score

        sorted_policies = sorted(similarity_scores.keys(), key=lambda pid: similarity_scores[pid], reverse=True)
        return sorted_policies

    def evaluate(self, request: AccessRequest) -> dict:
        best_community = self.find_best_community(request)

        if best_community is None:
            return {
                "result": "deny",
                "policy_id": None,
                "event_id": None
            }

        sorted_policies = self.order_policies_by_similarity(best_community, request)

        for idx, pid in enumerate(sorted_policies, 1):
            policy = self.policies.get(pid)
            if not policy:
                continue

            for event in policy.events:
                if EventMatcher.match_event(event, request):
                    return {
                        "result": policy.effect,
                        "policy_id": policy.policy_id,
                        "event_id": event.event_id
                    }

        return {
            "result": "deny",
            "policy_id": None,
            "event_id": None
        }
