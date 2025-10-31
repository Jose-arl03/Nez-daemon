# xolo/abac/matcher.py

from xolo.abac.models import Event, AttributeComponent,AccessRequest
# from abac.core.request import AccessRequest
from datetime import datetime


class EventMatcher:
    @staticmethod
    def is_time_in_range(policy_range: str, request_time: str) -> bool:
        try:
            start_str, end_str = policy_range.split("-")
            start_time = datetime.strptime(start_str.strip(), "%H:%M").time()
            end_time = datetime.strptime(end_str.strip(), "%H:%M").time()
            req_time = datetime.strptime(request_time.strip(), "%H:%M").time()
            return start_time <= req_time <= end_time
        except ValueError:
            return False

    @staticmethod
    def match_component(event_attr: AttributeComponent, request_attr: AttributeComponent) -> bool:
        """
        Compara dos AttributeComponent: uno del evento y otro de la solicitud.
        Soporta el uso de '*' como comodín que acepta cualquier valor.
        """
        attr_event = event_attr.attribute.strip().lower()
        val_event = event_attr.value.strip().lower()
        attr_req = request_attr.attribute.strip().lower()
        val_req = request_attr.value.strip().lower()

        # Si el atributo no es el mismo, no se compara
        if attr_event != attr_req:
            return False

        # Soporte para comodín '*'
        if val_event == "*":
            return True

        # Comparación especial para el tiempo
        if attr_event == "rango_horario":
            return EventMatcher.is_time_in_range(val_event, val_req)

        # Comparación exacta
        return val_event == val_req

    @staticmethod
    def match_event(event: Event, request: AccessRequest) -> bool:
        return (
            EventMatcher.match_component(event.subject, request.subject) and
            EventMatcher.match_component(event.asset, request.asset) and
            EventMatcher.match_component(event.space, request.space) and
            EventMatcher.match_component(event.time, request.time) and
            EventMatcher.match_component(event.action, request.action)
        )
