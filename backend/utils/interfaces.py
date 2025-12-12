from dataclasses import dataclass

@dataclass
class EndpointSpec:
    name: str
    index: int = 0

    @staticmethod
    def parse(value):
        """
        Normalise automatiquement :
          - "events"        → EndpointSpec("events", 0)
          - ("events", 1)   → EndpointSpec("events", 1)
          - ["events", 1]   → EndpointSpec("events", 1)
        """

        if isinstance(value, EndpointSpec):
            return value

        if isinstance(value, str):
            return EndpointSpec(name=value, index=0)

        if isinstance(value, (tuple, list)):
            if len(value) != 2:
                raise ValueError(f"❌ Format invalide : {value}. Format attendu : ('endpoint', index).")

            name, index = value

            if not isinstance(name, str):
                raise ValueError(f"❌ Le nom d’endpoint doit être une chaîne. Reçu : {type(name)}")
            
            if not isinstance(index, int):
                raise ValueError(f"❌ L'index doit être un entier. Reçu : {type(index)}")

            return EndpointSpec(name=name, index=index)

        raise TypeError(f"❌ dataEndpoint doit être str, tuple, list, ou EndpointSpec. Reçu : {type(value)}")

    # 🔹 Méthode pour retourner un tuple compatible _store()
    def to_tuple(self) -> tuple:
        return (self.name, self.index)

