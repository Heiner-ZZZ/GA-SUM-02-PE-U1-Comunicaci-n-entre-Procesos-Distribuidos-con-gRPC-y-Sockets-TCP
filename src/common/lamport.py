from __future__ import annotations

import threading


class LamportClock:
    def __init__(self, node_name: str, initial_value: int = 0) -> None:
        if initial_value < 0:
            raise ValueError("El valor inicial del reloj no puede ser negativo.")
        self.node_name = node_name
        self._value = int(initial_value)
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_timestamp: int) -> int:
        if received_timestamp < 0:
            raise ValueError("El timestamp recibido no puede ser negativo.")
        with self._lock:
            # Regla de Lamport al recibir: max(local, recibido) + 1
            self._value = max(self._value, int(received_timestamp)) + 1
            return self._value

    def reset(self, value: int = 0) -> int:
        if value < 0:
            raise ValueError("El valor de reinicio no puede ser negativo.")
        with self._lock:
            self._value = int(value)
            return self._value

    def __repr__(self) -> str:
        return f"LamportClock(node_name={self.node_name!r}, value={self.value})"
