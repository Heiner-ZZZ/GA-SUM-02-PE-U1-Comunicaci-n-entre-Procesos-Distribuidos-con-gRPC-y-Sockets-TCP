# GA-SUM-02 / PE-U1 — Comunicación entre Procesos Distribuidos con gRPC y Sockets TCP

**Universidad Técnica Estatal de Quevedo (UTEQ)**  
**Carrera:** Ingeniería en Software (Rediseño) — 7mo Nivel, Paralelo "A"  
**Asignatura:** Aplicaciones Distribuidas  
**Docente:** Dr. Gleiston Cicerón Guerrero Ulloa  
**Equipo:** EJPZ

| Integrante | Usuario GitHub | Rol en la práctica |
|---|---|---|
| Heiner Ariel Zambrano Ronquillo | `@Heiner-ZZZ` | Núcleo Lamport/configuración, protocolo experimental, discusión PFC-CAP y compilación |
| Luis Jeremy Jaramillo Acosta | `@JeremyJaramillo72` | Implementación Sockets TCP, arquitectura TikZ, fundamento SD/ANSA y documentación |
| Kenny Isaías España Vera | `@Kespanav` | Contrato Protobuf, implementación gRPC, fundamento fallos/seguridad y QA reproducible |
| Luis Alberto Paca Pilataza | `@Luispaca2002` | Motor estadístico, gráficos 300 DPI, verificación real de Lamport y conclusiones |

---

## Descripción

Práctica experimental orientada a implementar y comparar empíricamente dos mecanismos de comunicación entre procesos distribuidos en una topología de **tres nodos** (`nodo-A` en `127.0.0.1:5000`, `nodo-B` en `127.0.0.1:5001` y `nodo-C` en `127.0.0.1:5002`):

1. **Sockets TCP (`SOCK_STREAM`)** con entramado (*framing*) explícito y serialización JSON `{sender, timestamp, message}`.
2. **gRPC sobre HTTP/2 con Protocol Buffers** definido en `/proto/messaging.proto`.

Ambos mecanismos implementan **relojes lógicos de Lamport**, ejecutan 20 intercambios causales y se evalúan bajo el mismo protocolo experimental (corrida de calentamiento descartada, al menos 5 repeticiones de 100 envíos para mensajes de 100 bytes y 10 KB, y prueba con los tres clientes en concurrencia).

---

## Estructura del repositorio

```text
/
├── README.md                      # Documentación de ejecución, video y compilación LaTeX
├── BITACORA.md                    # Registro cronológico de sesiones, problemas y soluciones
├── requirements.txt               # Dependencias exactas (Python 3.11+)
├── proto/                         # Contrato Protobuf (messaging.proto)
├── src/
│   ├── common/
│   │   ├── lamport.py             # Reloj lógico de Lamport thread-safe
│   │   ├── config.py              # Definición de nodo-A/B/C, payloads (100 B, 10 KB) y esquemas CSV
│   │   └── machine_info.py        # Registro de SO, CPU, RAM y versión de Python
│   ├── sockets/
│   │   ├── framing.py             # Entramado TCP: prefijo de longitud de 4 bytes + JSON
│   │   ├── server.py              # Servidor TCP multihilo con reloj de Lamport
│   │   └── client.py              # 20 intercambios Lamport + benchmark RTT (100 B / 10 KB, concurrencia)
│   └── grpc/
│       ├── messaging_pb2*.py      # Stubs generados con grpcio-tools desde proto/messaging.proto
│       ├── server_grpc.py         # Servidor gRPC con reloj de Lamport
│       └── client_grpc.py         # Mismo protocolo que el cliente TCP, sobre gRPC
├── data/                          # CSV crudos sin editar, machine_info.json y SHA256SUMS
├── analysis/
│   ├── metrics.py                 # Estadísticas (media, mediana, desv., P95, IC 95 %, msg/s) y tablas booktabs
│   └── generated/                 # Tablas LaTeX y resúmenes generados por script
├── tests/                         # Pruebas unitarias y de integración (Lamport, TCP, gRPC, métricas)
└── docs/
    ├── guia1_practica.tex         # Archivo principal del informe
    ├── referencias.bib            # Bibliografía IEEE verificada con DOI (biblatex/biber)
    ├── sections/                  # Secciones modulares del informe LaTeX
    └── figures/
        └── evidencias/            # Capturas de ejecución de los cuatro integrantes
```

### Estado actual (19 de noviembre de 2026)

| Módulo | Estado |
|---|---|
| Núcleo común (`src/common/`): reloj de Lamport, configuración de nodos, registro de la máquina | Implementado y probado |
| Sockets TCP (`src/sockets/`): framing, servidor y cliente | Implementado y probado |
| gRPC (`proto/`, `src/grpc/`): contrato `.proto`, stubs, servidor y cliente | Implementado y probado |
| Motor estadístico y tablas `booktabs` (`analysis/metrics.py`) | Implementado y probado |
| Script de análisis con gráficos (`analysis/run_analysis.py`) | Implementado y probado |
| Verificación de Lamport sobre los CSV reales | Pendiente |
| Corridas experimentales en `/data/` + `SHA256SUMS` | Completado (commit [`33b44da`](https://github.com/Heiner-ZZZ/GA-SUM-02-PE-U1-Comunicaci-n-entre-Procesos-Distribuidos-con-gRPC-y-Sockets-TCP/commit/33b44dacb02eebe1e3659a9c3ce2443be8ca5708)) |
| Informe LaTeX (`docs/`) | En progreso (resumen, objetivos e hipótesis) |

---

## Instalación

Requisitos: **Python 3.11 o superior** (terminal recomendada en Windows: `cmd.exe`).

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

(Opcional) Regenerar los stubs gRPC a partir del contrato:

```bat
proto\compile_proto.bat
```

---

## Ejecución de los tres nodos

Cada nodo es un proceso independiente en `localhost`: `nodo-A` → 5000, `nodo-B` → 5001, `nodo-C` → 5002. Abrir **tres terminales** (con el entorno `.venv` activado) en la raíz del repositorio.

> Los servidores TCP y gRPC usan los mismos puertos: ejecutar **un mecanismo a la vez** y detener los nodos (`Ctrl+C`) antes de cambiar al otro.

### Sockets TCP

```bat
python -m src.sockets.server --node nodo-A
python -m src.sockets.server --node nodo-B
python -m src.sockets.server --node nodo-C
```

En una cuarta terminal, ejecutar el cliente:

```bat
python -m src.sockets.client
```

Sin argumentos ejecuta los 20 intercambios Lamport y el benchmark completo, y genera:

- `data/events_sockets.csv` y `data/events_sockets.json` (eventos: nodo, tipo, reloj y hora real)
- `data/latency_sockets.csv` (latencias RTT)

### gRPC

```bat
python -m src.grpc.server_grpc --node nodo-A
python -m src.grpc.server_grpc --node nodo-B
python -m src.grpc.server_grpc --node nodo-C
```

En una cuarta terminal:

```bat
python -m src.grpc.client_grpc
```

Genera `data/events_grpc.csv`, `data/events_grpc.json` y `data/latency_grpc.csv`, con el mismo esquema que los de sockets.

### Opciones de los clientes (iguales en TCP y gRPC)

| Opción | Descripción | Valor por defecto |
|---|---|---|
| `--exchanges` | Ejecuta solo los 20 intercambios Lamport | — |
| `--benchmark` | Ejecuta solo las mediciones de latencia | — |
| `--sizes 100B,10KB` | Tamaños de mensaje a medir | `100B,10KB` |
| `--payload-bytes N` | Mide con un tamaño personalizado de `N` bytes | — |
| `--runs N` | Repeticiones válidas | `5` |
| `--messages N` | Envíos por repetición | `100` |
| `--warmup N` | Envíos de la corrida de calentamiento (se descarta) | `100` |
| `--no-concurrent` | Omite la prueba con los tres clientes a la vez | — |

Ejemplo (cambiar el tamaño del mensaje y volver a medir):

```bat
python -m src.grpc.client_grpc --benchmark --payload-bytes 5000
```

### Protocolo experimental que ejecutan ambos clientes

- **20 intercambios** entre los tres nodos con reloj de Lamport (tick antes de enviar; al recibir `max(local, recibido) + 1`), incluidos envíos simultáneos reales coordinados con `threading.Barrier`.
- **Latencia RTT** medida con `time.perf_counter()`: 1 corrida de calentamiento de 100 envíos (descartada) + **5 repeticiones de 100 envíos**.
- Dos tamaños de mensaje: **100 B** y **10 KB**.
- Modo **secuencial** (nodo-A envía en round-robin) y modo **concurrente** (los tres clientes envían a la vez).

---

## Registro de las condiciones de la máquina

```bat
python -c "from src.common.machine_info import save_machine_info; print(save_machine_info())"
```

Guarda `data/machine_info.json` con sistema operativo, procesador, memoria, versión de Python, versiones de las librerías y modo de ejecución (procesos locales). Para solo verlo en pantalla:

```bat
python -m src.common.machine_info
```

---

## Pruebas

```bat
python -m unittest discover -s tests -t . -v
```

| Archivo | Qué verifica |
|---|---|
| `tests/test_lamport.py` | Reglas del reloj de Lamport y configuración de nodos/payloads |
| `tests/test_sockets_server.py` | Framing TCP, mensajes de 100 B / 10 KB y clientes concurrentes |
| `tests/test_sockets_integration.py` | 20 intercambios y benchmark TCP sobre tres nodos |
| `tests/test_grpc_server.py` | RPC de 100 B / 10 KB, eventos Lamport y llamadas concurrentes |
| `tests/test_grpc_integration.py` | 20 intercambios y benchmark gRPC sobre tres nodos |
| `tests/test_metrics.py` | Motor estadístico, descarte de calentamiento y tablas `booktabs` |
| `tests/test_analysis.py` | Script de análisis: tablas y gráficos a partir de CSV de prueba en una carpeta temporal |

---

## Análisis de resultados

Después de ejecutar los clientes de sockets y gRPC (con `data/latency_sockets.csv` y `data/latency_grpc.csv` ya generados):

```bat
python -m analysis.run_analysis
```

El script lee todos los `data/latency_*.csv`, descarta la corrida de calentamiento y calcula, por mecanismo, modo y tamaño de mensaje: latencia media, mediana, desviación estándar, percentil 95, intervalo de confianza del 95 % y rendimiento (mensajes/s). Muestra el resumen en pantalla y genera:

| Archivo | Contenido |
|---|---|
| `analysis/generated/resumen_latencias.csv` | Resumen completo de métricas |
| `analysis/generated/tabla_latencias_sequential.tex` | Tabla `booktabs` del modo secuencial |
| `analysis/generated/tabla_latencias_concurrent.tex` | Tabla `booktabs` del modo concurrente |
| `analysis/generated/comparacion_relativa.csv` y `tabla_comparacion.tex` | Variación porcentual de gRPC respecto a Sockets TCP |
| `docs/figures/boxplot_latencias.png` | Diagrama de cajas de las latencias (300 DPI) |
| `docs/figures/barras_latencia_ic95.png` | Latencia media con intervalo de confianza del 95 % |
| `docs/figures/barras_rendimiento.png` | Rendimiento en mensajes por segundo |

Las tablas y gráficos del informe se incluyen desde estos archivos (`\input{...}` / `\includegraphics{...}`); no se escriben a mano. Opciones: `--data-dir`, `--out-dir` y `--fig-dir` para usar otras carpetas.

---

## Compilación del informe LaTeX (al completar `/docs/`)

Compilador requerido: **TeX Live 2023+ / MiKTeX 23+** con `pdflatex` y `biber` (paquetes: `amsmath`, `booktabs`, `listings`, `graphicx`, `tikz`, `biblatex`, `hyperref`, `babel`, `geometry`).

Desde el directorio `docs/`:

```bat
pdflatex guia1_practica.tex
biber guia1_practica
pdflatex guia1_practica.tex
pdflatex guia1_practica.tex
```

---

## Video de respaldo

*(Pendiente de grabación tras completar la ejecución experimental — se enlazará aquí antes del cierre del 22 de noviembre de 2026).*
