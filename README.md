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
| Kenny Isaías España Vera | `@KennyVera` | Contrato Protobuf, implementación gRPC, fundamento fallos/seguridad y QA reproducible |
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
│   ├── sockets/                   # Servidor, cliente y orquestador de Sockets TCP
│   └── grpc/                      # Stubs generados, servidor, cliente y orquestador gRPC
├── data/                          # CSV crudos sin editar, machine_info.json y SHA256SUMS
├── analysis/
│   └── generated/                 # Tablas LaTeX (booktabs) y resúmenes generados por script
├── tests/                         # Pruebas unitarias y de integración
└── docs/
    ├── referencias.bib            # Bibliografía IEEE verificada con DOI (biblatex/biber)
    ├── sections/                  # Secciones modulares del informe LaTeX
    └── figures/
        └── evidencias/            # Capturas de ejecución de los cuatro integrantes
```

> **Estado actual:** Estructura inicial y módulo común (`src/common/`) implementados y probados. Los módulos de `/proto/`, `/src/sockets/`, `/src/grpc/`, `/data/`, `/analysis/` y `/docs/` se integrarán progresivamente conforme al plan de commits del equipo (18 al 22 de noviembre de 2026).

---

## Instalación y verificación inicial

Requisitos: **Python 3.11 o superior** (terminal recomendada en Windows: `cmd.exe`).

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Ejecutar pruebas unitarias del núcleo común:

```bat
python -m unittest discover -s tests -v
```

Inspeccionar condiciones de la máquina actual:

```bat
python -m src.common.machine_info
```

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
