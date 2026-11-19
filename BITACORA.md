# Bitácora del Equipo EJPZ — PE-U1

**Asignatura:** Aplicaciones Distribuidas (7mo Nivel – Paralelo A)  
**Actividad:** GA-SUM-02 / PE-U1 — Comunicación entre Procesos Distribuidos con gRPC y Sockets TCP  
**Equipo:** EJPZ (Jaramillo, España, Zambrano, Paca)

---

## Sesión 1 — Miércoles 18 de noviembre de 2026

- **Asistentes:** Heiner Zambrano, Jeremy Jaramillo, Kenny España, Luis Paca.
- **Horario y horas trabajadas:**
  - **Bloque mañana (09:00 – 11:30 | 2 h 30 min):** Búsqueda y verificación de referencias bibliográficas indexadas (2022–2026) con DOI activo en IEEE Xplore, ACM Digital Library, Springer y Scopus, además de la revisión del enunciado del SGA y la guía de laboratorio.
  - **Bloque tarde (15:00 – 19:30 | 4 h 30 min):** Inicio de la jornada técnica del equipo, análisis del enunciado y la guía de laboratorio, diseño de la topología de 3 nodos (`nodo-A:5000`, `nodo-B:5001`, `nodo-C:5002`), reparto de módulos y creación del repositorio público en GitHub (`17:43`).
  - **Bloque noche (21:00 – 23:10 | 2 h 10 min):** Integración progresiva de las 11 referencias verificadas en `docs/referencias.bib` por los cuatro integrantes (21:08 Zambrano, 21:29 Paca, 21:49 España, 22:34 Jaramillo), creación de la estructura de carpetas del proyecto (`21:27`) e implementación del núcleo común (`requirements.txt`, `src/common/lamport.py`, `config.py`, `machine_info.py` y `tests/test_lamport.py`) con pruebas aprobadas (7/7). Posteriormente, Jaramillo incorporó el módulo de entramado TCP con prefijo de 4 bytes (`src/sockets/framing.py`), el servidor TCP multihilo con reloj de Lamport (`src/sockets/server.py`) y sus pruebas unitarias (`tests/test_sockets_server.py`, 2/2 aprobadas; 9/9 en conjunto).
  - **Aporte de España (gRPC/Protobuf):** Incorporación de referencias sobre gRPC y serialización con Protocol Buffers en `docs/referencias.bib` (`jackson2024streaming`, `niswar2024performance`, `mehdi2026crosslanguage`) y definición del servicio `MessagingService` en Protobuf (`proto/messaging.proto`) con los RPC `EnviarMensaje`, `ReiniciarReloj` y `ObtenerEventos`.
  - **Aporte de Paca (motor estadístico, generador `booktabs`, base LaTeX e hipótesis previa):** Implementación del motor estadístico en `analysis/metrics.py` (validador estricto de esquemas CSV, descarte de corridas de calentamiento, cálculo de media, mediana, desviación estándar, P95, IC 95 %, throughput en msg/s, variaciones porcentuales y exportador automático de tablas LaTeX `booktabs`), pruebas unitarias sobre datos en memoria sin ensuciar `/data/` (`tests/test_metrics.py`, 4/4 aprobadas) y estructura base del informe en LaTeX (`docs/guia1_practica.tex` con `biblatex`/`biber` y `docs/sections/01_resumen_objetivos.tex` con resumen bilingüe, objetivos e hipótesis previa formulada antes de medir).
  - **Total de la sesión:** 9 h 10 min (tiempo acumulado del proyecto: 9 h 10 min).
- **Problemas encontrados y solución:**
  - *Inconsistencia de versiones locales de Python:* En las pruebas iniciales coexistían Python 3.12 y 3.14 con versiones distintas de `protobuf` y `grpcio`, y en Windows el alias de `WindowsApps` interceptaba el comando `python`. Se estandarizó `requirements.txt` con versiones exactas compatibles con Python 3.11+ (`grpcio==1.85.3`, `grpcio-tools==1.85.3`, `protobuf==7.36.2`, `pandas==3.0.2`, `matplotlib==3.11.2`, `numpy==2.4.4`), se verificó la ejecución mediante el lanzador `py` (Python 3.14.5) y se definió el uso de entorno virtual `.venv`.
  - *Organización de archivos iniciales:* Se limpió un directorio vacío mal ubicado (`src/proto`) y archivos temporales locales para que cada integrante suba exclusivamente su módulo asignado sin solapamientos.
  - *Control de autoría en Git:* Se acordó que cada integrante utilice una sola cuenta y correo en `git config` y realice `git pull --rebase` antes de cada envío.
- **Próximos pasos (cierre Día 1 y arranque Día 2):**
  - Subir servidor TCP con framing (`Jaramillo`), contrato `.proto` + servidor gRPC (`España`) y motor estadístico/exportador `booktabs` + base LaTeX e hipótesis previa (`Paca`).
