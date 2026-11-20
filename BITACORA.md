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

---

## Sesión 2 — Jueves 19 de noviembre de 2026

- **Asistentes:** [COMPLETAR]
- **Horario y horas trabajadas:** [COMPLETAR bloques y horas] (tiempo acumulado del proyecto: [COMPLETAR]).
- **Trabajo realizado:**
  - **Jaramillo — cliente TCP (`16:57`):** Implementación de `src/sockets/client.py` con los 20 intercambios entre los tres nodos con reloj de Lamport (incluidos envíos simultáneos reales coordinados con `threading.Barrier`) y el benchmark RTT con `time.perf_counter()`: calentamiento de 100 envíos descartado + 5 repeticiones de 100 envíos, mensajes de 100 B y 10 KB, en modo secuencial y con los tres clientes en concurrencia. Exporta `events_sockets.csv/.json` y `latency_sockets.csv`.
  - **España — cliente gRPC (`18:38`):** Implementación de `src/grpc/client_grpc.py`, que ejecuta exactamente el mismo protocolo que el cliente TCP (reutiliza la misma secuencia de intercambios y los mismos exportadores CSV/JSON) sobre los RPC `EnviarMensaje`, `ReiniciarReloj` y `ObtenerEventos`. Pruebas de integración en `tests/test_grpc_integration.py` levantando los tres nodos gRPC (3/3 aprobadas). Prueba manual con los tres servidores como procesos independientes en los puertos 5000–5002: 40 eventos y 4800 filas de latencia (4000 válidas).
  - **Jaramillo — pruebas de integración TCP (`19:01`):** `tests/test_sockets_integration.py` con tres servidores TCP locales: verificación de los 20 intercambios, de la relación `L(recv) > L(send)`, de eventos concurrentes con igual valor de reloj y del benchmark de 100 B / 10 KB (2/2 aprobadas).
  - **Integración:** suite completa del repositorio con 20/20 pruebas aprobadas. Actualización del `README.md` con las instrucciones para ejecutar los nodos, los clientes, el registro de la máquina y las pruebas.
- **Problemas encontrados y solución:**
  - *Prueba intermitente en gRPC:* Una prueba que comparaba el valor máximo del reloj entre dos corridas fallaba de forma aleatoria. Causa: cuando `ex-09` y `ex-10` llegan a la vez a `nodo-B`, el valor final del reloj depende de cuál se procesa primero (comportamiento correcto de Lamport con eventos concurrentes). Solución: verificar el reinicio con un caso determinista (el primer paso siempre produce relojes 1 y 1 en el envío y 2 y 3 en la recepción).
  - *Lentitud al abrir canales gRPC en Windows:* Abrir un canal nuevo en cada intercambio hacía que los 20 intercambios tardaran ~5 s. Solución: un canal persistente por cada par emisor–receptor (los intercambios bajaron a ~1,5 s y la suite de 17 s a 6,5 s). No afecta a la latencia medida, porque el canal se abre y queda listo antes de empezar a medir.
  - *Conexiones compartidas entre clientes gRPC:* gRPC reutiliza por defecto la misma conexión entre canales del mismo proceso hacia un mismo destino, lo que no sería equivalente a TCP en la prueba concurrente. Solución: opción `grpc.use_local_subchannel_pool` para que cada cliente use su propia conexión.
  - *Resolución de la hora real en Windows:* `datetime.now()` tiene una resolución de ~1 ms, por lo que algunos eventos quedan con la misma hora. Solución: el orden se desempata con el valor del reloj de Lamport y la verificación empareja envío y recepción por `msg_id`; se documentará como amenaza a la validez.
  - *Usuario de GitHub en los documentos:* El `README.md` y la carátula del informe indicaban un usuario distinto al que firma los commits de España. Se corrigió a `@Kespanav` para que la tabla de aportes enlace correctamente sus commits.
- **Próximos pasos:**
  - Script de análisis que lea los CSV de `/data/` y genere tablas, diagrama de cajas y gráfico de barras con intervalos.
  - Script de verificación de Lamport sobre los CSV reales.
  - Corridas experimentales completas de ambos mecanismos, `machine_info.json` y `SHA256SUMS`.
  - Secciones restantes del informe LaTeX, capturas de cada integrante y video.
