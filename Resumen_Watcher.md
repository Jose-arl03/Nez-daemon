#  Daemon Watcher

## 1. Tareas Realizadas

Se refactorizó el script `watcher.py` para mejorar su resiliencia y capacidad de diagnóstico en un entorno contenedorizado. Las modificaciones se centraron en el manejo de errores y la comunicación con el clúster MictlanX.

## 2. Tecnologías y Patrones Implementados

-   **Programación Asíncrona:** Se utilizó el módulo `asyncio` de Python para gestionar operaciones de red concurrentes sin bloqueo.
-   **Patrón Productor-Consumidor:**
    -   **Productor:** La librería `watchdog` detecta eventos del sistema de archivos (nuevos archivos) y los añade a una `asyncio.Queue`.
    -   **Consumidor:** Múltiples `workers` (tareas de `asyncio`) procesan los elementos de la cola en paralelo.
-   **Logging:** Se reemplazó el uso de `print()` por el módulo `logging` para generar logs estructurados con nivel y timestamp.
-   **Cliente MictlanX:** Se utiliza la librería `mictlanx.AsyncClient` para la interacción con el clúster.

## 3. Funcionalidad Implementada

-   **Verificación de Conectividad (`Health Check`):** Al iniciar, el script verifica la conexión con el `router` de MictlanX (`GET /buckets/{id}/metadata`) antes de empezar a procesar archivos.
-   **Mecanismo de Reintentos:** Se implementó un bucle de reintentos con una estrategia de *exponential backoff* para la subida de archivos. Si una subida falla, se reintenta hasta 3 veces con un retardo creciente (2s, 4s, 8s).
-   **Método de Subida:** Se cambió de `client.put_file(path=...)` a `client.put(value=...)`. El script ahora lee el contenido del archivo en bytes y lo envía directamente, evitando pasar rutas del sistema de archivos al clúster.

## 4. Estado Actual

-   **Detección de archivos:** Operacional.
-   **Comunicación con el `router`:** Operacional.
-   **Lógica de `workers` y reintentos:** Operacional.
-   **Subida de archivos a los `peers`:** **Pendiente de validación.** El éxito de esta operación depende de la estabilidad de los contenedores `mictlanx-peer-*`, que ha sido el foco principal de la depuración del entorno. La última configuración aplicada (`init: true` en `docker-compose.yml`) busca estabilizarlos.

## 5. Posibles Mejoras a Futuro

-   **Gestión de Archivos Fallidos (Cuarentena):** La lógica inicial para mover archivos fallidos a un directorio de cuarentena fue deshabilitada porque el cliente de MictlanX borraba el archivo fuente tras un fallo. Se podría implementar una estrategia más robusta, como copiar el archivo a una ubicación temporal *antes* del primer intento de subida.
-   **Creación de Buckets:** El `bucket` de destino (`nez-bucket`) está definido en el código. El script podría mejorarse para verificar la existencia del bucket al inicio y crearlo si no existe. (Esta funcionalidad no parece estar expuesta actualmente en `mictlanx-client`).
-   **Configuración Externalizada:** Parámetros como el número de reintentos, los tiempos de espera o el ID del bucket podrían externalizarse a variables de entorno en lugar de estar definidos en el código.
