# Usar una imagen oficial de Python como imagen base
FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Instalar poetry, que usaremos para construir la rueda (wheel) de mictlanx-client
RUN pip install poetry

# Copiar el código fuente del proyecto mictlanx-client
# Esto asume que el contexto de construcción de Docker es el directorio 'Estadia'
COPY mictlanx-client /tmp/mictlanx-client/

# Entrar en el directorio del proyecto, construir la rueda e instalarla globalmente en el contenedor
RUN cd /tmp/mictlanx-client && poetry build && pip install --extra-index-url https://test.pypi.org/simple/ dist/*.whl

# Ahora, copiar el código fuente de la aplicación watcher
COPY Nez-daemon/services/watcher/ /app/

# Instalar las dependencias propias del watcher desde su requirements.txt
RUN pip install -r requirements.txt

# El comando para iniciar el watcher. El directorio a vigilar se pasará desde docker-compose.
CMD ["python", "-u", "watcher.py"]