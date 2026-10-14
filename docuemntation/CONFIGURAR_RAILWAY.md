# Configurar Raspberry → Railway

El enlace ya usa el protocolo del backend SelfIQ desplegado: desafío, prueba
Ed25519, token de sucursal, estado de registro y órdenes. Se conserva esa lógica
y se configura el destino público con TLS. No se cambia a un backend ficticio.

Flujo: Raspberry → WSS → FastAPI en Railway → PostgreSQL en Supabase.
Las ESP32 siguen comunicándose con la Raspberry por la red local.

## Dónde ingresar cada dato

| Dato | Archivo o mecanismo en Raspberry |
| --- | --- |
| Dominio público del backend | `/etc/selfiq/backend.env`, variable `SELFIQ_BACKEND_URL` |
| URL WebSocket explícita, como alternativa | Mismo archivo, variable `SELFIQ_BACKEND_WS_URL` |
| Token de sucursal generado por SelfIQ | `sudo /opt/selfiq/.venv/bin/python /opt/selfiq/tools/configure_backend.py`, o formulario de registro del AP |
| ID y clave privada de Raspberry | Generados automáticamente y conservados en `/var/lib/selfiq/gateway.json`; no editarlos manualmente |
| Credenciales Wi-Fi | Formulario del AP existente; no se envían al backend |

El token de sucursal NO es JWT_SECRET, un JWT de usuario, una API key de Railway,
una clave Supabase ni la contraseña de PostgreSQL. Las variables DATABASE_URL y
JWT_SECRET siguen exclusivamente en el backend Railway. La Raspberry no necesita
conectarse directamente a Supabase ni guardar credenciales de la base de datos.

## 1. Aplicar solo los cambios

El paquete contiene rutas relativas a la raíz de tu proyecto `SelfIQ_RPI`.
Extrae sobre ese proyecto, conservando sus demás archivos:

```bash
tar -xzf SelfIQ_RPI-cambios-Railway.tar.gz -C /ruta/a/SelfIQ_RPI
cd /ruta/a/SelfIQ_RPI
sudo bash install.sh
```

El instalador actualiza el código en `/opt/selfiq` y crea
`/etc/selfiq/backend.env` solo si aún no existe. No sobrescribe la URL ni token de
una instalación anterior. Si ya existe ese archivo, edítalo en el siguiente paso.
No basta con modificar el proyecto si el servicio ejecuta su copia de `/opt/selfiq`.
La instalación/reinicio puede interrumpir una sesión SSH que use Wi-Fi.

## 2. Ingresar el dominio público de Railway

En Railway, abre el servicio del backend, Settings → Networking → Public
Networking. Usa su dominio público, el mismo que usaría el frontend para la API.
Si no tiene uno, genera el dominio. No uses el dominio de Supabase ni un hostname
terminado en `.railway.internal`.

```bash
sudo nano /etc/selfiq/backend.env
```

El contenido que debes ajustar es:

```ini
SELFIQ_BACKEND_URL=https://TU-DOMINIO-REAL.up.railway.app
```

Sustituye `TU-DOMINIO-REAL` por tu dominio. No agregues `:8000`: ese es el puerto
interno donde escucha Uvicorn; el dominio público usa HTTPS/WSS. El código
convierte esa dirección a `wss://TU-DOMINIO-REAL.up.railway.app/ws/gateways` y
agrega el ID del gateway automáticamente. Mantiene la validación normal del
certificado TLS; no hay que descargar o desactivar certificados para Railway.

También puedes conservar la variable anterior, ingresando directamente:

```ini
SELFIQ_BACKEND_WS_URL=wss://TU-DOMINIO-REAL.up.railway.app/ws/gateways
```

**Elige una de las dos.** Si ya hay una `SELFIQ_BACKEND_WS_URL` local activa,
reemplázala o comenta esa línea: tiene prioridad sobre `SELFIQ_BACKEND_URL`.
Nunca añadas el gateway ID al final de la variable.

Orden de lectura: `SELFIQ_BACKEND_WS_URL` → `SELFIQ_BACKEND_URL` →
`backend_ws_url` de `/etc/selfiq/config.json`. Antes, una variable ausente borraba
el valor de config.json; ahora se respeta ese valor como alternativa. El archivo
`backend.env` es la opción recomendada y systemd ya lo carga en el servicio.
Dejar todas las URLs vacías mantiene el aprovisionamiento Wi-Fi, pero desactiva
el enlace cloud. Las URLs HTTP/WS quedan limitadas a desarrollo en red privada.

```bash
sudo chmod 600 /etc/selfiq/backend.env
sudo systemctl restart selfiq
sudo systemctl status selfiq --no-pager
```

No necesitas cambiar `selfiq.service`: ya usa
`EnvironmentFile=-/etc/selfiq/backend.env`. No hay que desplegar el proyecto RPI
en Railway ni abrir puertos de tu router: la Raspberry inicia la conexión saliente.

## 3. Ingresar el token de sucursal

Si la Raspberry ya tiene un token válido, lo reutiliza. En caso contrario:

```bash
sudo /opt/selfiq/.venv/bin/python /opt/selfiq/tools/configure_backend.py
```

El programa pide el token con entrada oculta. Es el token generado por tu backend
para registrar equipos de la sucursal, con scope `register_gateway`. Se guarda
con la identidad del equipo; no se coloca dentro del código ni se imprime.
También puedes ingresarlo en el formulario del AP `SelfIQ-Admi`.

El HTTP `127.0.0.1` dentro de esa herramienta es correcto: sirve para configurar
el servicio que corre en la propia Raspberry. El enlace hacia Railway utiliza
WSS; no debe reemplazarse ese localhost por el dominio de Railway.

Si una instalación antigua conserva `SELFIQ_GATEWAY_TOKEN` en backend.env,
no lo uses como mecanismo de cambio de token: la identidad persistida conserva
su token. Actualízalo con la herramienta/formulario indicado. No borres
`gateway.json` para corregir un token: contiene la clave privada del equipo.

## 4. Comprobar comunicación

```bash
sudo journalctl -u selfiq -f
```

El log debe mostrar conexión por `WSS/TLS` y `Registro backend: registered`.
`token_invalid` requiere actualizar el token; `registration_rejected` requiere
revisar el token o identidad. `backend_unavailable` indica que no pudo establecer
el enlace o completar el intercambio; se reintenta con espera de hasta 30 s.

También puedes consultar desde la Raspberry:

```bash
curl --fail http://127.0.0.1/status
```

Comprueba `backend_conectado: true`, `registration_status: registered`,
`gateway_id` y `branch_id`. No se muestran claves privadas ni tokens.
El backend ofrece `/gateways` y `/gateways/{gateway_id}` para el frontend
Autenticado. Las órdenes son `tcp.enable`, `tcp.disable` y `status` mediante
`POST /gateways/{gateway_id}/commands`; el backend las envía por WSS a la Raspberry.

Se verificó la ruta `/ws/gateways/{gateway_id}` en el backend SelfIQ preparado
anteriormente. Este paquete no requiere modificar esa ruta ni las tablas de DB.
El backend actual administra las conexiones en memoria: mantener una sola
instancia/proceso para que las solicitudes del frontend encuentren la sesión
WebSocket del gateway. Un despliegue con varias réplicas requiere coordinación
entre procesos que esta versión aún no implementa.

## Alcance y pruebas

Se modificaron `models.py`, `gateway_identity.py`, `backend_link.py`, `install.sh`,
README y dos archivos de pruebas; se agregaron `backend_config.py`,
`config/backend.env.example`, esta guía y pruebas de Railway.

Se probaron 36 casos en host, incluyendo un servidor WSS con certificado
verificado, validación criptográfica de la prueba del gateway, recepción de una
orden, reconexión, identidad persistente, prioridades de configuración y
regresión del HTTP/TCP de aprovisionamiento. El certificado temporal de prueba
no se incluye ni se configura en el producto. Las pruebas Wi-Fi utilizan un
adaptador simulado.

No se probó tu dominio Railway real ni el hardware: no se proporcionaron dominio
ni token. Tras ingresar esos datos, verifica el estado siguiendo los pasos
anteriores. El simulador antiguo `dev/backend.py` utiliza autenticación Bearer y
no representa el protocolo actual de registro firmado; no se usa en producción.

Este cambio habilita el canal cloud de registro de Raspberry y órdenes. Todavía
falta implementar en Raspberry la respuesta TCP del broker y el puente MQTT de
registro por MAC para el paquete ESP32 anterior, además del reenvío de datos de
sensores. No se presentan esas funcionalidades como terminadas por cambiar la URL.

Referencias:
- https://docs.railway.com/networking/public-networking
- https://docs.railway.com/networking/public-networking/specs-and-limits
