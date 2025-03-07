# ATS MongoDB 2025: Sistema de Inspecciones de Restaurantes

## Descripción

Esta práctica consiste en trabajar con un sistema de inspecciones de restaurantes utilizando MongoDB. Deberás diseñar un esquema adecuado, implementar consultas y optimizar el rendimiento de la base de datos.

## Estructura del Repositorio

- **datasets/**: Contiene los archivos JSON con datos de restaurantes e inspecciones.
- **scripts/**: Aquí deberas incluir tu codigo que utilices y el código de tus consultas.
- **templates/**: Por ahora vacía, reservada para incluir algún ejemplo.
- **docs/**: Documentación y guías de la práctica.

## Fechas Importantes

- **06/03/2025**: Explicación, análisis del dataset, diseño inicial. Trabajo en grupo.
- **20/03/2025**: Exposición de los resultados en una presentación de máximo 10 minutos.

## Documentación

Consulta la documentación completa en los siguientes archivos dentro del directorio `docs/`:

- [Guía de la Práctica](docs/GUIA_PRACTICA.md): Explicación detallada del dataset y tareas a realizar.
- [Instrucciones de Entrega](docs/INSTRUCCIONES_ENTREGA.md): Proceso de entrega y evaluación.
- [Enunciado de la Práctica](docs/ENUNCIADO.md): Descripción completa de la práctica y objetivos.

## Instalación y Configuración

### Requisitos Previos

- Cuenta en MongoDB Atlas (si trabajáis en la nube)
- MongoDB instalado (versión 6.0 o superior).
- MongoDB Compass (opcional, para visualización de datos o trabajar con la shell).
- MongoDB Shell (mongosh) para ejecutar consultas.
- Python 3.9+ (para scripts opcionales de procesamiento de datos).
- Otras herramientas como NoSQLBooster o Studio 3T.
- API Key OpenAI (opcional y solicitar previamente)

### Importación de Datos

1. Clona este repositorio:
   ```bash
   git clone https://github.com/albertgilopez/ats-mongodb-2025-restaurantes.git
   cd ats-mongodb-2025-restaurantes
   ```
2. Importa los datos en MongoDB:

En localhost:

```bash
mongoimport --db restaurant_db --collection restaurants --file datasets/restaurants.json --jsonArray
mongoimport --db restaurant_db --collection inspections --file datasets/inspections.json --jsonArray
```

En MongoDB Atlas:

```bash
mongoimport --uri "mongodb+srv://usuario:contraseña@cluster.mongodb.net/restaurant_db" \
--collection restaurants --file restaurants.json --jsonArray
mongoimport --uri "mongodb+srv://usuario:contraseña@cluster.mongodb.net/restaurant_db" \
--collection inspections --file inspections.json --jsonArray
```

**O utilizar el proceso de importacion de datos desde MongoDB Compass**.

## Contacto

Para cualquier duda, consulta la documentación en `docs/`, escribe vía Discord o por correo electrónico.

