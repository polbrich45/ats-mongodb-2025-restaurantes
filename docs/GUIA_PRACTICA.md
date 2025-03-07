# GUÍA PRÁCTICA MongoDB: Sistema de Inspecciones de Restaurantes

## Introducción

Esta guía te ayudará a trabajar con el dataset de restaurantes e inspecciones sanitarias utilizando MongoDB. A lo largo de esta práctica, aprenderás a diseñar un esquema de base de datos adecuado, implementar consultas avanzadas y optimizar el rendimiento.

## Descripción del Dataset

El dataset consta de dos colecciones principales:

1. **Restaurantes**: Contiene información sobre establecimientos de comida, incluyendo nombre, dirección, tipo de comida y calificación.
2. **Inspecciones**: Contiene registros de inspecciones sanitarias realizadas a los restaurantes, incluyendo fecha, resultado y tipo de inspección.

Cada restaurante puede tener múltiples inspecciones asociadas, estableciendo una relación uno a muchos entre ambas colecciones.

## Preparación del Entorno

### Requisitos Previos

- Cuenta en MongoDB Atlas (si trabajáis en la nube)
- MongoDB instalado (versión 6.0 o superior).
- MongoDB Compass (opcional, para visualización de datos o trabajar con la shell).
- MongoDB Shell (mongosh) para ejecutar consultas.
- Python 3.9+ (para scripts opcionales de procesamiento de datos).
- Otras herramientas como NoSQLBooster o Studio 3T.
- API Key OpenAI (opcional y solicitar previamente)

### Instalación de MongoDB

Si aún no tienes MongoDB instalado:

1. Descarga MongoDB Community Server desde [mongodb.com/try/download](https://www.mongodb.com/try/download)
2. Sigue las instrucciones de instalación para tu sistema operativo.
3. Instala MongoDB Compass desde [mongodb.com/products/compass](https://www.mongodb.com/products/compass).

## Importación de Datos

Para importar los datos a MongoDB, puedes utilizar los siguientes comandos:

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

## Estructura de Datos

### Colección `restaurants`

```json
{
    "_id": ObjectId("..."),
    "name": "Nombre del restaurante",
    "address": {
        "street": "Calle",
        "city": "Ciudad",
        "postcode": "Código postal"
    },
    "rating": 5,
    "type_of_food": "Tipo de comida",
    "url": "URL del restaurante"
}
```

### Colección `inspections`

```json
{
    "_id": ObjectId("..."),
    "restaurant_id": ObjectId("..."),
    "certificate_number": 12345,
    "date": "2023-01-15",
    "result": "Resultado de la inspección",
    "sector": "Sector de la inspección",
    "address": {
        "city": "Ciudad",
        "postcode": "Código postal",
        "street": "Calle"
    }
}
```

## Tareas Prácticas

### 1. Diseño del Esquema

- Analiza la estructura de los datos y determina el tipo de relación entre restaurantes e inspecciones.
- Diseña un esquema de validación para ambas colecciones.
- Justifica tus decisiones de diseño.

### 2. Consultas Básicas

Implementa las siguientes consultas:

- Buscar restaurantes por tipo de comida.
- Encontrar inspecciones con resultado específico.
- Ordenar restaurantes por calificación.

### 3. Agregaciones

Implementa agregaciones para:

- Obtener el número de inspecciones por restaurante.
- Calcular la calificación promedio por tipo de comida.
- Unir restaurantes con sus inspecciones usando `$lookup`.

### 4. Optimización de Rendimiento

- Identifica las consultas más frecuentes.
- Implementa índices adecuados.
- Mide el rendimiento antes y después de la indexación.

### 5. Estrategias de Escalabilidad

- Propón una estrategia de sharding para la base de datos.
- Diseña un esquema de replicación para alta disponibilidad.
- Analiza posibles cuellos de botella y soluciones.

## Recursos Adicionales

- [Documentación oficial de MongoDB](https://www.mongodb.com/docs/manual/)
- [MongoDB University](https://university.mongodb.com/)
- [Guía de Agregaciones en MongoDB](https://www.mongodb.com/docs/manual/aggregation/)
- [Optimización de Consultas en MongoDB](https://www.mongodb.com/docs/manual/core/query-optimization/)
- [Índices en MongoDB](https://www.mongodb.com/docs/manual/indexes/)
- [Replicación en MongoDB](https://www.mongodb.com/docs/manual/replication/)
- [Sharding en MongoDB](https://www.mongodb.com/docs/manual/sharding/)

- [Cursos de MongoDB en DataCamp (enlace de invitación en Discord)](https://www.datacamp.com/es)
- [Documentación de LangChain](https://python.langchain.com/docs/introduction/)

**Tenéis acceso a los PDFs que hemos visto en clase de teoría y problemas vía Campus Virtual**

## Evaluación

La evaluación se realizará según los criterios establecidos en el [Enunciado de la Práctica](ENUNCIADO.md), considerando:

- Diseño adecuado del esquema.
- Implementación correcta de consultas.
- Optimización del rendimiento.
- Justificación técnica de las decisiones.
- Calidad de la documentación.

---

Para cualquier duda, consulta la documentación en `docs/` o contacta con el profesor a través del campus virtual.

