//Definir un esquema de validación para la colección "restaurants".
db.runCommand({
    collMod: "restaurants",  // Define la colección 'restaurants'
    validator: {  // Inicia la validación
      $jsonSchema: {  // Usamos un esquema JSON para validar
        bsonType: "object",  // La colección es de tipo objeto
        required: ["name", "type_of_food"],  // Campos requeridos
        properties: {  // Definimos los campos de la colección
          name: {  // Campo 'name' que es de tipo string
            bsonType: "string",
            description: "Nombre del restaurante obligatorio."
          },
          type_of_food: {  // Campo 'type_of_food' de tipo string
            bsonType: "string",
            description: "Tipo de comida obligatorio."
          },
          rating: {  // Campo 'rating' de tipo double, con rango entre 0 y 10
            bsonType: "double",
            minimum: 0,
            maximum: 10,
            description: "Calificación del restaurante entre 0 y 10."
          },
          address: {  // Campo 'address' que es de tipo string
            bsonType: "string",
            description: "Dirección del restaurante."
          }
        }
      },
      validationLevel: "strict"  // Nivel de validación estricto
    }
  });
  
  //Definir un esquema de validación para la colección "inspections".
  db.runCommand({
    collMod: "inspections",  // Define la colección 'inspections'
    validator: {  // Inicia la validación
      $jsonSchema: {  // Usamos un esquema JSON para validar
        bsonType: "object",  // La colección es de tipo objeto
        required: ["restaurant_id", "date", "result"],  // Campos requeridos
        properties: {  // Definimos los campos de la colección
          restaurant_id: {  // Campo 'restaurant_id' de tipo string
            bsonType: "string",
            description: "Debe referenciar un restaurante."
          },
          date: {  // Campo 'date' con un patrón de fecha específica
            bsonType: "string",
            pattern: "^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \\d{1,2} \\d{4}$",  // Formato de fecha
            description: "Fecha en formato 'Mon DD YYYY' (Ejemplo: 'Jul 31 2022')."
          },
          result: {  // Campo 'result' con valores posibles "Pass", "Fail", "Violation Issued"
            bsonType: "string",
            enum: ["Pass", "Fail", "Violation Issued"],
            description: "Debe ser 'Pass', 'Fail' o 'Violation Issued'."
          },
          certificate_number: {  // Campo 'certificate_number' de tipo entero
            bsonType: "int",
            description: "Número de certificado de la inspección."
          }
        }
      },
      validationLevel: "strict"  // Nivel de validación estricto
    }
  });


//Buscar todos los restaurantes de un tipo de comida específico (ej. "Chinese").
db.restaurants.find({ "type_of_food": "Chinese" });

//Listar las inspecciones con violaciones, ordenadas por fecha.
db.inspections.find({ "result": "Violation Issued" }).sort({ "date": 1 });

//Encontrar restaurantes con una calificación superior a 4.
db.restaurants.find({ "rating": { $gt: 4 } });

//Agrupar restaurantes por tipo de comida y calcular la calificación promedio.
db.restaurants.aggregate([
  { $group: { _id: "$type_of_food", avgRating: { $avg: "$rating" } } },
  { $sort: { avgRating: -1 } }
]);

//Contar el número de inspecciones por resultado y mostrar los porcentajes.
db.inspections.aggregate([
  { $group: { _id: "$result", count: { $sum: 1 } } },
  { $group: { _id: null, total: { $sum: "$count" }, results: { $push: { result: "$_id", count: "$count" } } } },
  { $unwind: "$results" },
  { $project: { result: "$results.result", count: "$results.count", percentage: { $multiply: [{ $divide: ["$results.count", "$total"] }, 100] } } },
  { $sort: { count: -1 } }
]);

//Unir restaurantes con sus inspecciones utilizando $lookup.
db.restaurants.aggregate([
  {
    $lookup: {
      from: "inspections",
      let: { restaurantId: { $toString: "$_id" } },
      pipeline: [
        { $match: { $expr: { $eq: ["$restaurant_id", "$$restaurantId"] } } }
      ],
      as: "inspections"
    }
  }
]);
// Unir restaurantes con sus inspecciones utilizando $lookup.
db.restaurants.aggregate([
    {
      $lookup: {
        from: "inspections", // Relaciona con la colección 'inspections'.
        let: { restaurantId: { $toString: "$_id" } }, // Convierte el _id a string.
        pipeline: [
          { $match: { $expr: { $eq: ["$restaurant_id", "$$restaurantId"] } } } // Une por restaurant_id.
        ],
        as: "inspections" // Almacena el resultado en el campo 'inspections'.
      }
    }
  ]);
  
  //Listar restaurantes con mejor historial de inspecciones (sin fallos).
db.restaurants.aggregate([
{
    $lookup: {
    from: "inspections",
    localField: "_id",
    foreignField: "restaurant_id",
    as: "inspection_history"
    }
},
{ $match: { "inspection_history.result": { $ne: "Fail" } } } // Excluye restaurantes con fallos.
]);

//Listar los 10 restaurantes con peor historial de inspecciones.
db.restaurants.aggregate([
{
    $lookup: {
    from: "inspections",
    let: { restaurantId: { $toString: "$_id" } },
    pipeline: [
        {
        $match: {
            $expr: { $eq: ["$restaurant_id", "$$restaurantId"] },
            result: { $in: ["Fail", "Violation Issued"] } // Filtra los fallos.
        }
        }
    ],
    as: "failed_inspections"
    }
},
{ $addFields: { failed_inspection_count: { $size: "$failed_inspections" } } }, // Cuenta fallos.
{ $sort: { failed_inspection_count: -1 } }, // Ordena por más fallos.
{ $limit: 10 }, // Toma los 10 peores.
{ $project: { name: 1, failed_inspection_count: 1, URL: 1, address: 1, type_of_food: 1 } } // Selecciona campos a mostrar.
]);

//Agrupar los peores restaurantes por tipo de comida y listar los peores 2 de cada uno.
db.restaurants.aggregate([
{
    $lookup: {
    from: "inspections",
    let: { restaurantId: { $toString: "$_id" } },
    pipeline: [
        {
        $match: {
            $expr: { $eq: ["$restaurant_id", "$$restaurantId"] },
            result: { $in: ["Fail", "Violation Issued"] }
        }
        }
    ],
    as: "failed_inspections"
    }
},
{ $addFields: { failed_inspection_count: { $size: "$failed_inspections" } } },
{ $sort: { failed_inspection_count: -1 } }, // Ordena por más fallos.
{
    $group: {
    _id: "$type_of_food", // Agrupa por tipo de comida.
    worstRestaurants: {
        $push: {
        name: "$name",
        failed_inspection_count: "$failed_inspection_count",
        address: "$address",
        URL: "$URL"
        }
    }
    }
},
{ $addFields: { worstRestaurants: { $slice: ["$worstRestaurants", 2] } } }, // Mantiene solo los 2 peores.
{ $limit: 10 }, // Máximo 10 tipos de comida.
{ $project: { _id: 0, type_of_food: 1, worstRestaurants: 1 } } // Formato final.
]);
